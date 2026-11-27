import asyncio
import unittest
from unittest import mock

from telethon.extensions import html as tl_html

import bot


class _DummyCallbackEvent:
    def __init__(self, data: bytes, sender_id: int = 101):
        self.data = data
        self.sender_id = sender_id
        self.calls = []
        self.answers = []

    async def edit(self, text, **kwargs):
        self.calls.append(("edit", text, kwargs))
        return _DummyMessage()

    async def respond(self, text, **kwargs):
        self.calls.append(("respond", text, kwargs))
        return _DummyMessage()

    async def answer(self, text="", alert=False):
        self.answers.append((text, alert))


class _DummyMessage:
    id = 42

    def __init__(self):
        self.edits = []

    async def edit(self, text, **kwargs):
        self.edits.append((text, kwargs))
        return self


class _DummyNewMessageEvent:
    def __init__(self, text: str, sender_id: int = 101, is_group: bool = False, chat_id: int = 101):
        self.text = text
        self.sender_id = sender_id
        self.is_group = is_group
        self.chat_id = chat_id
        self.id = 77
        self.responses = []

    async def respond(self, text, **kwargs):
        self.responses.append((text, kwargs))
        return _DummyMessage()


def _assert_valid_ui(testcase: unittest.TestCase, text: str, buttons):
    parsed_text, entities = tl_html.parse(text)
    testcase.assertTrue(len(parsed_text.strip()) > 0)
    testcase.assertNotIn("━━━━", text)
    if not buttons:
        return
    for row in buttons:
        testcase.assertIsInstance(row, list)
        testcase.assertLessEqual(len(row), 3)
        for btn in row:
            raw = btn.button if hasattr(btn, "button") else btn
            testcase.assertTrue(len(raw.text) > 0)
            btn_type = getattr(raw, "type", raw)
            if getattr(btn_type, "data", None) is not None:
                testcase.assertLessEqual(len(btn_type.data), 64)
            elif getattr(btn_type, "url", None) is not None:
                testcase.assertTrue(btn_type.url.startswith("https://"))


class TestBotUI(unittest.TestCase):
    def setUp(self):
        bot.USER_STATES.clear()
        bot.RUNNING_TASKS.clear()
        bot.LAST_TASK_INFO.clear()

    def tearDown(self):
        bot.USER_STATES.clear()
        bot.RUNNING_TASKS.clear()
        bot.LAST_TASK_INFO.clear()

    def test_main_menu_required_options_only(self):
        with mock.patch.object(bot, "STORAGE_CHANNELS", [-1001234567890]), mock.patch.object(
            bot, "STORAGE_CHANNEL_ID", -1001234567890
        ):
            text, buttons = bot._build_main_menu(101)
            _assert_valid_ui(self, text, buttons)
            labels = [b.text for row in buttons for b in row]
            self.assertIn("⬇️ New Download", labels)
            self.assertIn("📊 Stats", labels)
            self.assertIn("📂 Storage", labels)
            self.assertIn("❓ Help", labels)
            self.assertNotIn("🧹 Clear Temp", labels)

            # When a task is running, main menu shows Cancel Active Task instead of New Download
            bot.RUNNING_TASKS[101] = object()
            text_busy, buttons_busy = bot._build_main_menu(101)
            _assert_valid_ui(self, text_busy, buttons_busy)
            busy_labels = [b.text for row in buttons_busy for b in row]
            self.assertIn("🛑 Cancel Active Task", busy_labels)
            self.assertNotIn("⬇️ New Download", busy_labels)

    def test_stats_and_help_views_render_clean_html(self):
        async def run():
            with mock.patch.object(bot, "_get_supabase_media_count", return_value="12"):
                stats_text, stats_btns = await bot._build_stats_view()
                _assert_valid_ui(self, stats_text, stats_btns)
                stats_labels = [b.text for row in stats_btns for b in row]
                self.assertIn("🔄 Refresh", stats_labels)
                self.assertIn("🧹 Clear Cache", stats_labels)
                self.assertIn("‹ Back", stats_labels)

        asyncio.run(run())

        help_text, help_btns = bot._build_help_view()
        _assert_valid_ui(self, help_text, help_btns)
        self.assertIn("/download", help_text)
        self.assertIn("/cancel", help_text)

    def test_progress_bar_formatting(self):
        bar_mid = bot.get_progress_bar(50 * 1024 * 1024, 100 * 1024 * 1024)
        self.assertIn("█████░░░░░", bar_mid)
        self.assertIn("50%", bar_mid)
        bar_unknown = bot.get_progress_bar(10 * 1024 * 1024, 0)
        self.assertIn("10.0 MB", bar_unknown)

    def test_prompt_url_options_single_and_collection_and_html_escape(self):
        ev_single = _DummyNewMessageEvent("https://pimpbunny.com/videos/test-clip/")
        ev_col = _DummyNewMessageEvent("https://bunkr.cr/a/album123?x=1&y=<tag>")

        async def run():
            with (
                mock.patch.object(bot, "STORAGE_CHANNEL_ID", -1001234567890),
                mock.patch.object(bot, "get_chat_title_cached", return_value="Archive & <Main>"),
            ):
                await bot.prompt_url_options(ev_single, 101, ev_single.text)
                self.assertEqual(len(ev_single.responses), 1)
                text_s, kw_s = ev_single.responses[0]
                _assert_valid_ui(self, text_s, kw_s["buttons"])
                self.assertIn("Archive &amp; &lt;Main&gt;", text_s)
                labels_s = [b.text for row in kw_s["buttons"] for b in row]
                self.assertEqual(labels_s, ["⚡ Download Now", "📍 Destination", "✕ Cancel"])

                await bot.prompt_url_options(ev_col, 101, ev_col.text)
                self.assertEqual(len(ev_col.responses), 1)
                text_c, kw_c = ev_col.responses[0]
                _assert_valid_ui(self, text_c, kw_c["buttons"])
                labels_c = [b.text for row in kw_c["buttons"] for b in row]
                self.assertEqual(labels_c, ["📋 Browse & Select", "⬇️ Download All", "🔢 Page Range", "✕ Cancel"])

        asyncio.run(run())

    def test_callback_navigation_and_topic_picker_limits(self):
        async def run():
            long_title = "🔥 " + ("Very Long Topic Name " * 3)
            topics = [{"id": 88, "title": long_title, "closed": False}]

            with (
                mock.patch.object(bot, "is_admin", return_value=True),
                mock.patch.object(bot, "STORAGE_CHANNELS", [-1001234567890]),
                mock.patch.object(bot, "STORAGE_CHANNEL_ID", -1001234567890),
                mock.patch.object(bot, "get_chat_title_cached", return_value="Forum Group"),
                mock.patch.object(bot, "is_forum_chat", return_value=True),
                mock.patch.object(bot, "get_forum_topics_list", return_value=topics),
                mock.patch.object(bot.events.CallbackQuery, "Event", _DummyCallbackEvent),
            ):
                # 1. New download -> edit in place
                ev1 = _DummyCallbackEvent(b"new_download")
                await bot.callback_handler(ev1)
                self.assertEqual(bot.USER_STATES[101]["state"], "WAITING_URL")
                self.assertEqual(ev1.calls[0][0], "edit")
                _assert_valid_ui(self, ev1.calls[0][1], ev1.calls[0][2]["buttons"])

                # 2. Topic existing list ensures callback_data <= 64 bytes even for long emoji titles
                bot.USER_STATES[101] = {"url": "https://bunkr.cr/a/abc", "dest": -1001234567890}
                ev2 = _DummyCallbackEvent(b"topic_existing_0")
                await bot.callback_handler(ev2)
                self.assertEqual(bot.USER_STATES[101]["state"], "WAITING_TOPIC")
                text2, kw2 = ev2.calls[0][1], ev2.calls[0][2]
                _assert_valid_ui(self, text2, kw2["buttons"])
                first_btn = kw2["buttons"][0][0]
                first_data = getattr(getattr(first_btn, "type", first_btn), "data", None)
                self.assertEqual(first_data, b"top_pick:88")

                # 3. Back from topic_create resets WAITING_TOPIC_TITLE back to WAITING_TOPIC
                ev3 = _DummyCallbackEvent(b"topic_create")
                await bot.callback_handler(ev3)
                self.assertEqual(bot.USER_STATES[101]["state"], "WAITING_TOPIC_TITLE")
                ev4 = _DummyCallbackEvent(b"dest_topic_pick:-1001234567890")
                await bot.callback_handler(ev4)
                self.assertEqual(bot.USER_STATES[101]["state"], "WAITING_TOPIC")

                # 4. Destination picker & back_from_dest transitions cleanly
                ev_dest = _DummyCallbackEvent(b"quick_choose_dest")
                await bot.callback_handler(ev_dest)
                self.assertEqual(bot.USER_STATES[101]["state"], "WAITING_CHANNEL")
                _assert_valid_ui(self, ev_dest.calls[0][1], ev_dest.calls[0][2]["buttons"])
                ev_back = _DummyCallbackEvent(b"back_from_dest")
                await bot.callback_handler(ev_back)
                self.assertEqual(bot.USER_STATES[101]["state"], "WAITING_RANGE")
                _assert_valid_ui(self, ev_back.calls[0][1], ev_back.calls[0][2]["buttons"])

                # 5. Stats, Help, and Main Menu edit in place
                for cb in (b"stats_info", b"help", b"main_menu"):
                    ev_nav = _DummyCallbackEvent(cb)
                    await bot.callback_handler(ev_nav)
                    self.assertEqual(ev_nav.calls[0][0], "edit")
                    _assert_valid_ui(self, ev_nav.calls[0][1], ev_nav.calls[0][2]["buttons"])

                # 6. Cancel flow clears state and provides Menu + New Download actions
                bot.USER_STATES[101] = {"state": "WAITING_URL"}
                ev5 = _DummyCallbackEvent(b"cancel_flow")
                await bot.callback_handler(ev5)
                self.assertNotIn(101, bot.USER_STATES)
                _assert_valid_ui(self, ev5.calls[0][1], ev5.calls[0][2]["buttons"])
                cancel_labels = [b.text for row in ev5.calls[0][2]["buttons"] for b in row]
                self.assertIn("🏠 Menu", cancel_labels)
                self.assertIn("⬇️ New Download", cancel_labels)

                # 7. Topics list view renders interactive controls and handles refresh cleanup
                t_text, t_btns = await bot._render_topics_list_view(-1001234567890)
                _assert_valid_ui(self, t_text, t_btns)
                bot.USER_STATES[101] = {"state": "WAITING_TOPIC_TITLE", "dest": -1001234567890, "standalone": True}
                ev_ref = _DummyCallbackEvent(b"topics_refresh:-1001234567890")
                await bot.callback_handler(ev_ref)
                self.assertNotIn(101, bot.USER_STATES)
                _assert_valid_ui(self, ev_ref.calls[0][1], ev_ref.calls[0][2]["buttons"])

        asyncio.run(run())

    def test_set_bot_commands_concise_menu(self):
        captured = {}

        async def fake_client_call(req):
            captured["commands"] = req.commands

        async def run():
            with mock.patch.object(bot, "client", side_effect=fake_client_call):
                await bot.set_bot_commands()

        asyncio.run(run())
        names = [c.command for c in captured["commands"]]
        self.assertEqual(names, ["start", "download", "crawl", "topics", "stats", "id", "cancel", "help"])

    def test_group_non_url_messages_do_not_trigger_admin_error(self):
        ev = _DummyNewMessageEvent("hello everyone", sender_id=999999, is_group=True, chat_id=-1001)

        async def run():
            with mock.patch.object(bot, "check_admin_or_notify") as chk:
                await bot.message_handler(ev)
                chk.assert_not_called()

        asyncio.run(run())

    def test_cancel_command_clears_state(self):
        bot.USER_STATES[101] = {"state": "WAITING_URL"}
        ev = _DummyNewMessageEvent("/cancel", sender_id=101)

        async def run():
            with mock.patch.object(bot, "is_admin", return_value=True):
                await bot.cancel_command_handler(ev)

        asyncio.run(run())
        self.assertNotIn(101, bot.USER_STATES)
        self.assertEqual(len(ev.responses), 1)
        _assert_valid_ui(self, ev.responses[0][0], ev.responses[0][1]["buttons"])


if __name__ == "__main__":
    unittest.main()
