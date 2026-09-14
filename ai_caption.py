import os
import re
import json
import asyncio
import logging
import html
from typing import Dict, Any, List, Optional
import httpx

logger = logging.getLogger(__name__)

MISTRAL_API_URL = "https://api.mistral.ai/v1/chat/completions"
DEFAULT_MODEL = "mistral-small-latest"

def _escape(text: str) -> str:
    """Escapes text for Telegram HTML parse mode."""
    if not text:
        return ""
    return html.escape(str(text), quote=False)

def _sanitize_hashtag(tag: str) -> str:
    """Sanitize a string into a valid Telegram hashtag."""
    tag = tag.strip().lstrip("#")
    # Remove any characters not alphanumeric or underscore
    tag = re.sub(r"[^\w]", "", tag)
    if not tag:
        return ""
    return f"#{tag}"

COMPOUND_TAGS = {
    "brattymilf": ["bratty", "milf"],
    "onlyfans": ["only", "fans"],
    "fansly": ["fansly"],
    "photoshoot": ["photo", "shoot"],
    "bigboobs": ["big", "boobs"],
    "bigtits": ["big", "tits"],
    "bigass": ["big", "ass"],
    "naturalboobs": ["natural", "boobs"],
    "footjob": ["foot", "job"],
    "titjob": ["tit", "job"],
    "blowjob": ["blow", "job"],
    "deepthroat": ["deep", "throat"],
}

def extract_name_tags(raw_title: str) -> list[str]:
    """Breaks composite words and filenames into individual search hashtags."""
    text = re.sub(r"\.(mp4|mkv|mov|avi|webm|ts|m4v|flv|wmv)$", "", str(raw_title), flags=re.I)
    text = re.sub(r"^\d+[-_\s]+", "", text)
    text = re.sub(r"([a-z])([A-Z])", r"\1 \2", text)
    tokens = re.split(r"[^a-zA-Z0-9]+", text)

    tags = set()
    STOPWORDS = {"mp4", "mkv", "avi", "mov", "webm", "video", "com", "org", "http", "https", "the", "and", "with", "for", "part"}
    for tok in tokens:
        tok_clean = tok.strip()
        if len(tok_clean) <= 1 or tok_clean.isdigit():
            continue
        tok_lower = tok_clean.lower()
        if tok_lower in STOPWORDS:
            continue
        tags.add(f"#{tok_clean.title()}")
        for comp, parts in COMPOUND_TAGS.items():
            if comp in tok_lower:
                tags.add(f"#{comp.title()}")
                for p in parts:
                    tags.add(f"#{p.title()}")
    return sorted(list(tags))

def _build_fallback_caption(raw_title: str, content: str, service: str, creator: str, vid_id: str) -> Dict[str, Any]:
    """Generates standard caption with broken tags when Mistral AI is unavailable or disabled."""
    clean_title = re.sub(r"\.[a-zA-Z0-9]{2,4}$", "", raw_title)
    clean_title = re.sub(r"^\d+[-_\s]+", "", clean_title)
    clean_title = re.sub(r"([a-z])([A-Z])", r"\1 \2", clean_title)
    clean_title = re.sub(r"[_\-\.]+", " ", clean_title).strip()
    if clean_title.islower():
        clean_title = clean_title.title()
    if not clean_title:
        clean_title = f"{service.capitalize()} Video"

    # Extract broken name tags
    name_tags = extract_name_tags(raw_title)
    all_tags = set(name_tags)

    if service:
        srv = _sanitize_hashtag(service)
        if srv: all_tags.add(srv)
    if creator:
        cr = _sanitize_hashtag(creator.replace("-", "_").replace(" ", "_"))
        if cr: all_tags.add(cr)
    all_tags.add("#Video")
    all_tags.add("#Archive")

    tags = sorted(list(t for t in all_tags if t))

    caption_parts = [f"🎥 <b>{_escape(clean_title[:80])}</b>"]
    if content:
        caption_parts.append(f"📝 <i>{_escape(content[:140])}</i>")
    caption_parts.append(f"👤 {_escape(service)}/{_escape(creator)}\n🆔 <code>{_escape(str(vid_id))}</code>")
    if tags:
        caption_parts.append(f"🏷️ {' '.join(tags[:10])}")

    rich_caption = "\n\n".join(caption_parts)
    db_title = f"{clean_title[:90]} {' '.join(tags)}"

    return {
        "clean_title": clean_title[:100],
        "summary": content[:140] if content else "",
        "hashtags": tags,
        "rich_caption": rich_caption,
        "db_title": db_title[:240]
    }

async def generate_ai_caption(vid: Dict[str, Any], service: str, creator: str) -> Dict[str, Any]:
    """
    Generates rich metadata, descriptive title, summary, and hashtags using Mistral AI.
    Falls back gracefully to standard metadata if Mistral API key is not configured or errors.
    Retries automatically on 429 (rate limit) with exponential backoff.
    """
    api_key = os.getenv("MISTRAL_API_KEY", "").strip()
    model = os.getenv("MISTRAL_MODEL", DEFAULT_MODEL).strip() or DEFAULT_MODEL
    raw_title = vid.get("title") or vid.get("name") or "video"
    content = (vid.get("content") or "").strip()
    vid_id = str(vid.get("id", ""))

    fallback = _build_fallback_caption(raw_title, content, service, creator, vid_id)

    if not api_key:
        logger.debug("MISTRAL_API_KEY not set. Using fallback caption generator.")
        return fallback

    prompt_user = {
        "platform": service,
        "creator": creator,
        "raw_filename": raw_title,
        "post_description": content[:500] if content else None
    }

    system_prompt = (
        "You are an expert video archivist and SEO metadata specialist. "
        "Analyze the provided video info and generate structured metadata for Telegram indexing.\n"
        "Rules:\n"
        "1. 'clean_title': Catchy, natural, descriptive title (maximum 80 characters). "
        "Remove raw random hashes, UUIDs, underscores, and file extensions like .mp4/.mkv.\n"
        "2. 'summary': 1-2 sentence compelling overview of the video/post (maximum 150 characters). "
        "Do not include URLs or spam.\n"
        "3. 'hashtags': 5 to 8 searchable hashtags starting with '#'. Include platform, creator name, "
        "content format, and relevant keywords in CamelCase (e.g., #OnlyFans, #CreatorName, #Exclusive, #Vlog, #1080p).\n"
        "Output ONLY valid JSON matching this schema:\n"
        '{"clean_title": "...", "summary": "...", "hashtags": ["#...", "#..."]}'
    )

    MAX_RETRIES = 3
    BASE_DELAY = 2.0  # seconds; doubles each retry: 2s, 4s, 8s

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with httpx.AsyncClient(timeout=15.0) as http_client:
                response = await http_client.post(
                    MISTRAL_API_URL,
                    headers={
                        "Authorization": f"Bearer {api_key}",
                        "Content-Type": "application/json"
                    },
                    json={
                        "model": model,
                        "messages": [
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": json.dumps(prompt_user)}
                        ],
                        "response_format": {"type": "json_object"},
                        "temperature": 0.3
                    }
                )

            if response.status_code == 429 or response.status_code == 503:
                # Rate limited or overloaded — respect Retry-After header if present, else backoff
                retry_after = float(response.headers.get("Retry-After", BASE_DELAY * (2 ** (attempt - 1))))
                retry_after = min(retry_after, 30.0)  # cap at 30s so we don't stall uploads
                if attempt < MAX_RETRIES:
                    logger.warning(
                        f"Mistral API {response.status_code} on attempt {attempt}/{MAX_RETRIES}. "
                        f"Retrying in {retry_after:.1f}s..."
                    )
                    await asyncio.sleep(retry_after)
                    continue
                else:
                    logger.warning(f"Mistral API rate limited after {MAX_RETRIES} attempts. Using fallback caption.")
                    return fallback

            if response.status_code != 200:
                logger.warning(f"Mistral API returned status {response.status_code}: {response.text[:150]}")
                return fallback

            data = response.json()
            ai_message = data["choices"][0]["message"]["content"]
            parsed = json.loads(ai_message)

            clean_title = (parsed.get("clean_title") or fallback["clean_title"]).strip()
            summary = (parsed.get("summary") or fallback["summary"]).strip()
            raw_hashtags = parsed.get("hashtags") or fallback["hashtags"]

            # Ensure valid hashtags
            hashtags: List[str] = []
            for tag in raw_hashtags:
                st = _sanitize_hashtag(tag)
                if st and st not in hashtags:
                    hashtags.append(st)

            # Merge all broken name tags so every filename token is indexed
            for nt in extract_name_tags(raw_title):
                if nt not in hashtags:
                    hashtags.append(nt)

            # Always guarantee creator and service tags exist for Telegram search
            srv_tag = _sanitize_hashtag(service)
            cr_tag = _sanitize_hashtag(creator.replace("-", "_").replace(" ", "_"))
            if srv_tag and srv_tag not in hashtags:
                hashtags.insert(0, srv_tag)
            if cr_tag and cr_tag not in hashtags:
                hashtags.insert(1, cr_tag)

            # Build rich HTML caption
            caption_parts = [f"🎥 <b>{_escape(clean_title[:80])}</b>"]
            if summary:
                caption_parts.append(f"📝 <i>{_escape(summary[:150])}</i>")
            caption_parts.append(f"👤 {_escape(service)}/{_escape(creator)}\n🆔 <code>{_escape(vid_id)}</code>")
            if hashtags:
                caption_parts.append(f"🏷️ {' '.join(hashtags[:10])}")

            rich_caption = "\n\n".join(caption_parts)

            # Keep strictly under Telegram's 1024 character caption limit
            if len(rich_caption) > 1000:
                summary = summary[:80] + "..."
                caption_parts[1] = f"📝 <i>{_escape(summary)}</i>"
                rich_caption = "\n\n".join(caption_parts)

            db_title = f"{clean_title[:90]} {' '.join(hashtags[:12])}"

            logger.info(f"Generated AI metadata for '{clean_title}' with {len(hashtags)} hashtags.")
            return {
                "clean_title": clean_title[:100],
                "summary": summary,
                "hashtags": hashtags,
                "rich_caption": rich_caption,
                "db_title": db_title[:240]
            }

        except Exception as e:
            logger.warning(f"Error calling Mistral AI (attempt {attempt}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES:
                await asyncio.sleep(BASE_DELAY * (2 ** (attempt - 1)))
            else:
                logger.warning("Mistral AI failed after all retries. Falling back to default caption.")
                return fallback

    return fallback  # unreachable but satisfies type checker
