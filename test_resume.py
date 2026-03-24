import asyncio
import os
from pathlib import Path
from scraper import download_video

async def test_resume():
    domain = "coomer.st"
    video = {
        "url": "https://coomer.st/data/8d/23/8d2345d349317f3cb318ab118af68a1eb7db1b724e9b634f6160e6a4886810c2.mp4",
        "name": "resume_test.mp4",
        "id": "test_id"
    }
    output_dir = Path("test_resume")
    file_path = output_dir / f"{video['id']}_{video['name']}"
    
    if os.path.exists(file_path):
        os.remove(file_path)
    
    print("Step 1: Partial download...")
    # I'll manually interrupt the download after 5 seconds
    task = asyncio.create_task(download_video(domain, video, output_dir))
    await asyncio.sleep(5)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        print("Download interrupted as planned.")
    
    if file_path.exists():
        partial_size = os.path.getsize(file_path)
        print(f"Partial file size: {partial_size} bytes")
        
        print("Step 2: Resuming download...")
        success = await download_video(domain, video, output_dir)
        if success:
            final_size = os.path.getsize(file_path)
            print(f"✅ Resume successful! Final size: {final_size} bytes")
            if final_size > partial_size:
                print("Confirmed: Resumed from previous position.")
            else:
                print("Warning: Final size is not greater than partial size.")
        else:
            print("❌ Resume failed.")
    else:
        print("❌ Partial file not found. Download might have been too fast or failed.")

if __name__ == "__main__":
    asyncio.run(test_resume())
