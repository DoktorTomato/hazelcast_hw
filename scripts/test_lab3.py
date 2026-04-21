import httpx
import asyncio
import time

async def test_post_messages():
    async with httpx.AsyncClient() as client:
        for i in range(1, 11):
            msg = f"msg{i}"
            try:
                print(f"Sending {msg} to facade...")
                r = await client.post("http://localhost:8000/messages", json={"msg": msg})
                print(f"Response: {r.status_code} - {r.json()}")
            except Exception as e:
                print(f"Failed to send {msg}: {e}")
            await asyncio.sleep(0.5)

async def test_get_messages():
    async with httpx.AsyncClient() as client:
        try:
            print("Fetching all messages from facade...")
            r = await client.get("http://localhost:8000/messages")
            print(f"Response: {r.status_code}")
            data = r.json()
            print(f"Logs: {data.get('logs')}")
            print(f"Counter messages: {data.get('counter_messages')}")
        except Exception as e:
            print(f"Failed to fetch messages: {e}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "get":
        asyncio.run(test_get_messages())
    else:
        asyncio.run(test_post_messages())
