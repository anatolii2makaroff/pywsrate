
import asyncio
import websockets

async def test():
    async with websockets.connect('ws://localhost:8080') as websocket:
        while True:
            cmd = input("What's cmd?")
            if cmd == "assets":
                await websocket.send({"action":"assets",
                                      "message":{}})

            data = await websocket.recv()
            print("< {}".format(data))

if __name__ == "__main__":

    asyncio.get_event_loop().run_until_complete(test())
