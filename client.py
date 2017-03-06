
import asyncio
import websockets
import json

async def test():
    async with websockets.connect('ws://localhost:8080') as websocket:
        while True:
            cmd = input("What's cmd?\n")
            if cmd == "ass":
                d = json.dumps({"action":"assets","message":{}})
                await websocket.send(d)

                data = await websocket.recv()
                print("< {}".format(data))

            elif cmd == "sub":
                d = json.dumps({"action":"subscribe","message":{"assetId":2}})
                await websocket.send(d)

                while True:
                    data = await websocket.recv()
                    # break
                    print("< {}".format(data))

            continue




if __name__ == "__main__":

    asyncio.get_event_loop().run_until_complete(test())
