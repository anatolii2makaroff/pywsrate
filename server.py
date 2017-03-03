

import asyncio
import websockets
from threading import Thread
import requests
import time
from lxml import etree

R_LIST = {

    1:"EURUSD",
    2:"USDJPY"
}


rates = []



def _time_log(f):
    def wrap(*args, **kwargs):
        tb = time.time()
        ret = f(*args, **kwargs)
        print("elapsed: {}".format(time.time() - tb))
        return ret
    return wrap

@_time_log
def parseXML(data):

    xml = etree.fromstring(data.encode("utf-8"))
    nodes = xml.xpath('/Rates/Rate')

    d = {}
    for node in nodes:
        i = [x.text for x in node.getchildren()]
        d[i[0]] = (float(i[1]) + float(i[2])) / 2.0

    return d

def parseRE(data):
    pass


def get_rates(rates):

    while True:
        res = requests.get("http://rates.fxcm.com/RatesXML2", timeout=0.05)
        time.sleep(1)
        rates.append(parseXML(res.text))

        #write to redis



async def hello(websocket, path):
    name = await websocket.recv()
    print("< {}".format(name))

    greeting = "Hello {}!".format(name)
    await websocket.send(greeting)

    while True:
        import pdb; pdb.set_trace()  # XXX BREAKPOINT


        await websocket.send(rates[:-1])
        await asyncio.sleep(1.0)


def main():

    t = Thread(target=get_rates, args=(rates,))
    t.start()


    start_server = websockets.serve(hello, 'localhost', 8765)

    asyncio.get_event_loop().run_until_complete(start_server)

    asyncio.get_event_loop().run_forever()


if __name__ == "__main__":
    main()
