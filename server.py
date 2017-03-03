#
# Test ex. Websocket Rate Server
#
# Authtor Makarov A
# Date    04/03/17
#

import asyncio
import websockets
from threading import Thread
import requests
import time
from lxml import etree
import logging


#
# config
#

R_LIST = {
    1: "EURUSD",
    2: "USDJPY"
}

HOST = "localhost"
PORT = 8080

FX_URL = "http://rates.fxcm.com/RatesXML2"

######

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


rates = []


def _time_log(f):
    """
    time measure decorator
    """
    def wrap(*args, **kwargs):
        tb = time.time()
        ret = f(*args, **kwargs)
        logger.debug("time elapsed: {} {} s.".format(f.__name__, time.time() - tb))
        return ret
    return wrap

@_time_log
def parseXML(data):

    xml = etree.fromstring(data.encode("utf-8"))
    nodes = xml.xpath('/Rates/Rate')

    d = {}
    for node in nodes:
        i = [x.text for x in node.getchildren()]

        if i[0] in R_LIST.values():
            d[i[0]] = (float(i[1]) + float(i[2])) / 2.0
        else:
            continue

    return d

def parseRE(data):
    """
    TODO: maybe regexp be more faster parsing way
    """
    pass



def b_time():
    """ begin date memorizer with skip"""
    tb = time.time()
    while True:
        i = (yield)

        if i:
            tb = time.time()

        yield tb


@_time_log
def get_rates(rates):

    s = requests.Session()

    # tb = b_time()
    # tb.send(None)

    while True:
        tb = time.time()
        try:
            res = s.get(FX_URL, timeout=0.05)
            if res.status_code != 200:
                raise Exception(res.text)

            # perse result and put to cache
            rates.append(parseXML(res.text))

        except Exception as e:
            logger.error(e)
            # try immedeatly 50 ms
            time.sleep(0.050)
            logger.debug("try to reconnect..")
            continue

        _sleep1(time.time() - tb)


def _sleep1(delta):
    #
    # get more fair interval
    #
    logger.debug("finish ops for: {}".format(delta))
    if delta >= 1.0:
        logger.warn("time is out..")

    else:
        time.sleep(1.0 - delta)



async def h_rate(websocket, path):
    name = await websocket.recv()
    print("< {}".format(name))

    greeting = "Hello {}!".format(name)
    await websocket.send(greeting)

    while True:
        import pdb; pdb.set_trace()  # XXX BREAKPOINT


        await websocket.send(rates[:-1])
        await asyncio.sleep(1.0)


def main():

    #
    # start lister for rates
    # it pull, write & save to redis rates
    #
    t = Thread(target=get_rates, args=(rates,))
    t.start()


    start_server = websockets.serve(h_rate, HOST, PORT)
    asyncio.get_event_loop().run_until_complete(start_server)
    asyncio.get_event_loop().run_forever()


if __name__ == "__main__":
    main()
