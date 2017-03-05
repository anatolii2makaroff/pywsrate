#
# Test ex. Websocket Rate Server
#
# Authtor Makarov A
# Date    04/03/17
#

import asyncio
import websockets
from threading import Thread
from queue import Queue
from collections import deque
import requests
import time
from lxml import etree
import logging


#
# config
#

R_LIST = {
    1: "EURUSD",
    2: "USDJPY",
    3: "GBPUSD",
    4: "AUDUSD",
    5: "USDCAD"
}

HOST = "localhost"
PORT = 8080

FX_URL = "http://rates.fxcm.com/RatesXML2"

######

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(threadName)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


class Rate(object):
    """
        Rate values struct
        and L1 cache (default 300)
    """
    def __init__(self, cached_size=300):
        self.data = {}
        self._cache = deque([], cached_size)

    def update(self, timestamp, data):
        for i in data.items():
            self.data[i[0]] = {"assetName": i[0],
                               "time": timestamp,
                               "assetId": [x[0] for x in R_LIST.items()
                                            if x[1] == i[0]][0],
                                "value": i[1]}

        # add to cache
        self._cache.append(self.data)


    def current(self):
        return self.data

    def get_cache(self):
        return self._cache


def _time_log(f):
    """
    util
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
        skip = yield
        if skip:
            tb = time.time()

        yield tb


def rates_saver(queue):
    """
        data save consumer

    """

    while True:
        data = queue.get()
        logger.debug("get data: {0}".format(data))

        # TODO save to redis..




def rates_producer(rates, queue):

    s = requests.Session()
    _flag = True

    tbg = b_time()
    tbg.send(None)

    while True:
        tb = tbg.send(_flag)
        logger.debug("begin time is {}".format(tb))
        try:
            res = s.get(FX_URL, timeout=0.3)
            if res.status_code != 200:
                raise Exception(res.text)

            # perse result and put to cache
            rate = parseXML(res.text)

            rates.update(tb, rate)
            logger.debug("rate's data: {0}".format(rates.current()))

            if not queue.full():
                logger.debug("queue size is {0}".format(queue.qsize()))
                queue.put_nowait(rates.current())

            next(tbg)
            _flag = True

        except Exception as e:
            # TODO: need catch only FX service err..
            logger.error(e)
            # try immedeatly 150 ms
            # TODO: increasing interval value (if FX is not avaliable too long)
            time.sleep(0.150)
            logger.info("try to reconnect..")

            #
            # if we'll make more trying - need save first time
            # after we'll sleep less time
            #
            next(tbg)
            _flag = False

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
    """
    Handler for ws connections
    """
    while True:

        data = await websocket.recv()
        logger.debug("recv {}".format(data))

        cmd = data.get("action")
        logger.debug("cmd is {}".format(cmd))

        if cmd == "assets":
            pass
        elif cmd == "subscribe":
            while True:

                await asyncio.sleep(1.0)


        else:
            logger.warn("unknow cmd: {}".format(cmd))
            await websocket.send("unknow cmd: {}".format(cmd))


def main():

    #
    # TODO when starts -> fill rates from store
    #
    rates = Rate()

    queue = Queue(600)  # 10 min capacity

    #
    # start producer for rates
    # it pull, write & publish
    #
    t = Thread(target=rates_producer, args=(rates, queue))

    #
    # start consumer for rates
    # save rates to any store (last 30 min)
    #
    # if store is not avaliable put rates in memory queue
    # for later flush
    #
    t2 = Thread(target=rates_saver, args=(queue,))

    t.start()
    t2.start()

    #
    # start event loop for websocket requests
    # rates comes from publisher
    #

    start_server = websockets.serve(h_rate, HOST, PORT)
    asyncio.get_event_loop().run_until_complete(start_server)
    asyncio.get_event_loop().run_forever()


if __name__ == "__main__":
    main()
