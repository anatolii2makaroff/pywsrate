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

logging.basicConfig(level=logging.DEBUG,
                    format="%(asctime)s %(threadName)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


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
        skip = yield
        if skip:
            tb = time.time()

        yield tb


def rates_saver(queue):

    while True:
        data = queue.get()
        logger.debug("get data: {0}".format(data))




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

            rates.append(rate)

            if not queue.full():
                logger.debug("queue size is {0}".format(queue.qsize()))
                queue.put_nowait(rate)

            next(tbg)
            _flag = True

        except Exception as e:
            logger.error(e)
            # try immedeatly 150 ms
            # TODO: increasing interval value (if FX is not avaliable too long)
            time.sleep(0.150)
            logger.debug("try to reconnect..")

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
    name = await websocket.recv()
    print("< {}".format(name))

    greeting = "Hello {}!".format(name)
    await websocket.send(greeting)

    while True:
        import pdb; pdb.set_trace()  # XXX BREAKPOINT


        await websocket.send(rates[:-1])
        await asyncio.sleep(1.0)


def main():

    rates = []
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
