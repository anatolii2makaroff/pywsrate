#
# Test ex. Websocket Rate Server
#
# Authtor Makarov A
# Date    06/03/17
#

import asyncio
import websockets
from threading import Thread
from queue import Queue
from collections import deque
import redis
import requests
import time
from lxml import etree
import json
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

RHOST = "localhost"
RPORT = 6379
RDB = 0

FX_URL = "http://rates.fxcm.com/RatesXML2"

######

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(threadName)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


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


class Rate(object):
    """
        Rate values struct
        and L1 cache (default 300)
    """
    def __init__(self, cached_size=300):
        self.data = {}
        self._cache = deque([], cached_size)

    @_time_log
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

    @_time_log
    def set_cache(self, data):
        d = {}
        for i in data:
            tmp = json.loads(i)

            for j in  tmp.items():
                d[j[0]] = j[1]
            self._cache.append(d)


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
    r = redis.StrictRedis(host=RHOST, port=RPORT, db=RDB)
    _cap = 30 * 60  # 30 min capacity

    while True:
        data = queue.get()
        data = json.dumps(data)  # convert to json
        logger.debug("get data: {0}".format(data))

        # try to save
        while True:
            try:
                r.lpush("p", data)
                r.ltrim("p", 0, _cap - 1)

                break

            except Exception as e:
                logger.debug("cann't save data: {0} .. retry after 3 sec..".format(data))
                time.sleep(3)

                continue


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
            time.sleep(0.150)

            if time.time() - tb > 0.7:
                logger.warn("lost rate dot {0}..".format(tb))
                next(tbg)
                _flag = True

            else:
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


async def _execute(websocket, data):
        """
           Cmd handler
        """
        global rates

        cmd = data.get("action")
        logger.debug("cmd is {}".format(cmd))

        #
        # Assets handler
        #
        if cmd == "assets":

            res = {"action":"assets",
                   "message":{
                       "assets": [{"id":x[0], "name":  x[1]} for x in R_LIST.items()]
                    }
                   }

            await websocket.send(json.dumps(res))

        #
        # Subscribe handler
        #
        elif cmd == "subscribe":

            _name = await _push_cache(websocket, data)
            # send last
            _tmp = None
            while True:
                #
                # long pooling
                # TODO create pub/sub messaging throw registered client &
                # observable class
                #
                data = rates.current().get(_name)

                listener_task = asyncio.ensure_future(websocket.recv())
                producer_task = asyncio.ensure_future(_push_last(websocket, _tmp, data))

                done, pending = await asyncio.wait([listener_task, producer_task],
                                                    return_when=asyncio.FIRST_COMPLETED)

                if producer_task in done:
                    message = producer_task.result()

                    if message == 1:
                        break
                    else:
                        _tmp = message
                else:
                    producer_task.cancel()

                if listener_task in done:
                    message = json.loads(listener_task.result())
                    if message.get("action") == "subscribe":
                        _name = await _push_cache(websocket, message)

                else:
                    listener_task.cancel()


        #
        # anything handler
        #
        else:
            raise Exception("asset is not avaliable")


async def _push_last(websocket, _tmp, data):

    try:
        if _tmp != data.get("time"):
            await websocket.send(json.dumps(data))
            _tmp = data.get("time")

        await asyncio.sleep(0.250)  # time delta beetwen diff clients

    except Exception:
        return 1

    return _tmp

async def _push_cache(websocket, data):

    _id = data.get("message").get("assetId")
    _name = R_LIST.get(_id)

    if _name is None:
        raise Exception("assetId: {0} is not avaliable".format(_id))

    # send cache 5 min data
    cache5 = [x.get(_name) for x in rates.get_cache()]
    logger.debug("len cache is {}".format(len(cache5)))
    await websocket.send(json.dumps(cache5))

    return _name


async def h_rate(websocket, path):
    """
    Handler for ws connections
    """

    while True:

        data = await websocket.recv()

        try:
            data = json.loads(data)
            logger.debug("recv {}".format(data))
            await _execute(websocket, data)

        except Exception as e:
            logger.error("cmd: {}".format(e))
            await websocket.send(json.dumps({"error": "cmd: {}".format(e)}))

        continue


@_time_log
def restore_rates(rates, size=300):
    """
    restore 5 min cache from redis
    """
    try:
        r = redis.StrictRedis(host=RHOST, port=RPORT, db=RDB, decode_responses=True)
        data = r.lrange("p", 0, size - 1)

        logger.debug("reading from cache from redis: {0}".format(len(data)))
        rates.set_cache(data)

    except Exception as e:
        logger.warn("error when restore from redis: {0}".format(e))


#
# TODO remove global reference
#
rates = Rate()

def main():

    global rates

    restore_rates(rates)

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
    # start event loop in main thread for websocket requests
    # rates comes from publisher
    #

    start_server = websockets.serve(h_rate, HOST, PORT)
    asyncio.get_event_loop().run_until_complete(start_server)
    asyncio.get_event_loop().run_forever()


if __name__ == "__main__":
    main()
