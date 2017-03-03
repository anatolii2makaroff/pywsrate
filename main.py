
"""
    Example asyncio server

"""


import asyncio
import logging
import time

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)



def get_data():
    time.sleep(5)
    return 1


async def get():
    a =  get_data()
    print(a)
    return a


def handler():
    data = get()
    print(data)


def main():
    logger.debug("starting..")

    loop = asyncio.get_event_loop()
    loop.set_debug(True)

    loop.run_until_complete(get()).add_done_callback(lambda x: print(x))
    loop.close()



if __name__ == "__main__":
    main()
