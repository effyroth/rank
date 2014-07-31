# vim: fileencoding=utf-8

import logging

import motor

from tornado import gen
from tornado import ioloop

from webapi.atlaslib.config import MONGO


_TIMEOUT = 10

def _mongo_conn_fac():
    conn = dict(servers=None, client=None)
    @gen.engine
    def _(callback):
        if not conn['servers'] or conn['servers'] != MONGO['CLOUDATLAS']['servers']:
            conn['servers'] = MONGO['CLOUDATLAS']['servers']
            logging.info('mongo ==> %s' % conn['servers'])
            conn['client'] and conn['client'].close()
            client = motor.MotorClient(conn['servers'],
                    io_loop=ioloop.IOLoop.instance(), max_wait_time=_TIMEOUT)
            result = yield gen.Task(client.open)
            conn['client'] = client
            logging.info('mongo connect result: %s' % repr(result))
        callback(conn['client'])
    return _


get_mongo_client = _mongo_conn_fac()


