import tornado.ioloop
import tornado.web
import tornadoredis
import pymongo
from tornadoredis import Client as Redis

from tornado import gen
import json

CONNECTION_POOL = tornadoredis.ConnectionPool(max_connections=500, wait_for_available=True)

class RankApp(tornado.web.Application):
    """docstring for RankApp"""
    def __init__(self, arg):
        super(RankApp, self).__init__()
        self.arg = arg


class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("Hello, world")

class TopHandler(tornado.web.RequestHandler):

    @tornado.web.asynchronous
    @gen.engine
    def get(self):
        appname = self.get_argument("appname", "unknow")
        uid = self.get_argument("uid", 0)
        r = Redis(connection_pool=CONNECTION_POOL)
        result = {}

        result["top"] = yield gen.Task(r.zrevrange, "rank:" + appname, 0, 10, "withscore")
        print result
        self.finish(json.dumps(result))

class RankHandler(tornado.web.RequestHandler):

    @tornado.web.asynchronous
    @gen.engine
    def get(self):
        appname = self.get_argument("appname", "unknow")
        uid = self.get_argument("uid", 0)
        r = Redis(connection_pool=CONNECTION_POOL)
        result = {}

        result["top"] = yield gen.Task(r.zrevrange, "rank:" + appname, 0, 10, "withscore")
        result["user"] = {}
        print appname
        rank = (yield gen.Task(r.zrevrank, "rank:" + appname, uid))
        if rank != None:
            ++rank
            pass
        print "rank:", rank, appname, uid
        result["user"]["rank"] = rank
        result["user"]["score"] = yield gen.Task(r.zscore, "rank:" + appname, uid)
        print result
        self.finish(json.dumps(result))

    @tornado.web.asynchronous
    @gen.engine
    def post(self):

        appname = self.get_argument("appname", "unknow")
        uid = self.get_argument("uid", 0)
        score = int(self.get_argument("score", 0))
        print appname, uid, score
        r = Redis(connection_pool=CONNECTION_POOL)
        ok = yield gen.Task(r.zadd, "rank:" + appname, score, uid)
        self.finish("post:" + str(ok))

    @tornado.web.asynchronous
    @gen.engine
    def put(self):
        appname = self.get_argument("appname", "unknow")
        uid = self.get_argument("uid", 0)
        score = int(self.get_argument("score", 0))

        r = Redis(connection_pool=CONNECTION_POOL)
        ok = yield gen.Task(r.zadd, "rank:" + appname, score, uid)
        self.finish("post:" + str(ok))

application = tornado.web.Application([
    (r"/", MainHandler),
    (r"/rank", RankHandler),
    (r"/top", TopHandler),
])



if __name__ == "__main__":
    port = 8888
    application.listen(port)
    print "listen on :", port
    tornado.ioloop.IOLoop.instance().start()