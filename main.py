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

        result["top"] = yield gen.Task(r.zrevrange, "rank:%s" % appname, 0, 10, "withscore")
        print result
        self.finish(json.dumps(result))

class TimesHandler(tornado.web.RequestHandler):

    maxtimes = 5

    oncewaittime = 10 * 60

    @tornado.web.asynchronous
    @gen.engine
    def get(self):
        appname = self.get_argument("appname", "unknow")
        uid = self.get_argument("uid", 0)
        r = Redis(connection_pool=CONNECTION_POOL)
        result = {}

        ttl = yield gen.Task(r.ttl, "times:%s:%s" % (appname, uid))
        if ttl > 0:
            result["times"] = self.maxtimes - (ttl / self.oncewaittime + 1)
            result["wait"] = ttl % self.oncewaittime
            pass
        else :
            result["times"] = self.maxtimes
            result["wait"] = 0
        print result
        self.finish(json.dumps(result))

    @tornado.web.asynchronous
    @gen.engine
    def put(self):
        appname = self.get_argument("appname", "unknow")
        uid = self.get_argument("uid", 0)
        r = Redis(connection_pool=CONNECTION_POOL)
        result = {}

        if not (yield gen.Task(r.exists, "times:%s:%s" % (appname, uid))):
            yield gen.Task(r.set, "times:%s:%s" % (appname, uid), 0)

        ttl = yield gen.Task(r.ttl, "times:%s:%s" % (appname, uid))
        ttl = ttl or 0
        if ttl > 4 * self.oncewaittime:
            result["code"] = -1
            result["msg"] = "failed"
        else :
            code = yield gen.Task(r.expire, "times:%s:%s" % (appname, uid), ttl + self.oncewaittime)
            result["code"] = 0
            result["msg"] = "ok"
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

        result["top"] = yield gen.Task(r.zrevrange, "rank:%s" % appname, 0, 10, "withscore")
        result["user"] = {}
        rank = (yield gen.Task(r.zrevrank, "rank:%s" % appname, uid))
        if rank != None:
            rank+=1
            pass
        result["user"]["rank"] = rank
        result["user"]["score"] = yield gen.Task(r.zscore, "rank:%s" % appname, uid)
        print result
        self.finish(json.dumps(result))

    @tornado.web.asynchronous
    @gen.engine
    def post(self):

        appname = self.get_argument("appname", "unknow")
        uid = self.get_argument("uid", 0)
        score = int(self.get_argument("score", 0))
        r = Redis(connection_pool=CONNECTION_POOL)

        ok = yield gen.Task(r.zadd, "rank:%s" % appname, score, uid)
        result = {}

        result["user"] = {}
        rank = (yield gen.Task(r.zrevrank, "rank:%s" % appname, uid))
        if rank != None:
            rank+=1
            pass
        result["user"]["rank"] = rank
        print result
        self.finish(json.dumps(result))

    @tornado.web.asynchronous
    @gen.engine
    def put(self):
        appname = self.get_argument("appname", "unknow")
        uid = self.get_argument("uid", 0)
        score = int(self.get_argument("score", 0))

        r = Redis(connection_pool=CONNECTION_POOL)
        ok = yield gen.Task(r.zadd, "rank:%s" % appname, score, uid)
        self.finish("post:" + str(ok))

application = tornado.web.Application([
    (r"/", MainHandler),
    (r"/rank", RankHandler),
    (r"/top", TopHandler),
    (r"/times", TimesHandler),

])



if __name__ == "__main__":
    port = 8888
    application.listen(port)
    print "listen on :", port
    tornado.ioloop.IOLoop.instance().start()