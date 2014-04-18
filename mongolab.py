import redis
import pymongo
from pymongo import MongoClient
import os
import json
from bson.objectid import ObjectId
import networkx as nx
import urlparse
from itertools import combinations, product

from clustering import clusters


# connect to MongoDB
mongolab = True
# True if connecting to MongoLab, 
# False if connecting to local MongoDB server
if mongolab:
    mongo_connection = MongoClient(os.environ['MONGOLAB_URI'])
    mongo_uri = os.environ['MONGOLAB_URI']
    parseobj = urlparse.urlparse(mongo_uri)
    dbname = parseobj.path[1:]
    db = mongo_connection[dbname]
else:
    mongo_connection = MongoClient()
    db = mongo_connection["plato_forum_development"]

proxies_collection = db['proxies']
comments_collection = db['comments']
stances_collection = db['stances']
# To test if grapher.py is up running and can connect to MongoLab

# subscribe to Redis To Go
redistogo = True
if redistogo:
    redis_url = os.environ['REDISTOGO_URL']
    rc = redis.from_url(redis_url)
else:
    rc = redis.Redis()

ps = rc.pubsub()
ps.subscribe(['jobqueue'])
