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

Gs = {}
count = {}
PERIOD = 5 # run clustering every 5 updates
NUM_CLUSTERS = 3
delta = 1

def adjust_preference(G, pid, cid, offset):
    print 'before adjustment'
    #graph_status(G)
    proxy = proxies_collection.find_one({"_id":ObjectId(pid)})
    #works = comments_collection.find({"owner_id":ObjectId(pid)})
    #for widobj in works:
    #    print widobj['_id']
    #    print str(widobj['_id'])
    #    G[str(widobj['_id'])][cid]['weight'] -= offset
    if 'approval_ids' in proxy.keys():
        for aidobj in proxy['approval_ids']:
            print 'approval'
            print str(aidobj)
            print cid
            G[str(aidobj)][cid]['weight'] -= offset
    if 'disapproval_ids' in proxy.keys():
        for didobj in proxy['disapproval_ids']:
            print 'disapproval'
            G[str(didobj)][cid]['weight'] += offset
    print 'after adjustment'
    #graph_status(G)

def like(G, pid, cid):
    print 'like'
    global delta
    adjust_preference(G, pid, cid, delta)
def unlike(G, pid, cid):
    print 'unlike'
    global delta
    adjust_preference(G, pid, cid, -delta)

def dislike(G, pid, cid):
    print 'dislike'
    unlike(G, pid, cid)

def undislike(G, pid, cid):
    print 'undislike'
    like(G, pid, cid)

def create(G, pid, cid):
    print 'create'
    G.add_node(cid)
    for node in G.nodes():
        G.add_edge(cid, node)
        G[cid][node]['weight'] = 0
    like(G, pid, cid)

# stances are given in the form of clusters (dictionary)
def update_stances(topic_id, stances):
    for comments in stances.values():
        i = 0
        for cid in comments:
            comments_collection.update({"_id": cid}, {"$set": "stance":i})
        i += 1


def process_job(job):
    global Gs
    tid = job['group']['$oid']
    pid = job['who']['$oid']
    cid = job['post']['$oid']
    if tid not in Gs.keys():
        Gs[tid] = nx.Graph()
        count[tid] = 0

    y = job['action']
    if y == 'like':
        like(Gs[tid], pid, cid)
    elif y == 'unlike':
        unlike(Gs[tid], pid, cid)
    elif y == 'dislike':
        dislike(Gs[tid], pid, cid)
    elif y == 'undislike':
        undislike(Gs[tid], pid, cid)
    elif y == 'create':
        create(Gs[tid], pid, cid)
    else:
        # should throw an exception here
        pass

    count[tid] += 1
    if count[tid] >= PERIOD:
        stances = clusters(Gs[tid], NUM_CLUSTERS, 'weight') 
        update_stances(tid, stances)
        count[tid] = 0

# for debugging
def graph_status(G):
    print [(n,G[n]) for n in G.nodes()]


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
# To test if grapher.py is up running and can connect to MongoLab
#jobs_collection = db['jobs']
#job = {"message": "grapher.py is running"}
#print jobs_collection.insert(job)

# Build graph from current content in MongoDB
comments = comments_collection.find()
for c in comments:
    tid = str(c['target_id'])
    cid = str(c['_id'])
    if tid not in Gs.keys():
        Gs[tid] = nx.Graph()
    Gs[tid].add_node(cid)
    for n in Gs[tid].nodes():
        Gs[tid].add_edge(n, cid)
        Gs[tid][n][cid]['weight'] = 0

proxies = proxies_collection.find()
for p in proxies:
    tid = str(p['topic_id'])
    if 'approval_ids' in p.keys():
        for (ai, aj) in combinations(p['approval_ids'], 2):
            Gs[tid][str(ai)][str(aj)]['weight'] -= delta
    if 'disapproval_ids' in p.keys():
        for (di, dj) in combinations(p['disapproval_ids'], 2):
            Gs[tid][str(di)][str(dj)]['weight'] -= delta
    if 'approval_ids' in p.keys() and 'disapproval_ids' in p.keys():
        for (ai, di) in product(p['approval_ids'],p['disapproval_ids']):            
            Gs[tid][str(ai)][str(di)]['weight'] += delta

# Compute initial clusters
for tid in Gs.keys():
    update_stances(tid, clusters(Gs[tid], NUM_CLUSTERS, 'weight'))

#for (x,y) in Gs.items():
#    print x
#    graph_status(y)

# subscribe to Redis To Go
redistogo = True
if redistogo:
    redis_url = os.environ['REDISTOGO_URL']
    rc = redis.from_url(redis_url)
else:
    rc = redis.Redis()

ps = rc.pubsub()
ps.subscribe(['jobqueue'])

for item in ps.listen():
    if item['type'] == 'message':
        job = json.loads(item['data'])
        process_job(job)
