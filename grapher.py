import redis
import pymongo
from pymongo import MongoClient
import os
import json
from bson.objectid import ObjectId
import networkx as nx
import urlparse
from itertools import combinations, product
from operator import itemgetter
from multiprocessing import Process

from clustering import clusters

Gs = {}
yesno_topics = set()
open_topics = set()
count = {}
PERIOD = 10 # run clustering every 5 updates
NUM_CLUSTERS = 3
MULTIPLIER = 3 # the supporting/opposing +/- MULTIPLIER*delta
delta = 1
DELTA = delta
MONGOLAB = True

# connect to MongoDB
# remote = True if connecting to MongoLab, 
#          False if connecting to local MongoDB server
def connect_mongodb(remote=False):
    if remote:
        mongo_connection = MongoClient(os.environ['MONGOLAB_URI'])
        mongo_uri = os.environ['MONGOLAB_URI']
        parseobj = urlparse.urlparse(mongo_uri)
        dbname = parseobj.path[1:]
        db = mongo_connection[dbname]
    else: 
        mongo_connection = MongoClient()
        db = mongo_connection["plato_forum_development"]
    # To test if successfully connects to MongoLab
    #jobs_collection = db['jobs']
    #job = {"message": "new connection is established"}
    #print jobs_collection.insert(job)
    return db

db = connect_mongodb(MONGOLAB)
proxies_collection = db['proxies']
comments_collection = db['comments']
stances_collection = db['stances']
topics_collection = db['topics']


def adjust_preference(G, pid, cid, offset):
    proxy = proxies_collection.find_one({"_id":ObjectId(pid)})
    if not proxy:
        print 'Proxy not found: ' + str(ObjectId(pid))
        return
    #works = comments_collection.find({"owner_id":ObjectId(pid)})
    #for widobj in works:
    #    print widobj['_id']
    #    print str(widobj['_id'])
    #    G[str(widobj['_id'])][cid]['weight'] -= offset
    if 'approval_ids' in proxy.keys():
        for aidobj in proxy['approval_ids']:
            G[str(aidobj)][cid]['weight'] -= offset
    if 'disapproval_ids' in proxy.keys():
        for didobj in proxy['disapproval_ids']:
            G[str(didobj)][cid]['weight'] += offset

def like(G, pid, cid):
    global delta
    adjust_preference(G, pid, cid, delta)
def unlike(G, pid, cid):
    global delta
    adjust_preference(G, pid, cid, -delta)

def dislike(G, pid, cid):
    unlike(G, pid, cid)

def undislike(G, pid, cid):
    like(G, pid, cid)

def create(G, pid, cid):
    G.add_node(cid)
    for node in G.nodes():
        G.add_edge(cid, node)
        G[cid][node]['weight'] = 0
    like(G, pid, cid)

def support(G, pid, cid):
    print 'support'
    G[pid][cid]['weight'] -= MULTIPLIER*DELTA

def oppose(G, pid, cid):
    print 'oppose'
    G[pid][cid]['weight'] -= MULTIPLIER*DELTA

# stances are given in the form of clusters (dictionary)
def update_stances(topic_id, stances):
    used = set()
    for comments in stances.values():
        stance_counts = {x+1:0 for x in range(NUM_CLUSTERS)}
        for cid in comments:
            c = comments_collection.find_one({"_id":ObjectId(cid)})
            s = stances_collection.find_one({'_id':c['stance_id']})
            stance_counts[s['number']] += 1
        while True:
            i = max(stance_counts.iteritems(), key=itemgetter(1))[0]
            if i in used:
                stance_counts[i] = -1
                continue
            else:
                used.add(i)
                break

        # get stance document with number i
        stance = stances_collection.find_one({"topic_id":ObjectId(topic_id), "number":i})
        if stance:
            sidobj = stance["_id"]
        else:
            sidobj = stances_collection.insert({"topic_id":ObjectId(topic_id), "number":i})

        for cid in comments:
            comments_collection.update({"_id":ObjectId(cid)}, {"$set":{"stance_id":sidobj}})
        i += 1

def run_cluster(G0, tid):
    print 'Clustering worker spawned'
    if hasattr(os, 'getppid'):
        print 'parent process:', os.getppid()
    print 'process id:', os.getpid()
    G = G0.copy()

    if len(G.nodes()) < NUM_CLUSTERS:
        stances = clusters(G, 1, 'weight') 
    else:
        stances = clusters(G, NUM_CLUSTERS, 'weight') 
    update_stances(tid, stances)

def process_job(job):
    global Gs
    #print 'Job: ' + str(job)

    tid = job['group']['$oid']
    if job['who']:
        pid = job['who']['$oid']
    elif job['target']:
        pid = job['target']['$oid']
    cid = job['post']['$oid']
    if tid not in Gs.keys():
        Gs[tid] = nx.Graph()
    if tid not in count.keys():
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
    elif y == 'support':
        support(Gs[tid], pid, cid)
    elif y == 'oppose':
        oppose(Gs[tid], pid, cid)
    else:
        # should throw an exception here
        pass

    count[tid] += 1
    if count[tid] >= PERIOD:
        p = Process(target=run_cluster, args=(Gs[tid],tid))
        p.start()
        count[tid] = 0


# for debugging
def graph_status(G):
    print [(n,G[n]) for n in G.nodes()]

def is_yesno(tid):
    if tid in yesno_topics:
        #print 'Yesno topic from set'
        return True
    else:
        if tid in open_topics:
            #print 'Open topic from set'
            return False
        else:
            t = topics_collection.find_one({"_id":ObjectId(tid)})
            ttype = t['topic_type']
            if ttype == 'yesno':
                yesno_topics.add(tid)
                #print 'Yesno topic from MongoDB'
                return True 
            elif ttype == 'open':
                open_topics.add(tid)
                #print 'Open topic from MongoDB'
                return False
            else:
                print 'Unknown topic'
                # should throw exception here
                return False
 

print 'Build graph from current content in MongoDB'
comments = comments_collection.find()
for c in comments:
    tid = str(c['topic_id'])
    if is_yesno(tid):
        continue
    cid = str(c['_id'])
    if tid not in Gs.keys():
        Gs[tid] = nx.Graph()
    Gs[tid].add_node(cid)
    for n in Gs[tid].nodes():
        Gs[tid].add_edge(n, cid)
        Gs[tid][n][cid]['weight'] = 0

comments = comments_collection.find()
for c in comments:
    tid = str(c['topic_id'])
    if is_yesno(tid):
        continue
    cid = str(c['_id'])
    if 'opposing_ids' in c.keys():
        for oidobj in c['opposing_ids']:
            Gs[tid][str(oidobj)][cid]['weight'] += MULTIPLIER*DELTA
    if 'supporting_ids' in c.keys():
        for sidobj in c['supporting_ids']:
            Gs[tid][str(sidobj)][cid]['weight'] -= MULTIPLIER*DELTA


proxies = proxies_collection.find()
for p in proxies:
    tid = str(p['topic_id'])
    if is_yesno(tid):
        continue
    if 'approval_ids' in p.keys():
        for (ai, aj) in combinations(p['approval_ids'], 2):
            Gs[tid][str(ai)][str(aj)]['weight'] -= delta
    if 'disapproval_ids' in p.keys():
        for (di, dj) in combinations(p['disapproval_ids'], 2):
            Gs[tid][str(di)][str(dj)]['weight'] -= delta
    if 'approval_ids' in p.keys() and 'disapproval_ids' in p.keys():
        for (ai, di) in product(p['approval_ids'],p['disapproval_ids']):           
            Gs[tid][str(ai)][str(di)]['weight'] += delta

print 'Assigning initial stances'
for tid in Gs.keys():
    if len(Gs[tid].nodes()) < NUM_CLUSTERS:
        update_stances(tid, clusters(Gs[tid], 1, 'weight'))
    else:
        update_stances(tid, clusters(Gs[tid], NUM_CLUSTERS, 'weight'))

# subscribe to Redis To Go
redistogo = True
if redistogo:
    redis_url = os.environ['REDISTOGO_URL']
    rc = redis.from_url(redis_url)
else:
    rc = redis.Redis()

ps = rc.pubsub()
ps.subscribe(['jobqueue'])

print 'Listen from redis pub/sub'
for item in ps.listen():
    if item['type'] == 'message':
        job = json.loads(item['data'])
        process_job(job)
