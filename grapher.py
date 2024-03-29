import redis
import pymongo
from pymongo import MongoClient
import os
import json
from bson.objectid import ObjectId
import networkx as nx
import heapq
import urlparse
from itertools import combinations, product
from operator import itemgetter
from multiprocessing import Process
import time

from clustering import clusters

Gs = {}
Ds = {}
yesno_topics = set()
open_topics = set()
count = {}
PERIOD = 10 # run clustering every 5 updates
NUM_CLUSTERS = 3
NUM_SUGGESTIONS = 5
WEIGHT = 'weight'
MULTIPLIER = 6 # the supporting/opposing +/- MULTIPLIER*delta
delta = 1
DELTA = delta
DISLIKE_W = 2
OPPOSE_W = 2
SUPPORT_W = 6
CREATE_W = 1.5

TRACK_STANCES = False
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
 

def adjust_undirected(G, pid, cid, offset):
    #for e in D.edges():
    #    print e
    proxy = proxies_collection.find_one({"_id":ObjectId(pid)})
    if not proxy:
        print 'Proxy not found: ' + str(ObjectId(pid))
        return

    if 'approval_ids' in proxy.keys():
        for aidobj in proxy['approval_ids']:
            G[str(aidobj)][cid]['weight'] -= offset
    if 'disapproval_ids' in proxy.keys():
        for didobj in proxy['disapproval_ids']:
            G[str(didobj)][cid]['weight'] += offset

def adjust_directed(D, pid, cid, upvote=True):
    works = comments_collection.find({"owner_id":ObjectId(pid)})
    for w in works:
        widobj = w['_id']
        if upvote:
            #print 'add_edge: ' + str(widobj) + ',' + str(cid)
            D.add_edge(str(widobj), cid) 
        else:
            if WEIGHT not in D[str(widobj)][cid].keys():
                #print 'remove_edge: ' + str(widobj) + ',' + str(cid)
                D.remove_edge(str(widobj), cid) 


def like(G, D, pid, cid):
    global delta
    adjust_undirected(G, pid, cid, DELTA)
    adjust_directed(D, pid, cid, upvote=True)

def unlike(G, D, pid, cid):
    global delta
    adjust_undirected(G, pid, cid, -DELTA)
    adjust_directed(D, pid, cid, upvote=False)

def dislike(G, D, pid, cid):
    global delta
    adjust_undirected(G, pid, cid, -DELTA*DISLIKE_W)

def undislike(G, D, pid, cid):
    global delta
    adjust_undirected(G, pid, cid, DELTA*DISLIKE_W)

def create(G, D, pid, cid):
    G.add_node(cid)
    for node in G.nodes():
        G.add_edge(cid, node)
        G[cid][node]['weight'] = 0
    adjust_undirected(G, pid, cid, delta)
    proxy = proxies_collection.find_one({"_id":ObjectId(pid)})
    if not proxy:
        print 'Proxy not found: ' + str(ObjectId(pid))
        return
    if 'approval_ids' in proxy.keys():
        for aidobj in proxy['approval_ids']:
            D.add_edge(cid, str(aidobj))
    #works = comments_collection.find({"owner_id":ObjectId(pid)})
    #for w in works:
    #    widobj = w['_id']
    #    D.add_edge(cid, str(widobj))
    #    D[cid][str(widobj)][WEIGHT] = CREATE_W
    #    D.add_edge(str(widobj), cid)
    #    D[str(widobj)][cid][WEIGHT] = CREATE_W

def support(G, D, pid, cid):
    G[pid][cid]['weight'] -= MULTIPLIER*DELTA
    D.add_edge(cid, pid)
    D[cid][pid][WEIGHT] = SUPPORT_W

def oppose(G, D, pid, cid):
    G[pid][cid]['weight'] -= MULTIPLIER*DELTA
    D.add_edge(cid, pid)
    D[cid][pid][WEIGHT] = OPPOSE_W

# stances are given in the form of clusters (dictionary)
def update_stances(tid, stances, ranking):
    start_time = time.time()
    if is_yesno(tid):
        for comments in stances.values():
            for cid in comments:
                comments_collection.update({"_id":ObjectId(cid)},
                                           {"$set":{"importance_factor":ranking[cid]}})
        return

    used = set()
    for comments in stances.values():
        stance_counts = {x+1:0 for x in range(NUM_CLUSTERS)}
        for cid in comments:
            c = comments_collection.find_one({"_id":ObjectId(cid)})
            s = stances_collection.find_one({'_id':c['stance_id']})
            #num = s['number']
            #if num == 3:
            #    num = 2
            #stance_counts[num] += 1
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
        stance = stances_collection.find_one({"topic_id":ObjectId(tid), "number":i})
        if stance:
            sidobj = stance["_id"]
        else:
            sidobj = stances_collection.insert({"topic_id":ObjectId(tid), "number":i})

        for cid in comments:
            comments_collection.update({"_id":ObjectId(cid)}, 
                                       {"$set":{"stance_id":sidobj, 
                                                "importance_factor":ranking[cid]}})
    print 'Updating stances costs (updating database)'
    print time.time() - start_time, "seconds"

def update_neighbors(G):
    for c in G.nodes():
        dist = {nbr:G[c][nbr][WEIGHT] for nbr in G.neighbors(c) if nbr != c}
        #print dist
        slist = heapq.nsmallest(NUM_SUGGESTIONS, dist, dist.__getitem__)
        sobjlist = [ObjectId(x) for x in slist]
        #print {x:dist[x] for x in slist}
        comments_collection.update({"_id":ObjectId(c)},
                                   {'$set':{'neighbors':[]}})
        comments_collection.update({"_id":ObjectId(c)},
                                   {'$push':{'neighbors':{'$each':sobjlist}}})

def update_ranking(ranking):
    for cid in ranking.keys():
        comments_collection.update({"_id":ObjectId(cid)},
                                   {"$set":{"importance_factor":ranking[cid]}})

def run_cluster(G0, D0, tid):
    print 'Clustering worker spawned'
    if hasattr(os, 'getppid'):
        print 'parent process:', os.getppid()
    print 'process id:', os.getpid()
    G = G0.copy()
    D = D0.copy()

    start_time = time.time()
    update_neighbors(G)
    print 'Updating nearest K-neighbors costs'
    print time.time() - start_time, "seconds"

    if TRACK_STANCES:
        start_time = time.time()
        if len(G.nodes()) < NUM_CLUSTERS:
            stances = clusters(G, 1, 'weight') 
        else:
            stances = clusters(G, NUM_CLUSTERS, 'weight') 
        print 'Clustering costs (k-mean)'
        print time.time() - start_time, "seconds"

    start_time = time.time()
    ranking = nx.pagerank(D)
    print 'PageRanking costs'
    print time.time() - start_time, "seconds"
    
    if TRACK_STANCES:
        update_stances(tid, stances, ranking)
    else:
        update_ranking(ranking)

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
        Ds[tid] = nx.DiGraph()
    if tid not in count.keys():
        count[tid] = 0

    y = job['action']
    if y == 'like':
        like(Gs[tid], Ds[tid], pid, cid)
    elif y == 'unlike':
        unlike(Gs[tid], Ds[tid], pid, cid)
    elif y == 'dislike':
        dislike(Gs[tid], Ds[tid], pid, cid)
    elif y == 'undislike':
        undislike(Gs[tid], Ds[tid], pid, cid)
    elif y == 'create':
        create(Gs[tid], Ds[tid], pid, cid)
    elif y == 'support':
        support(Gs[tid], Ds[tid], pid, cid)
    elif y == 'oppose':
        oppose(Gs[tid], Ds[tid], pid, cid)
    else:
        # should throw an exception here
        pass

    count[tid] += 1
    if count[tid] >= PERIOD:
        p = Process(target=run_cluster, args=(Gs[tid],Ds[tid],tid))
        p.start()
        count[tid] = 0


# for debugging
def graph_status(G):
    print [(n,G[n]) for n in G.nodes()]


print 'Build graph from current content in MongoDB'
start_time = time.time()
comments = comments_collection.find()
for c in comments:
    tid = str(c['topic_id'])
    cid = str(c['_id'])
    if tid not in Gs.keys():
        Gs[tid] = nx.Graph()
        Ds[tid] = nx.DiGraph()
    Gs[tid].add_node(cid)
    Ds[tid].add_node(cid)
    for n in Gs[tid].nodes():
        Gs[tid].add_edge(n, cid)
        Gs[tid][n][cid]['weight'] = 0

comments = comments_collection.find()
for c in comments:
    tid = str(c['topic_id'])
    cid = str(c['_id'])
    # Build DiGraph for page ranking
    if 'opposing_ids' in c.keys():
        for oidobj in c['opposing_ids']:
            Ds[tid].add_edge(cid, str(oidobj))
            Ds[tid][cid][str(oidobj)][WEIGHT] = OPPOSE_W
    if 'supporting_ids' in c.keys():
        for sidobj in c['supporting_ids']:
            Ds[tid].add_edge(cid, str(sidobj))
            Ds[tid][cid][str(sidobj)][WEIGHT] = SUPPORT_W
    # Build weighted graph for clustering
    if is_yesno(tid):
        continue
    if 'opposing_ids' in c.keys():
        for oidobj in c['opposing_ids']:
            Gs[tid][str(oidobj)][cid]['weight'] += MULTIPLIER*DELTA
    if 'supporting_ids' in c.keys():
        for sidobj in c['supporting_ids']:
            Gs[tid][str(sidobj)][cid]['weight'] -= MULTIPLIER*DELTA

proxies = proxies_collection.find()
for p in proxies:
    tid = str(p['topic_id'])
    # Build DiGraph for page ranking
    # like relation
    works = comments_collection.find({"owner_id":p['_id']})
    if 'approval_ids' in p.keys():
        for w in works:
            wobj = w['_id']
            for aobj in p['approval_ids']:
                Ds[tid].add_edge(str(wobj), str(aobj))
    # Build weighted graph for clustering
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
print time.time() - start_time, " seconds"

print 'Assigning initial stances: and importance factors'
for tid in Gs.keys():
    update_neighbors(Gs[tid])
    if TRACK_STANCES:
        if len(Gs[tid].nodes()) < NUM_CLUSTERS:
            update_stances(tid,
                           clusters(Gs[tid], 1, WEIGHT),
                           nx.pagerank(Ds[tid]))
        else:
            update_stances(tid, 
                           clusters(Gs[tid], NUM_CLUSTERS, WEIGHT),
                           nx.pagerank(Ds[tid]))
    else:
        update_ranking(nx.pagerank(Ds[tid]))

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
