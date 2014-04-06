import networkx as nx
import random
import sys
import unittest

# Find clusters given a weighted complete graph G, with weights on edges
# represent distance between vertices
# n: number of clusters
# w: the key to the weight attribute of graph G
def clusters(G, n, w='weight'):
    # randomly pick n vertices
    centroids = random.sample(G.nodes(), n)

    clustering = { x:[] for x in centroids }
    for v in G.nodes():
        clustering[assign_cluster(G, v, centroids, w)] += [v]

# cluster is a list containing all the nodes assigned to the cluster
def centroid(G, cluster, w):
    """ given a vertex v in cluster, find the total distance from v to every other vertex
    >>>
    >>>
    >>>
    """
    d = {sum([G[v][n][w] for n in cluster if v!= n]):v for v in cluster}
    # should we break the ties randomly?
    return d[min(d.keys())]

def assign_cluster(G, v, centroids, w):
    m = sys.maxint
    for c in centroids:
        if v == c:
            return c
        if G[v][c][w] < m:
            m = G[v][c][w]
            ret = c
    return ret


import itertools
class TestClustering(unittest.TestCase):
    def setUp(self):
        self.g1 = nx.Graph()
        l = list(range(10))
        [self.g1.add_node(n) for n in l]
        [self.g1.add_edge(i,j) for (i,j) in itertools.combinations(l,2)]
        for (i,j) in itertools.combinations(l, 2):
            self.g1[i][j]['w'] = 0
        for (i,j) in itertools.combinations(l[:5], 2):
            self.g1[i][j]['w'] = -2
        for (i,j) in itertools.combinations(l[5:], 2):
            self.g1[i][j]['w'] = -2

        self.g2 = nx.complete_graph(10)
        for (i,j) in itertools.combinations(l,2):
            self.g2[i][j]['w'] = 10
        for (i,j) in itertools.combinations(l[:4], 2):
            self.g2[i][j]['w'] = 2
        for (i,j) in itertools.combinations(l[6:], 2):
            self.g2[i][j]['w'] = 2
        self.g2[0][2]['w'] = 1
        self.g2[1][2]['w'] = 1
        self.g2[3][2]['w'] = 1
        self.g2[4][2]['w'] = 1



    def test_assign(self):
        centroids = [0, 6]
        self.assertEqual(assign_cluster(self.g1, 0, centroids, 'w'), 0)
        self.assertEqual(assign_cluster(self.g1, 4, centroids, 'w'), 0)
        self.assertEqual(assign_cluster(self.g1, 6, centroids, 'w'), 6)
        self.assertEqual(assign_cluster(self.g1, 7, centroids, 'w'), 6)

    def test_centroid(self):
        A
        self.assertEqual(centroid(self.g2, [0,1,2,3,4], 'w'), 2)

if __name__ == '__main__':
    unittest.main()


