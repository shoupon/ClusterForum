import networkx as nx
import random
import sys
import unittest

# Find clusters given a weighted complete graph G, with weights on edges
# represent distance between vertices
# n: number of clusters
# w: the key to the weight attribute of graph G
def clusters(G, n, w='weight', num_iteration=8):
    # randomly pick n vertices
    centroids = random.sample(G.nodes(), n)
    for it in range(num_iteration):
        clustering = { x:[] for x in centroids }
        for v in G.nodes():
            clustering[assign_cluster(G, v, centroids, w)] += [v]
        centroids = [centroid(G, clustering[k], w) for k in clustering.keys()]
        #print clustering
        #print centroids
    return clustering

def total_dist(G, cluster, centroid, w):
    assert centroid in cluster
    return sum([G[centroid][v][w] for v in cluster if v != centroid])

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
        self.l = list(range(10))
        [self.g1.add_node(n) for n in self.l]
        [self.g1.add_edge(i,j) for (i,j) in itertools.combinations(self.l,2)]
        for (i,j) in itertools.combinations(self.l, 2):
            self.g1[i][j]['w'] = 0
        for (i,j) in itertools.combinations(self.l[:5], 2):
            self.g1[i][j]['w'] = -2
        for (i,j) in itertools.combinations(self.l[5:], 2):
            self.g1[i][j]['w'] = -2

        self.g2 = nx.complete_graph(10)
        for (i,j) in itertools.combinations(self.l,2):
            self.g2[i][j]['w'] = 50
        for (i,j) in itertools.combinations(self.l[:5], 2):
            self.g2[i][j]['w'] = 2
        for (i,j) in itertools.combinations(self.l[5:], 2):
            self.g2[i][j]['w'] = 10
        # cluster 1
        self.g2[0][2]['w'] = 1
        self.g2[1][2]['w'] = 1
        self.g2[3][2]['w'] = 1
        self.g2[4][2]['w'] = 1
        # cluster 2
        self.g2[6][7]['w'] = -1
        self.g2[7][5]['w'] = -1
        self.g2[5][6]['w'] = -2
        # cluster 3
        self.g2[8][9]['w'] = 2



    def test_assign(self):
        centroids = [0, 6]
        self.assertEqual(assign_cluster(self.g1, 0, centroids, 'w'), 0)
        self.assertEqual(assign_cluster(self.g1, 4, centroids, 'w'), 0)
        self.assertEqual(assign_cluster(self.g1, 6, centroids, 'w'), 6)
        self.assertEqual(assign_cluster(self.g1, 7, centroids, 'w'), 6)

    def test_dist(self):
        self.assertEqual(total_dist(self.g2, self.l[:5], 2, 'w'), 4)
        self.assertEqual(total_dist(self.g2, self.l[:5], 3, 'w'), 7)
        self.assertEqual(total_dist(self.g2, [7,8,9], 7, 'w'), 20)
        self.assertEqual(total_dist(self.g2, [8,9], 9, 'w'), 2)

    def test_centroid(self):
        self.assertEqual(centroid(self.g2, [0,1,2,3,4], 'w'), 2)

    def test_clustering(self):
        result = clusters(self.g2, 2, 'w')
        print result
        result = clusters(self.g2, 3, 'w')
        print result


if __name__ == '__main__':
    unittest.main()


