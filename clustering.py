import networkx as nx
import random
import sys
import unittest

# Find clusters given a weighted graph G, with weights on edges
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
    """
    >>> g = nx.Graph()
    >>> l = list(range(10))
    >>> [g.add_node(n) for n in l]
    >>> [g.add_edge(i,j) for (i,j) in itertools.combinations(l,2)]
    >>> for (i,j) in itertools.combinations(l[:5], 2):
    >>>   g[i][j]['w'] = -2
    >>> for (i,j) in itertools.combinations(l[5:], 2):
    >>>   g[i][j]['w'] = -2
    >>> centroids = [0, 6]
    >>> assign_cluster(g, 0, centroids, 'w')
    0
    >>> assign_cluster(g, 4, centroids, 'w')
    0
    >>> assign_cluster(g, 6, centroids, 'w')
    6
    >>> assign_cluster(g, 7, centroids, 'w')
    6
    """
    m = sys.maxint
    for c in centroids:
        if v == c:
            return c
        if G[v][c][w] < m:
            m = G[v][c][w]
            ret = c
    return ret

if __name__ == '__main__':
    import doctest
    doctest.testmod()
