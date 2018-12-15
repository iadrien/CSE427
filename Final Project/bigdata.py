from pyspark import SparkContext
import numpy as np
import sys
from math import radians, sin, cos


def closest_point(p, ps, m):
    """
    given a (latitude/longitude) point
    and an array of current center points
    returns the index in the array of
    the center closest to the given poin
    :param p:  given point
    :param ps: center points
    :param m:  distance functions
    :return:   the index of the closest center point
    """
    ps = np.asarray(ps)
    return np.argmin(m(p, ps))


def add_points(p1, p2):
    """
    given two points, return a point
    which is the sum of the two points.
    """
    return p1[0] + p2[0], p1[1] + p2[1]


def euclidean_distance(p1, p2):
    """
    calculate the eculidean distance between two points
    :param p1: pair of coordinate
    :param p2: pair of coordinate
    :return: eculidean distance
    """
    return np.sqrt(np.sum((np.array(p2) - np.array(p1)) ** 2, axis=1))


def great_circle_distance(p1, p2):
    lat1, lon1 = radians(p1[1]), radians(p1[0])
    lat2, lon2 = np.radians(p2[:, 1]), np.radians(p2[:, 0])

    sin_lat1, cos_lat1 = sin(lat1), cos(lat1)
    sin_lat2, cos_lat2 = np.sin(lat2), np.cos(lat2)

    d_lon = np.subtract(lon2, lon1)
    cos_d_lon, sin_d_lon = np.cos(d_lon), np.sin(d_lon)

    return np.arctan2(np.sqrt((cos_lat2 * sin_d_lon) ** 2 +
                              (cos_lat1 * sin_lat2 -
                               sin_lat1 * cos_lat2 * cos_d_lon) ** 2),
                      sin_lat1 * sin_lat2 + cos_lat1 * cos_lat2 * cos_d_lon)


def WCSS(ps, cps):
    """
    Within-Clusters Sum-of-Squares measure
    """
    return sum(np.sum((cps[i] - p) ** 2) for (i, p) in ps)


def kmeans_cluster(data, converge_dist, m, k):
    cp = data.takeSample(False, k, 20181214)
    temp_dist = 1.0
    while temp_dist > converge_dist:
        closest = data.map(lambda p: (closest_point(p, cp, m), (p, 1)))
        point_stats = closest.reduceByKey(lambda p1, p2: (add_points(p1[0], p2[0]), p1[1] + p2[1])).map(
            lambda o: (o[0], np.array(o[1][0]) / o[1][1])).collect()

        temp_dist = WCSS(point_stats, cp)
        for (i, p) in point_stats:
            cp[i] = p
    clusters = data.map(lambda p: (closest_point(p, cp, m), p)).groupByKey().map(lambda o: (o[0], list(o[1])))
    return cp, clusters


if __name__ == "__main__":
    if len(sys.argv) != 5:
        exit(-1)
    sc = SparkContext()
    data = sc.textFile(sys.argv[1]).map(lambda o: o[1: len(o) - 1]).map(lambda o: o.split(',')).map(
        lambda o: (float(o[0]), float(o[1]))).cache()
    if sys.argv[3] == "eu":
        m = euclidean_distance
    else:
        m = great_circle_distance
    cp, output = kmeans_cluster(data, 0.01, m, sys.argv[4])
    output.saveAsTextFile(sys.argv[2])
    np.savetxt('cp.txt', cp)
    sc.stop()
