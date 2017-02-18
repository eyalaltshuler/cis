from pyspark import SparkContext, SparkConf
from cis import apriori

threshold = 100
num_of_partitions = 10

def support(s):
    pass


def count_candidates(x):
    pass


def secondReduceFunc(a ,b):
    return a+b


def secondMapFunc(x):
    count_candidates(x)


def firstReduceFunc(a, b):
    return a.union(b)


def firstMapFunc(x):
    L, supportData = apriori.apriori(x, threshold / num_of_partitions)
    return L


def son(dataset):
    sc = SparkContext()
    sc.parallelize(dataset)

    first_map = sc.map(firstMapFunc)
    first_reduce = first_map.reduce(firstReduceFunc)
    second_map = first_reduce.secondMap(secondMapFunc)
    second_reduce = second_map.reduce(secondReduceFunc)
    result = second_reduce.collect()
    for r in result:
        if support(r) < threshold:
            result.remove(r)

if __name__ == "__main__":
    son()