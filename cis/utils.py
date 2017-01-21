import os
import csv
import optparse
import pyspark
import math


def workerMap(v):
    def closure(key, value, P):
        if key <= v:
            transactions = [set(transacation) for transaction in value]
            result = transactions[0]
            for transaction in transactions:
                result.intersection(transaction)
            return result.intersection(P)
        return []
    return closure(v)


def paarllelClosure(v):
    return workerMap(v)


def closure(dataset):
    return reduce(set.intersection, dataset)


ALPHA = 0.1

def requiredSampleSize(n, pSize, epsilon=0.1):
    return (2 * n * math.log(1 / epsilon)) / (pSize * (DELTA ** 2) * (1 - ALPHA))


def requiredNumOfWorkers(n, pSize, workersNum, epsilon=0.1):
    workerSize = n / workersNum
    sampleSize = requiredSampleSize(n, pSize, epsilon)
    return int(math.ceil(sampleSize / workerSize))


def workersRequired(n, workersNum, threshold, epsilon):
    newThreshold = threshold / ALPHA
    return requiredNumOfWorkers(n, newThreshold, workersNum, epsilon)


if __name__=="__main__":
    data = readData()
    partitions = getPartitionsNum()
    data = convertDataToKeyValuePairs()
    conf = pyspark.SparkConf()
    master = 'local[%s]' % workers
    conf.setMaster(master)
    sc = pyspark.SparkContext(conf=conf)
    P = getP()
    tmp = sc.parallelize(data).map(closureFunc)
    Q = sum(sc.parallelize(data).map(closureFunc).collect())
    closureFunc
    approximatedResult = closure(1)
    realResult = closure(Partitions)
