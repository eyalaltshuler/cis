import os
import csv
import optparse
import pyspark
import math
import logging


def sample(dataset, datasetSize, fraction):
    return dataset.sample(False, fraction).collect()

def cis(dataset, itemset):
    filterFunc = lambda t: itemset.issubset(t)
    filteredDataset = dataset.filter(filterFunc)
    return filteredDataset.reduce(lambda a,b: a.intersection(b))

def countElements(dataset):
    counts = dataset.flatMap(lambda t: [(e,1) for e in t]).reduceByKey(lambda a,b: a+b).collect()
    return {k:v for (k,v) in counts}

def workerMap(v):
    def closure(key, value, P):
        if key <= v:
            transactions = [set(transaction) for transaction in value]
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
DELTA = 0.1

def requiredSampleSize(n, pSize, epsilon=0.1):
    return (2 * n * math.log(1 / epsilon)) / (pSize * (DELTA ** 2) * (1 - ALPHA))


def requiredNumOfWorkers(numOfTransactions, pSize, workersNum, epsilon=0.1):
    workerSize = numOfTransactions / workersNum
    sampleSize = requiredSampleSize(numOfTransactions, pSize, epsilon)
    return min(int(math.ceil(sampleSize / workerSize)), workersNum)


def workersRequired(n, workersNum, threshold, epsilon):
    newThreshold = threshold / ALPHA
    return requiredNumOfWorkers(n, newThreshold, workersNum, epsilon)