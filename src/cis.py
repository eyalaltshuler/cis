import os
import csv
import optparse
import pyspark


class Setup(object):
    def __init__(self, inputData, numOfWorkers, itemSpaceSize):
        self._n = itemSpaceSize
        self._input = self._parse(inputData)
        self._numOfWorkers = numOfWorkers

    def estimateSetCardinality(self, s):
        return len(s) * self._numOfWorkers

    def estimateCis(self):
        pass

    def calculateCis(self):
        pass

    def getChildren(self, s):
        if len(s) == self._n:
            return
        l = list(s)
        l.sort()
        minItem = l.pop(0)
        if not l:
            for i in xrange(minItem):
                yield set([i, minItem])
        else:
            tmp = l
            secondSmallestItem = l.pop(0)
            for i in xrange(minItem, secondSmallestItem):
                yield set([minItem, i] + tmp)


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
