import os
import csv
import optparse
import pyspark
import math
import logging
import numpy
from itertools import combinations


NEWLINE = '\n'
ALPHA = 0.1
DELTA = 0.1


def sample(dataset, datasetSize, fraction):
    return dataset.sample(False, fraction).collect()


def cis(dataset, itemset):
    filterFunc = lambda t: itemset.issubset(t)
    filteredDataset = dataset.filter(filterFunc)
    return filteredDataset.count(), filteredDataset.reduce(lambda a,b: a.intersection(b))


def countElements(dataset):
    counts = dataset.flatMap(lambda t: [(e,1) for e in t]).reduceByKey(lambda a,b: a+b).collect()
    return {k:v for (k,v) in counts}


def countElementsInSample(dataset):
    res = {}
    for transaction in dataset:
        for element in transaction:
            value = res.get(element)
            res[element] = value + 1 if value is not None else 1
    return res


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


def requiredSampleSize(n, pSize, epsilon=0.1):
    return (2 * n * math.log(1 / epsilon)) / (pSize * (DELTA ** 2) * (1 - ALPHA))


def requiredNumOfWorkers(numOfTransactions, pSize, workersNum, epsilon=0.1, sample=True, one=False):
    if not sample:
        return workersNum
    if one:
        return 1
    workerSize = numOfTransactions / workersNum
    sampleSize = requiredSampleSize(numOfTransactions, pSize, epsilon)
    return min(int(math.ceil(float(sampleSize) / workerSize)), workersNum)


def workersRequired(n, workersNum, threshold, epsilon):
    newThreshold = threshold / ALPHA
    return requiredNumOfWorkers(n, newThreshold, workersNum, epsilon)


def generate_transaction():
    return set(numpy.random.zipf(1.1, 50))


def format_out(transaction):
    return " ".join([str(i) for i in transaction]) + NEWLINE


def generate_data(path, num_transactions):
    with open(path, 'w') as f:
        for i in xrange(num_transactions / 100):
            for j in xrange(100):
                data = [format_out(generate_transaction()) for _ in xrange(100)]
                f.writelines(data)
            print '%d%% of work done' % (i + 1)


def compare_itemset_lists(x, y):
    res = []
    for i in x:
        found = False
        for j in y:
            if i == j:
                found = True
                break
        if not found:
            res.append(i)
    return res


def convert_format(input_path, output_path):
    try:
        sc = pyspark.SparkContext()
        rdd = sc.newAPIHadoopFile(input_path,
                                  "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
                                  "org.apache.hadoop.io.Text",
                                  "org.apache.hadoop.io.LongWritable",
                                  conf={'textinputformat.record.delimiter': '---END.OF.DOCUMENT---'})
        rdd1 = rdd.map(lambda a: a[1].strip().split(" "))
        rdd2 = rdd1.map(lambda a: " ".join([str(hash(i)) for i in a]))
        rdd2.saveAsTextFile(output_path)
    finally:
        sc.stop()


def create_dataset_different_sizes(input_path, output_path, db_name):
    try:
        sc = pyspark.SparkContext()
        rdd = sc.textFile(input_path)
        rdd.cache()
        print 'loaded rdd from %s' % input_path

        xsmall_rdd = rdd.sample(False, 0.2)
        path = os.path.join(output_path, db_name + '-xsmall')
        xsmall_rdd.saveAsTextFile(path)
        print 'Created db at %s' % path

        path = os.path.join(output_path, db_name + '-small')
        small_rdd = rdd.sample(False, 0.4)
        small_rdd.saveAsTextFile(path)
        print 'Created db at %s' % path

        path = os.path.join(output_path, db_name + '-medium')
        medium_rdd = rdd.sample(False, 0.6)
        medium_rdd.saveAsTextFile(path)
        print 'Created db at %s' % path

        path = os.path.join(output_path, db_name + '-large')
        large_rdd = rdd.sample(False, 0.8)
        large_rdd.saveAsTextFile(path)
        print 'Created db at %s' % path

        path = os.path.join(output_path, db_name + '-xlarge')
        xlarge_rdd = rdd.sample(False, 1.0)
        xlarge_rdd.saveAsTextFile(path)
        print 'Created db at %s' % path
    finally:
        sc.stop()
        print 'done'
