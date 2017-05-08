import pyspark

INPUT_DATASET_PATH = "data/a.txt"
from cis import algnaive
from time import time

def get_dataset_rdd(sc, path):
    lines_rdd = sc.textFile(path)
    dataset_rdd = lines_rdd.map(lambda x: set([int(i) for i in x.strip().split(" ")]))
    return dataset_rdd

def exp1():
    print 'Starting Test'
    start = time()
    sc = pyspark.SparkContext()
    transaction = get_dataset_rdd(sc, INPUT_DATASET_PATH)
    threshold = 8
    res = algnaive.alg_naive(transaction, threshold)
    end =  time()
    print 'Test ended and took %d seconds' % int(end - start)
    import ipdb
    ipdb.set_trace()
    sc.stop()

if __name__=="__main__":
    exp1()