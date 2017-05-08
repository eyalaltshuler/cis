import pyspark

INPUT_DATASET_PATH = "data/b.txt"
OUTPUT_PATH = "data/b-lattice.bin"
OUTPUT_WITH_SAMPLE = "data/b-lattice-sample.bin"

from cis import alg
from cis import frequents
from time import time

def get_dataset_rdd(sc, path):
    lines_rdd = sc.textFile(path, minPartitions=4)
    dataset_rdd = lines_rdd.map(lambda x: set([int(i) for i in x.strip().split(" ")]))
    return dataset_rdd

def exp1():
    print 'Starting Test'
    start = time()
    conf = pyspark.SparkConf()
    conf.setMaster("local[4]")
    sc = pyspark.SparkContext(conf=conf)
    transaction = get_dataset_rdd(sc, INPUT_DATASET_PATH)
    threshold = 10000
    epsilon = 0.1
    res = alg.alg(sc, transaction, threshold, epsilon)
    end = time()
    print 'Test ended and took %d seconds' % int(end - start)
    frequents.Frequents.save(res, OUTPUT_PATH)
    sc.stop()


if __name__=="__main__":
    exp1()