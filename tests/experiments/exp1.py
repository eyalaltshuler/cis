import pyspark

INPUT_DATASET_PATH = "data/b.txt"
OUTPUT_PATH = "data/b-lattice.bin"
OUTPUT_PATH_RAND = "data/b-lattice-sample.bin"

from cis import alg
from cis import frequents
from time import time

def get_dataset_rdd(sc, path):
    lines_rdd = sc.textFile(path, minPartitions=4)
    dataset_rdd = lines_rdd.map(lambda x: set([int(i) for i in x.strip().split(" ")]))
    return dataset_rdd

def exp1():
    conf = pyspark.SparkConf()
    conf.setMaster("local[4]")
    sc = pyspark.SparkContext(conf=conf)
    transaction = get_dataset_rdd(sc, INPUT_DATASET_PATH)
    threshold = 10000
    epsilon = 0.1

    data_set_size = transaction.count()
    sample = transaction.takeSample(False, min(4000, data_set_size))

    print 'Starting Randomized test'
    start = time()
    res = alg.alg(sc, transaction, threshold, epsilon, sample=sample)
    end = time()
    print 'Randomized test ended and took %d seconds' % int(end - start)
    frequents.Frequents.save(res, OUTPUT_PATH_RAND)

    print 'Starting non randomized test'
    start = time()
    res = alg.alg(sc, transaction, threshold, epsilon, randomized=False, sample=sample)
    end = time()
    print 'Non randomized test ended and took %d seconds' % int(end - start)
    frequents.Frequents.save(res, OUTPUT_PATH)

    sc.stop()


if __name__=="__main__":
    exp1()