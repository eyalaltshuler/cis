import pyspark
import logging
import os
import time

INPUT_DATASET_PATH = "data/b.txt"
OUTPUT_PATH = "data/b-lattice.bin"
OUTPUT_PATH_RAND = "data/b-lattice-sample.bin"

DATE = time.strftime("%x").replace("/", "_")
TIME = time.strftime("%X").replace(":", "_")
LOGS_DIR = "logs/%s_%s__%s" % (__file__.split("/")[-1].split(".")[0], DATE, TIME)
TEST_LOG_FILE_NAME = 'test.log'

from cis import alg
from cis import frequents

NUM_WORKERS = 4
threshold = 15000
epsilon = 0.1
log = logging.getLogger()

def get_dataset_rdd(sc, path):
    lines_rdd = sc.textFile(path, minPartitions=NUM_WORKERS)
    dataset_rdd = lines_rdd.map(lambda x: set([int(i) for i in x.strip().split(" ")]))
    return dataset_rdd

def _configure_log():
    if not os._exists(LOGS_DIR):
        os.mkdir(LOGS_DIR)
    fh = logging.FileHandler('%s/%s' %(LOGS_DIR, TEST_LOG_FILE_NAME))
    fh.setFormatter(logging.Formatter('%(asctime)-12s %(message)s'))
    log.addHandler(fh)
    log.setLevel(logging.INFO)

def exp1():
    _configure_log()
    conf = pyspark.SparkConf()
    conf.setMaster("local[%d]" % NUM_WORKERS)
    log.info('Created a spark configuration with %d workers', NUM_WORKERS)
    sc = pyspark.SparkContext(conf=conf)
    log.info('Loading a data set from path %s', INPUT_DATASET_PATH)
    transaction = get_dataset_rdd(sc, INPUT_DATASET_PATH)
    log.info('Done loading data set successfully')

    log.info('Configuration for randomized test:')
    log.info('Threshold is %d, epsilon is %f', threshold, epsilon)

    log.info('Computing data set number of transactions')
    data_set_size = transaction.count()
    log.info('Data set has %d transactions', data_set_size)
    sample_size = min(4000, data_set_size)
    log.info('Taking in master a sample of %d transactions', sample_size)
    sample = transaction.takeSample(False, sample_size)
    log.info('Sample taken successfully')

    log.info('Starting Randomized test')
    start = time.time()
    res = alg.alg(sc, transaction, threshold, epsilon, sample=sample)
    end = time.time()
    log.info('Randomized test ended and took %d seconds', int(end - start))
    log.info('Saving lattice into path %s', OUTPUT_PATH_RAND)
    frequents.Frequents.save(res, OUTPUT_PATH_RAND)
    log.info('Lattice saved successfully')

    log.info('Starting non randomized test')
    start = time.time()
    res = alg.alg(sc, transaction, threshold, epsilon, randomized=False, sample=sample)
    end = time.time()
    log.info('Non randomized test ended and took %d seconds', int(end - start))
    log.info('Saving lattice into path %s', OUTPUT_PATH_RAND)
    frequents.Frequents.save(res, OUTPUT_PATH)
    log.info('Lattice saved successfully')

    log.info('Freeing Spark context object')
    sc.stop()
    log.info('Experiement done')

if __name__=="__main__":
    exp1()