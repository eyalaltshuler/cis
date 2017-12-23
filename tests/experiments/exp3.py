import pyspark
import logging
import os
import time
import json

from cis import alg
from cis import frequents

INPUT_DATASET_PATH = "./data/b.txt"
LATTICE_NAME = "b-lattice.bin"

DATE = time.strftime("%x").replace("/", "_")
TIME = time.strftime("%X").replace(":", "_")
LOGS_DIR = "logs/%s_%s__%s" % (__file__.split("/")[-1].split(".")[0], DATE, TIME)
TEST_LOG_FILE_NAME = 'test.log'


threshold = 400000
epsilon = 0.001
log = logging.getLogger()

def get_dataset_rdd(sc, path):
    return sc.parallelize([set([1, 2, 3, 4]), set([1, 2, 3]), set([4, 3]), set([1]), set([2, 4])] * 1000000)


def _configure_log():
    if not os._exists(LOGS_DIR):
        os.mkdir(LOGS_DIR)
    fh = logging.FileHandler('%s/%s' %(LOGS_DIR, TEST_LOG_FILE_NAME))
    fh.setFormatter(logging.Formatter('%(asctime)-12s %(message)s'))
    log.addHandler(fh)
    log.setLevel(logging.INFO)

def exp3():
    _configure_log()
    conf = pyspark.SparkConf()
    conf.setMaster('local[4]')
    sc = pyspark.SparkContext(conf=conf)

    dataset_rdd = get_dataset_rdd(sc, INPUT_DATASET_PATH)
    log.info('Done loading data set from %s', INPUT_DATASET_PATH)


    dataset_rdd.cache()
    data_set_size = dataset_rdd.count()
    log.info('Configuration for randomized test: Threshold=%(threshold)d, epsilon=%(epsilon)s, dataset_size=%(data_set_size)s',
             dict(threshold=threshold, epsilon=epsilon, data_set_size=data_set_size))
    log.info('Starting test')
    start = time.time()
    res = alg.alg(sc, dataset_rdd, data_set_size, threshold, epsilon)
    end = time.time()
    log.info('Test ended and took %d seconds', int(end - start))

    output_path = os.path.join(LOGS_DIR, LATTICE_NAME)
    log.info('Saving lattice into path %s', output_path)
    frequents.Frequents.save(res, output_path)
    log.info('Lattice saved successfully')

    log.info('Freeing Spark context object')
    sc.stop()
    log.info('Experiement done')

if __name__=="__main__":
    exp3()