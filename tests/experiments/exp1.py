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


threshold = 50000
epsilon = 0.001
log = logging.getLogger()

def get_dataset_rdd(sc, path):
    lines_rdd = sc.textFile(path, 4)
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
    conf.setMaster('local[4]')
    sc = pyspark.SparkContext(conf=conf)

    dataset_rdd = get_dataset_rdd(sc, INPUT_DATASET_PATH)
    log.info('Done loading data set from %s', INPUT_DATASET_PATH)

    log.info('Configuration for randomized test: Threshold=%(threshold)d, epsilon=%(epsilon)s',
             dict(threshold=threshold, epsilon=epsilon))

    log.info('Starting test')
    start = time.time()
    res = alg.alg(sc, dataset_rdd, threshold, epsilon)
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
    exp1()