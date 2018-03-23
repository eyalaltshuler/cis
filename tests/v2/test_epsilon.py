import unittest

import pyspark
import pickle
import time
import os

from cis import alg
from cis import algfpgrowth
from cis import utils


XSMALL_EPSILON = 0.001
SMALL_EPSILON = 0.01
MEDUIM_EPSILON = 0.05
LARGE_EPSILON = 0.1
XLARGE_EPSILON = 0.3

DATA_SET_NAME = 'dataset-single-news'
THRESHOLD_RATIO = 0.9
DATA_PATH = "data/single-news/"
NUM_MACHINES = 4

DATE = time.strftime("%x").replace("/", "_")
TIME = time.strftime("%X").replace(":", "_")
TEST_DIR = "results/%s/%s_%s__%s" % (DATA_SET_NAME, __file__.split("/")[-1].split(".")[0], DATE, TIME)
TEST_FILE_NAME = 'test_epsilon.res'

RES = None

class TestEpsilon:

    def __init__(self):
        self._data_path = DATA_PATH
        self._num_machines = NUM_MACHINES
        self._sc = utils.get_spark_context()
        self._data_set_rdd = self._get_dataset_rdd()
        self._data_set_rdd.cache()
        self._data_set_size = self._data_set_rdd.count()
        print 'num transactions - %d' % self._data_set_size
        self._threshold = THRESHOLD_RATIO * self._data_set_size
        if not os.path.exists("results/%s" % DATA_SET_NAME):
            os.mkdir("results/%s" % DATA_SET_NAME)
        if not os.path.exists(TEST_DIR):
            os.mkdir(TEST_DIR)
        global RES
        if RES is None:
            RES = {'xsmall': self._init_res_dict(),
                   'small': self._init_res_dict(),
                   'medium': self._init_res_dict(),
                   'large': self._init_res_dict(),
                   'xlarge': self._init_res_dict()}

    def reset(self):
        if self._sc:
            self._sc.stop()
        self._sc = utils.get_spark_context()
        self._data_set_rdd = self._get_dataset_rdd()
        self._data_set_rdd.cache()
        self._data_set_size = self._data_set_rdd.count()
        self._threshold = THRESHOLD_RATIO * self._data_set_size

    def _get_dataset_rdd(self):
        lines_rdd = self._sc.textFile(self._data_path, self._num_machines)
        dataset_rdd = lines_rdd.map(lambda x: set([hash(i) for i in x.split(" ")]))
        return dataset_rdd

    def _init_res_dict(self):
        return {'alg': {}, 'base': {}, 'spark': {}}

    def _collect_results(self, param, epsilon):
        global RES
        RES[param]['value'] = epsilon
        self.reset()
        RES[param]['base']['graph'], RES[param]['base']['time'] = run_base(self._sc, self._data_set_rdd, self._data_set_size,
                                                                           self._threshold, epsilon)

        self.reset()
        RES[param]['spark']['graph'], RES[param]['spark']['time'] = run_spark(self._data_set_rdd,
                                                                              THRESHOLD_RATIO, self._num_machines)

        base_graph = RES[param]['base']['graph']
        RES[param]['base']['num_cis'] = len(base_graph.frequentsDict().keys())
        self.reset()
        RES[param]['alg']['graph'], RES[param]['alg']['time'] = run_alg(self._sc, self._data_set_rdd, self._data_set_size,
                                                                        self._threshold, epsilon)
        alg_graph = RES[param]['alg']['graph']
        RES[param]['alg']['error'], RES[param]['alg']['wrong_cis'], RES[param]['alg']['detected_cis'] = \
            alg_graph.calc_error(base_graph)

    def test_xsmall(self):
        self._collect_results('xsmall', XSMALL_EPSILON)

    def test_small(self):
        self._collect_results('small', SMALL_EPSILON)

    def test_medium(self):
        self._collect_results('medium', MEDUIM_EPSILON)

    def test_large(self):
        self._collect_results('large', LARGE_EPSILON)

    def test_xlarge(self):
        self._collect_results('xlarge', XLARGE_EPSILON)

    def finish(self):
        self._sc.stop()
        global RES
        experiments = RES.values()
        for e in experiments:
            for value in e.itervalues():
                if value == {}:
                    return
        save_results(RES)
        RES = None


def save_results(results):
    output_file_path = os.path.join(TEST_DIR, TEST_FILE_NAME)
    with open(output_file_path, 'w') as f:
        pickle.dump(results, f)


def measure_time(test):
    def func(*args, **kwargs):
        start = time.time()
        res = test(*args, **kwargs)
        end = time.time()
        return res, end - start
    return func


@measure_time
def run_base(sc, data_set_rdd, data_set_size, threshold, epsilon):
    return alg.alg(sc, data_set_rdd, data_set_size, threshold, epsilon, randomized=False)


@measure_time
def run_spark(data_set_rdd, threshold, num_of_partitions):
    return algfpgrowth.alg_fp_growth(data_set_rdd, threshold, num_of_partitions)


@measure_time
def run_alg(sc, data_set_rdd, data_set_size, threshold, epsilon):
    return alg.alg(sc, data_set_rdd, data_set_size, threshold, epsilon, randomized=True)


if __name__=="__main__":
    print 'starting epsilon test and creating test class'
    test = TestEpsilon()
    print 'running xsmall test'
    test.test_xsmall()
    print 'running small test'
    test.test_small()
    print 'running medium test'
    test.test_medium()
    print 'running large test'
    test.test_large()
    print 'running xlarge test'
    test.test_xlarge()
    print 'test complete. Finishing'
    test.finish()
    print 'done'