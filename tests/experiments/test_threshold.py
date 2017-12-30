import unittest

import pyspark
import pickle
import time
import os

from cis import alg
from cis import algfpgrowth


XSMALL_THRESHOLD = 0.5
SMALL_THRESHOLD = 0.6
MEDUIM_THRESHOLD = 0.7
LARGE_THRESHOLD = 0.8
XLARGE_THRESHOLD = 0.9

DATA_SET_NAME = 'generated-b'

DATE = time.strftime("%x").replace("/", "_")
TIME = time.strftime("%X").replace(":", "_")
TEST_DIR = "results/%s/%s_%s__%s" % (DATA_SET_NAME, __file__.split("/")[-1].split(".")[0], DATE, TIME)
TEST_FILE_NAME = 'test_threshold.res'

RES = None

class TestThreshold(unittest.TestCase):

    def setUp(self):
        self._data_path = "data/b.txt"
        self._num_machines = 4
        self._sc = pyspark.SparkContext()
        self._data_set_rdd = self._get_dataset_rdd()
        self._data_set_rdd.cache()
        self._data_set_size = self._data_set_rdd.count()
        self._epsilon = 0.1
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

    def _get_dataset_rdd(self):
        lines_rdd = self._sc.textFile(self._data_path, self._num_machines)
        dataset_rdd = lines_rdd.map(lambda x: set([int(i) for i in x.strip().split(" ")]))
        return dataset_rdd

    def _init_res_dict(self):
        return {'alg': {}, 'base': {}, 'spark': {}}

    def _collect_results(self, param, threshold):
        global RES
        RES[param]['value'] = threshold
        RES[param]['base']['graph'], RES[param]['base']['time'] = run_base(self._sc,
                                                                           self._data_set_rdd,
                                                                           self._data_set_size,
                                                                           threshold * self._data_set_size,
                                                                           self._epsilon)
        RES[param]['spark']['graph'], RES[param]['spark']['time'] = run_spark(self._data_set_rdd,
                                                                              threshold,
                                                                              self._num_machines)
        RES[param]['alg']['graph'], RES[param]['alg']['time'] = run_alg(self._sc,
                                                                        self._data_set_rdd,
                                                                        self._data_set_size,
                                                                        threshold * self._data_set_size,
                                                                        self._epsilon)
        alg_graph = RES[param]['alg']['graph']
        base_graph = RES[param]['base']['graph']

        RES[param]['alg']['error'], RES[param]['alg']['wrong_cis'] = alg_graph.calc_error(base_graph)

    def test_xsmall(self):
        self._collect_results('xsmall', XSMALL_THRESHOLD)

    def test_small(self):
        self._collect_results('small', SMALL_THRESHOLD)

    def test_medium(self):
        self._collect_results('medium', MEDUIM_THRESHOLD)

    def test_large(self):
        self._collect_results('large', LARGE_THRESHOLD)

    def test_xlarge(self):
        self._collect_results('xlarge', XLARGE_THRESHOLD)

    def tearDown(self):
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