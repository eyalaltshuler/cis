import unittest

import pyspark
import pickle
import time
import os

from cis import alg
from cis import algfpgrowth
from cis import utils


XSMALL_ALPHA = 0.01
SMALL_ALPHA = 0.05
MEDUIM_ALPHA = 0.1
LARGE_ALPHA = 0.2
XLARGE_ALPHA = 0.3

DATA_SET_NAME = 'wiki'
THRESHOLD_RATIO = 0.7
DATA_PATH = "s3a://cis-exp1/wiki-xlarge/"
NUM_MACHINES = 40

DATE = time.strftime("%x").replace("/", "_")
TIME = time.strftime("%X").replace(":", "_")
TEST_DIR = "results/%s/%s_%s__%s" % (DATA_SET_NAME, __file__.split("/")[-1].split(".")[0], DATE, TIME)
TEST_FILE_NAME = 'test_alpha.res'

RES = None

class TestAlpha(unittest.TestCase):

    def setUp(self):
        self._data_path = DATA_PATH
        self._num_machines = NUM_MACHINES
        self._sc = utils.get_spark_context()
        self._data_set_rdd = self._get_dataset_rdd()
        self._data_set_rdd.cache()
        self._data_set_size = self._data_set_rdd.count()
        self._threshold = THRESHOLD_RATIO * self._data_set_size
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
        dataset_rdd = lines_rdd.map(lambda x: set([hash(i) for i in x.split(" ")]))
        return dataset_rdd

    def _init_res_dict(self):
        return {'alg': {}, 'base': {}, 'spark': {}}

    def _collect_results(self, param, alpha):
        global RES
        RES[param]['value'] = alpha
        RES[param]['base']['graph'], RES[param]['base']['time'] = run_base(self._sc,
                                                                           self._data_set_rdd,
                                                                           self._data_set_size,
                                                                           self._threshold,
                                                                           self._epsilon,
                                                                           alpha=alpha)
        RES[param]['spark']['graph'], RES[param]['spark']['time'] = run_spark(self._data_set_rdd,
                                                                              THRESHOLD_RATIO,
                                                                              self._num_machines)
        RES[param]['alg']['graph'], RES[param]['alg']['time'] = run_alg(self._sc,
                                                                        self._data_set_rdd,
                                                                        self._data_set_size,
                                                                        self._threshold,
                                                                        self._epsilon,
                                                                        alpha=alpha)
        alg_graph = RES[param]['alg']['graph']
        base_graph = RES[param]['base']['graph']

        RES[param]['alg']['error'], RES[param]['alg']['wrong_cis'] = alg_graph.calc_error(base_graph)

    def test_xsmall(self):
        self._collect_results('xsmall', XSMALL_ALPHA)

    def test_small(self):
        self._collect_results('small', SMALL_ALPHA)

    def test_medium(self):
        self._collect_results('medium', MEDUIM_ALPHA)

    def test_large(self):
        self._collect_results('large', LARGE_ALPHA)

    def test_xlarge(self):
        self._collect_results('xlarge', XLARGE_ALPHA)

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
def run_base(sc, data_set_rdd, data_set_size, threshold, epsilon, alpha=0.1):
    return alg.alg(sc, data_set_rdd, data_set_size, threshold, epsilon, randomized=False, alpha=alpha)


@measure_time
def run_spark(data_set_rdd, threshold, num_of_partitions):
    return algfpgrowth.alg_fp_growth(data_set_rdd, threshold, num_of_partitions)


@measure_time
def run_alg(sc, data_set_rdd, data_set_size, threshold, epsilon, alpha=0.1):
    return alg.alg(sc, data_set_rdd, data_set_size, threshold, epsilon, randomized=True, alpha=alpha)