import unittest

import pickle
import time
import os

from cis import alg
from cis import algfpgrowth


XSMALL_EPSILON = 0.001
SMALL_EPSILON = 0.01
MEDUIM_EPSILON = 0.05
LARGE_EPSILON = 0.1
XLARGE_EPSILON = 0.3

TITLE = ''
XLABEL = ''
YLABEL = ''

DATA_SET_NAME = 'abc'

DATE = time.strftime("%x").replace("/", "_")
TIME = time.strftime("%X").replace(":", "_")
TEST_DIR = "results/%s_%s__%s" % (__file__.split("/")[-1].split(".")[0], DATE, TIME)
TEST_FILE_NAME = '%s_test_epsilon.res' % DATA_SET_NAME


class TestEpsilon(unittest.TestCase):
    def setUp(self):
        self._res = {'xsmall': self._init_res_dict(),
                     'small': self._init_res_dict(),
                     'medium': self._init_res_dict(),
                     'large': self._init_res_dict(),
                     'xlarge': self._init_res_dict()}

    def _init_res_dict(self):
        return {'alg': {}, 'base': {}, 'spark': {}}

    def _collect_results(self, param):
        self._res[param]['base']['graph'], self._res[param]['base']['time'] = run_base()
        self._res[param]['spark']['graph'], self._res[param]['spark']['time'] = run_spark()
        self._res[param]['alg']['graph'], self._res[param]['alg']['time'] = run_alg()

        import ipdb
        ipdb.set_trace()

        alg_graph = self._res[param]['alg']['grpah']
        base_graph = self._res[param]['base']['braph']

        self._res[param]['alg']['error'], self._res[param]['alg']['wrong_cis'] = alg_graph.calc_error(base_graph)

    def test_xsmall(self):
        self._collect_results('xsmall')

    def _test_small(self):
        self._collect_results('small')

    def _test_medium(self):
        self._collect_results('medium')

    def _test_large(self):
        self._collect_results('large')

    def _test_xlarge(self):
        self._collect_results('xlarge')

    def tearDown(self):
        save_results(self._res)


def save_results(results):
    output_file_path = os.path.join(TEST_DIR, TEST_FILE_NAME)
    with open(output_file_path, 'w') as f:
        pickle.dump(results, f)


def measure_time(test):
    def func(*args, **kwargs):
        start = time.time()
        res = test(args, kwargs)
        end = time.time()
        return res, end - start
    return func


@measure_time
def run_base(dataset, epsilon):
    time.sleep(3)


@measure_time
def run_spark(dataset, epsilon):
    time.sleep(2)


@measure_time
def run_alg(dataset, epsilon):
    time.sleep(1)