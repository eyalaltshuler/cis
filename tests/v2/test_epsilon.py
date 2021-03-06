import unittest

import pyspark
import pickle
import time
import os

from cis import alg
from cis import algfpgrowth
from cis import utils


XSMALL_EPSILON = 0.00001
SMALL_EPSILON = 0.0001
MEDUIM_EPSILON = 0.001
LARGE_EPSILON = 0.01
XLARGE_EPSILON = 0.1

DATA_SET_NAME = 'dataset-single-wiki'
THRESHOLD_RATIO = 0.7
DATA_PATH = "data/single-wiki.txt"
NUM_MACHINES = 4

DATE = time.strftime("%x").replace("/", "_")
TIME = time.strftime("%X").replace(":", "_")
TEST_DIR = "results/%s/%s_%s__%s" % (DATA_SET_NAME, __file__.split("/")[-1].split(".")[0], DATE, TIME)
TEST_FILE_NAME = 'test_epsilon.res'

RES = None

class TestEpsilon:

    def __init__(self, times):
        self._times = times
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
                   'xlarge': self._init_res_dict(),
                   'base': {},
                   'spark': {}}

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
        return {'alg': {}, }

    def run_base(self, _times=1):
        global RES
        self.reset()
        RES['base']['time'] = 0
        for i in range(_times):
            print 'running base - %d iteration' % i
            RES['base']['graph'], iter_running_time = run_base(self._sc, self._data_set_rdd, self._data_set_size,
                                                               self._threshold)
            RES['base']['time'] += iter_running_time
        RES['base']['time'] /= self._times

    def run_spark(self, _times=1):
        global RES
        self.reset()
        RES['spark']['time'] = 0
        RES['spark']['sets_calc_time'] = 0
        RES['spark']['cis_collect_and_filter'] = 0
        for i in range(_times):
            print 'running spark - %d iteration' % i
            results, iter_running_time = run_spark(self._data_set_rdd, THRESHOLD_RATIO, self._num_machines)
            RES['spark']['time'] += iter_running_time
            RES['spark']['graph'] = results[0]
            RES['spark']['sets_calc_time'] += results[1]
            RES['spark']['cis_collect_and_filter'] += results[2]
        RES['spark']['time'] /= self._times
        RES['spark']['sets_calc_time'] /= self._times
        RES['spark']['cis_collect_and_filter'] /= self._times

    def _collect_results(self, param, epsilon):
        RES[param]['value'] = epsilon
        base_graph = RES['base']['graph']
        RES['base']['num_cis'] = len(base_graph.frequentsDict().keys())
        spark_graph = RES['spark']['graph']
        self.reset()
        RES[param]['alg']['time'] = 0
        RES[param]['alg']['not_identified'] = 0
        RES[param]['alg']['approx_overhead'] = 0
        RES[param]['alg']['approx_alpha_mean'] = 0
        for i in range(self._times):
            print 'runing alg - %d iteration' % i
            RES[param]['alg']['graph'], iter_running_time = run_alg(self._sc, self._data_set_rdd, self._data_set_size,
                                                                    self._threshold, epsilon)
            RES[param]['alg']['time'] += iter_running_time
            alg_graph = RES[param]['alg']['graph']
            results = alg_graph.calc_error(spark_graph)
            RES[param]['alg']['not_identified'] += results[0]
            RES[param]['alg']['approx_overhead'] += results[1]
            RES[param]['alg']['approx_alpha_mean'] += results[2]
        RES[param]['alg']['time'] /= self._times
        RES[param]['alg']['not_identified'] /= self._times
        RES[param]['alg']['approx_overhead'] /= self._times
        RES[param]['alg']['approx_alpha_mean'] /= self._times

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
def run_base(sc, data_set_rdd, data_set_size, threshold, epsilon=0.1):
    return alg.alg(sc, data_set_rdd, data_set_size, threshold, epsilon, randomized=False)


@measure_time
def run_spark(data_set_rdd, threshold, num_of_partitions):
    return algfpgrowth.alg_fp_growth(data_set_rdd, threshold, num_of_partitions)


@measure_time
def run_alg(sc, data_set_rdd, data_set_size, threshold, epsilon):
    return alg.alg(sc, data_set_rdd, data_set_size, threshold, epsilon, randomized=True)


if __name__=="__main__":
    import sys
    try:
        times = int(sys.argv[1])
    except:
        times = 1
    print 'starting epsilon test and creating test class'
    test = TestEpsilon(times)
    print 'running base algorithm'
    test.run_base()
    print 'running spark algorithm'
    test.run_spark()
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
