import pyspark
import pickle
import time
import os

from cis import alg
from cis import algfpgrowth
from cis import utils


XSMALL_ALPHA = 0.00001
SMALL_ALPHA = 0.0001
MEDUIM_ALPHA = 0.001
LARGE_ALPHA = 0.01
XLARGE_ALPHA = 0.1

DATA_SET_NAME = 'dataset-single-wiki'
THRESHOLD_RATIO = 0.7
DATA_PATH = "data/single-wiki.txt"
NUM_MACHINES = 4

DATE = time.strftime("%x").replace("/", "_")
TIME = time.strftime("%X").replace(":", "_")
TEST_DIR = "results/%s/%s_%s__%s" % (DATA_SET_NAME, __file__.split("/")[-1].split(".")[0], DATE, TIME)
TEST_FILE_NAME = 'test_alpha.res'

RES = None


class TestAlpha:

    def __init__(self, times):
        self._times = times
        self._data_path = DATA_PATH
        self._num_machines = NUM_MACHINES
        self._sc = None
        # self._sc = utils.get_spark_context()
        # self._data_set_rdd = self._get_dataset_rdd()
        # self._data_set_rdd.cache()
        # self._data_set_size = self._data_set_rdd.count()
        # self._threshold = THRESHOLD_RATIO * self._data_set_size
        self._epsilon = 0.001
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
        dataset_rdd = dataset_rdd.repartition(self._num_machines)
        return dataset_rdd

    def _init_res_dict(self):
        return {'alg': {}, 'base': {}, 'spark': {}}

    def _collect_results(self, param, alpha):
        global RES
        RES[param]['value'] = alpha

        self.reset()
        RES[param]['base']['time'] = 0
        for i in range(self._times):
            print 'running base - %d iteration' % i
            RES[param]['base']['graph'], iter_running_time = run_base(self._sc, self._data_set_rdd,
                                                                      self._data_set_size,
                                                                      self._threshold, self._epsilon, alpha=alpha)
            RES[param]['base']['time'] += iter_running_time
        RES[param]['base']['time'] /= self._times

        self.reset()
        RES[param]['spark']['time'] = 0
        RES[param]['spark']['sets_calc_time'] = 0
        RES[param]['spark']['cis_collect_and_filter'] = 0
        for i in range(self._times):
            print 'running spark - %d iteration' % i
            results, iter_running_time = run_spark(self._data_set_rdd, THRESHOLD_RATIO, self._num_machines)
            RES[param]['spark']['time'] += iter_running_time
            RES[param]['spark']['graph'] = results[0]
            RES[param]['spark']['sets_calc_time'] += results[1]
            RES[param]['spark']['cis_collect_and_filter'] += results[2]
        RES[param]['spark']['time'] /= self._times
        RES[param]['spark']['sets_calc_time'] /= self._times
        RES[param]['spark']['cis_collect_and_filter'] /= self._times

        self.reset()
        base_graph = RES[param]['base']['graph']
        RES[param]['base']['num_cis'] = len(base_graph.frequentsDict().keys())

        RES[param]['alg']['time'] = 0
        RES[param]['alg']['not_identified'] = 0
        RES[param]['alg']['approx_overhead'] = 0
        RES[param]['alg']['approx_alpha_mean'] = 0
        for i in range(self._times):
            print 'runing alg - %d iteration' % i
            RES[param]['alg']['graph'], iter_running_time = run_alg(self._sc, self._data_set_rdd, self._data_set_size,
                                                                    self._threshold, self._epsilon, alpha=alpha)
            RES[param]['alg']['time'] += iter_running_time
            alg_graph = RES[param]['alg']['graph']
            results = alg_graph.calc_error(base_graph, alpha)
            RES[param]['alg']['not_identified'] += results[0]
            RES[param]['alg']['approx_overhead'] += results[1]
            RES[param]['alg']['approx_alpha_mean'] += results[2]
        RES[param]['alg']['time'] /= self._times
        RES[param]['alg']['not_identified'] /= self._times
        RES[param]['alg']['approx_overhead'] /= self._times
        RES[param]['alg']['approx_alpha_mean'] /= self._times

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

    def reset(self):
        if self._sc:
            self._sc.stop()
        self._sc = utils.get_spark_context()
        self._data_set_rdd = self._get_dataset_rdd()
        self._data_set_rdd.cache()
        self._data_set_size = self._data_set_rdd.count()
        print 'num transactions in data - %d' % self._data_set_size
        self._threshold = THRESHOLD_RATIO * self._data_set_size

    def finish(self):
        self._sc.stop()
        global RES
        experiments = RES.values()
        # for e in experiments:
        #     for value in e.itervalues():
        #         if value == {}:
        #             return
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


if __name__ == "__main__":
    import sys
    try:
        times = int(sys.argv[1])
    except:
        times = 1
    print 'starting alpha test and creating test class'
    test = TestAlpha(times)
    print 'running xsmall test'
    test.test_xsmall()
    print 'running small test'
    test.test_small()
    print 'running medium test'
    test.test_medium()
    print 'running large test'
    test.test_large()
    print 'runnin xlarge test'
    test.test_xlarge()
    print 'test complete. Finishing'
    test.finish()
    print 'done'
