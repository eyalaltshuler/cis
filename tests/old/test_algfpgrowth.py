import unittest
import pyspark

from cis import algfpgrowth


class test_algfpgrowth(unittest.TestCase):
    def setUp(self):
        self.transactionSet = [set([1, 2, 3, 4]), set([1, 2, 3]), set([4, 3]), set([1, 2]), set([1, 2, 4])]
        self.sc = pyspark.SparkContext()
        self.rdd = self.sc.parallelize(self.transactionSet, 2)

    def test_alg(self):
        threshold = 2 / 5
        res = algfpgrowth.alg_fp_growth(self.rdd, threshold, 2)

    def tearDown(self):
        self.sc.stop()