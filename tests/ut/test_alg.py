import unittest
import mock
import random
import pyspark

from cis import alg
from cis import apriori


class test_alg(unittest.TestCase):
    def setUp(self):
        self.transactionSet = [set([1, 2, 3, 4]), set([1, 2, 3]), set([4, 3]), set([1, 2]), set([1, 2, 4])]
        self.sc = pyspark.SparkContext()
        self.rdd = self.sc.parallelize(self.transactionSet, 2)

    def test_alg(self):
        threshold = 2
        epsilon = 0.1
        res = alg.alg(self.rdd, threshold, epsilon)

    def tearDown(self):
        self.sc.stop()