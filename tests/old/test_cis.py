import unittest
import mock
import cis


class test_cis(unittest.TestCase):
    def test_parallelClosure(self):
        dataset = [set([1,2,3,4]), 
                   set([1,2,4,5,6]), 
                   set([1,2,5,3,6,4,10,123]), 
                   set([1,2,4,5,11,12,13,14,15,16])]
        closure = cis.closure(dataset)
        self.assertEquals(closure, set([1,2,4]))
