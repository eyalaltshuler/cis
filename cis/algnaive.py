import logging
import pyspark

import frequents

from cis import utils


def alg_naive(data_set_rdd, threshold):
    frequencies = utils.countElements(transactionRdd)
    data_set_size = data_set_rdd.count()
    common_elements = filter(frequencies, lambda a: a(1) >= threshold)
    candidates = common_elements
    cis_tree = frequents.Frequents(common_elements)

    while candidates:
        for candidate in candidates:
            candidate_cis = utils.cis(data_set_rdd, candidate)
            cis_tree.add_itemset(candidate_cis)
        candidates = cis_tree.expand()
    return res


if __name__=="__main__":
    transactionSet = [set(1,2,3,4), set(1,2,3), set(4,3), set(1), set(2,4)]
    sc = pyspark.SparkContext()
    transactionRdd = sc.parallelize(transactionSet)
    threshold = 10
    res = alg_naive(sc, transactionRdd, threshold)