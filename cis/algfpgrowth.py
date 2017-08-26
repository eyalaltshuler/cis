import pyspark
from pyspark.mllib.fpm import FPGrowth
from frequents import Frequents
import time


def alg_fp_growth(data_set_rdd, threshold, num_of_partitions):
    start = time.time()
    model = FPGrowth.train(data_set_rdd, threshold, num_of_partitions)
    end = time.time()
    print 'training took %s seconds' % (end - start)
    start = time.time()
    result = model.freqItemsets().collect()
    end = time.time()
    print 'building cis tree took %s seconds' % (end - start)
    return _build_cis_tree(result)

def _build_cis_tree(result):
    result = [(set(n.items), n.freq) for n in result]
    cis_tree = Frequents()
    singletons = [(set(n[0]), n[1]) for n in result if len(n[0]) == 1]
    cis_tree.add_level(singletons)
    left_item_sets = [n for n in result if n not in singletons]
    next_level = []
    while left_item_sets:
        left = [n for n in left_item_sets]
        for scanned_itemset in left:
            found_containing = False
            for found_itemset in next_level:
                if found_itemset[0].issubset(set(scanned_itemset[0])):
                    if found_itemset[1] == scanned_itemset[1]:
                        next_level.remove(found_itemset)
                        next_level.append(scanned_itemset)
                        if scanned_itemset in left_item_sets:
                            left_item_sets.remove(scanned_itemset)
                    if found_itemset[1] > scanned_itemset[1]:
                        next_level.remove(found_itemset)
                        next_level.append(scanned_itemset)
                        left_item_sets.append(found_itemset)
                    found_containing = True
            if not found_containing:
                next_level.append(scanned_itemset)
                left_item_sets.remove(scanned_itemset)
        cis_tree.add_level(next_level)
    return cis_tree


if __name__ == "__main__":
    transactionSet = [set([1, 2, 3, 4]), set([1, 2, 3]), set([4, 3]), set([1]), set([2, 4])]
    sc = pyspark.SparkContext()
    transactionRdd = sc.parallelize(transactionSet)
    threshold = 2 / float(5)
    res = alg_fp_growth(transactionRdd, threshold, 1)
    sc.stop()
