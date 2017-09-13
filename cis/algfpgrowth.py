import pyspark
from pyspark.mllib.fpm import FPGrowth
from frequents import Frequents
import time


def alg_fp_growth(data_set_rdd, threshold, num_of_partitions):
    start = time.time()
    model = FPGrowth.train(data_set_rdd, threshold, num_of_partitions)
    end = time.time()
    print 'Training took %s seconds' % (end - start)
    start = time.time()
    result = model.freqItemsets().collect()
    end = time.time()
    print 'Frequent itemsets collection took %s seconds' % (end - start)
    # start = time.time()
    # res = _build_cis_tree(result)
    # end = time.time()
    # print 'building cis tree took %s seconds' % (end - start)
    return result

def _build_cis_tree(result):
    result = [(set(n.items), n.freq) for n in result]
    cis_tree = Frequents()
    next_level = [(set(n[0]), n[1]) for n in result if len(n[0]) == 1]
    left_item_sets = [n for n in result if n not in next_level]
    while left_item_sets:
        print 'There are still %d itemsets to handle' % len(left_item_sets)
        left = [i for i in left_item_sets]
        for scanned_itemset in left:
            if next_level == []:
                next_level.append(scanned_itemset)
                if scanned_itemset in left_item_sets:
                    left_item_sets.remove(scanned_itemset)
                continue
            next_level_copy = [i for i in next_level]
            for found_itemset in next_level_copy:
                if found_itemset[0].issubset(set(scanned_itemset[0])):
                    if found_itemset[1] == scanned_itemset[1]:
                        next_level.remove(found_itemset)
                        next_level.append(scanned_itemset)
                        if scanned_itemset in left_item_sets:
                            left_item_sets.remove(scanned_itemset)
                        continue
                if set(scanned_itemset[0]).issubset(found_itemset[0]):
                    if found_itemset[1] == scanned_itemset[1]:
                        if scanned_itemset in left_item_sets:
                            left_item_sets.remove(scanned_itemset)
                        continue
                    if found_itemset[1] > scanned_itemset[1]:
                        next_level.remove(found_itemset)
                        left_item_sets.append(found_itemset)
                        next_level.append(scanned_itemset)
                        if scanned_itemset in left_item_sets:
                            left_item_sets.remove(scanned_itemset)
                        continue
                next_level.append(scanned_itemset)
                if scanned_itemset in left_item_sets:
                    left_item_sets.remove(scanned_itemset)
        cis_tree.add_level(next_level)
        next_level = []
    return cis_tree


if __name__ == "__main__":
    transactionSet = [set([1, 2, 3, 4]), set([1, 2, 3]), set([4, 3]), set([1]), set([2, 4])]
    sc = pyspark.SparkContext()
    transactionRdd = sc.parallelize(transactionSet)
    threshold = 2 / float(5)
    res = alg_fp_growth(transactionRdd, threshold, 1)
    sc.stop()
