import pyspark
from pyspark.mllib.fpm import FPGrowth

def alg_fp_growth(data_set_rdd, threshold, num_of_partitions):
    model = FPGrowth.train(data_set_rdd, threshold, num_of_partitions)
    result = model.freqItemsets().collect()
    return result


if __name__ == "__main__":
    transactionSet = [set([1, 2, 3, 4]), set([1, 2, 3]), set([4, 3]), set([1]), set([2, 4])]
    sc = pyspark.SparkContext()
    transactionRdd = sc.parallelize(transactionSet)
    threshold = 2 / 4
    res = alg_fp_growth(transactionRdd, threshold, 1)
    sc.stop()
