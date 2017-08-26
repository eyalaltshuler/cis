import pyspark
from cis import frequents

INPUT_DATASET_PATH = "./data/b.txt"
OUTPUT_PATH_RAND = "./b-lattice-fpg.bin"

from cis import algfpgrowth
from time import time

def get_dataset_rdd(sc, path):
    lines_rdd = sc.textFile(path, 4)
    dataset_rdd = lines_rdd.map(lambda x: set([int(i) for i in x.strip().split(" ")]))
    return dataset_rdd

def exp2():
    conf = pyspark.SparkConf()
    conf.setMaster('local[4]')
    sc = pyspark.SparkContext(conf=conf)
    data = get_dataset_rdd(sc, INPUT_DATASET_PATH)
    data.cache()
    data_set_size = data.count()
    threshold = 50000 / float(data_set_size)

    print 'Starting alg-fpgrowth test'
    start = time()
    res = algfpgrowth.alg_fp_growth(data, threshold, 4)
    end = time()
    print 'alg-fp-growth test ended and took %d seconds' % int(end - start)
    frequents.Frequents.save(res, OUTPUT_PATH_RAND)
    sc.stop()

if __name__=="__main__":
    exp2()
