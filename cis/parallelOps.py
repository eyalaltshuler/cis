import pyspark
import logging
import os


def cis(dataset, itemset):
    filterFunc = lambda t: itemset.issubset(t)
    filteredDataset = dataset.filter(filterFunc)
    return filteredDataset.reduce(lambda a,b: a.intersection(b))


def sample(dataset, datasetSize, fraction):
    return dataset.sample(False, fraction).collect()

def countElements(dataset):
    counts = dataset.flatMap(lambda t: [(e,1) for e in t]).reduceByKey(lambda a,b: a+b).collect()
    return {k:v for (k,v) in counts}


if __name__=="__main__":
    sc = pyspark.SparkContext()
    dataDir = "/home/eyal/work/cis/data"
    path = os.path.join(dataDir, "gen1.txt")
    logger = logging.getLogger(__name__)
    logger.addHandler(logging.FileHandler("a.txt"))
    logger.setLevel(logging.INFO)
    logger.info("loading data from %s", path)
    origData = sc.textFile(path)
    dataset = origData.map(lambda x: set([int(i) for i in x.strip().split(' ')]))
    logger.info("dataset partitions number is %s", dataset.getNumPartitions())
    dataset.repartition(2)
    logger.info("now data is partitioned into 2 partitions")
    logger.info("caching dataset")
    dataset.cache()
    itemset = set([3])
    logger.info("computing cis function")
    res1 = cis(dataset, itemset)
    logger.info("got result %s", res1)
    
    tasks = {0 : set([1]), 1: set([3])}
    logger.info("computing cis batch function")
    dataset = dataset.repartition(2)
    res2 = cisBatch(dataset, tasks)
    logger.info("got result %s", res2)

    dataset = sc.parallelize([1,2,3,4,5,6,7,8,9,10])
    sampledData = sample(dataset, 10, 0.4)
    logger.info("got sample of fraction 0.4 - %s", sampledData)
   
    dataset = sc.parallelize([[1,2], [1,3], [1,2,3]])
    logger.info("calculating counts")
    counts = countElements(dataset)
    logger.info("Got counts %s", counts)

    logger.info("done.")
