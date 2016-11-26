import pyspark
import logging
import ConfigParser

sc = pyspark.SparkContext()
path = "data/gen1.txt"


def cis(dataset, itemset):
    filterFunc = lambda t: itemset.issubset(t)
    filteredDataset = dataset.filter(filterFunc)
    return filteredDataset.reduce(lambda a,b: a.intersection(b))

def cisBatch(dataset, tasksDict):
    ''' 
    Assumes that dataset is an RDD that can be quried for its partitions number
    tasksDict maps between workers to their tasks
    '''
    def partitionFunc(i, partitionIter):
        data = []
        for element in partitionIter:
            data.append(element)
        projected = filter(lambda t: tasksDict[i].issubset(t), data)
        yield reduce(lambda a,b: a.intersection(b), projected)

    return dataset.mapPartitionsWithIndex(partitionFunc, preservesPartitioning=True).collect()


if __name__=="__main__":
    origData = sc.textFile(path)
    dataset = origData.map(lambda x: set([int(i) for i in x.strip().split(' ')]))
    dataset.repartition(2)
    dataset.cache(
    )
    itemset = set([3])
    res1 = cis(dataset, itemset)
    
    tasks = {0 : set([1]), 1: set([3])}
    dataset = dataset.repartition(2)
    res2 = cisBatch(dataset, tasks)
    print 'res1 - %s' % res1
    print 'res2 - %s' % res2
