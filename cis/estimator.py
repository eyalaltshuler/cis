import ast
from pyspark import PickleSerializer

class Estimator(object):
    def __init__(self, sample, datasetRdd, data_set_size):
        self._sample = sample
        self._sampleSize = len(sample)
        self._dataset_rdd = datasetRdd
        self._data_set_size = data_set_size
        self._serializer = PickleSerializer()

    def estimate_cis(self, tasks):
        ''' 
        Assumes that dataset is an RDD that can be quried for its partitions number
        tasksDict maps between workers to their tasks
        '''
        def partition_func(i, partition_iter):
            data = []
            for element in partition_iter:
                data.append(element)
            task_i = tasks[i]
            for item_set in task_i:
                projected = filter(lambda t: item_set.issubset(t), data)
                if projected:
                    yield (str(sorted(list(item_set))), reduce(lambda a,b: a.intersection(b), projected))

        tmp =  self._dataset_rdd.mapPartitionsWithIndex(partition_func, preservesPartitioning=True).\
                                 reduceByKey(lambda a,b: a.intersection(b)).collect()
        return [(set(ast.literal_eval(k)), v) for k,v in tmp]

    def estimate_sizes(self, itemset_list):
        return [(itemset, self._count_itemset_in_sample(itemset)  * self._data_set_size / self._sampleSize) for itemset in itemset_list]

    def _count_itemset_in_sample(self, itemset):
        return len(filter(lambda transaction: itemset.issubset(transaction), self._sample))