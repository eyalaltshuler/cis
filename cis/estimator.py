class Estimator(object):
    def __init__(self, sample, datasetRdd):
        self._sample = sample
        self._datasetRdd = datasetRdd

    def estimateSizes(self, itemsetList):
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

    def estimateCis(self, itemsetTasks):
        pass
