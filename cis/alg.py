import parallelOps
import pyspark
import cis


def alg(sc, datasetRdd, threshold, epsilon):
    frequencies = parallelOps.countElements(transactionRdd))
    datasetSize = datasetRdd.count()
    commonElements = filter(frequencies, lambda a: a(1) > threshold)
    sampleSize = _calcSampleSize(threshold)
    sample = datasetRdd.takeSample(sampleSize)
    candidates = commonElements
    res = lattice.Lattice(commonElements)

    while candidates:
        candidatesSize = _estimaiteSizes(candidates, sample)
        workers = {k: cis.requiredNumOfWorkers(datasetSize, datasetRdd.workers, candidatesSize[k], epsilon) for k in candidates}
        tasks = _assignTasks(candidates, workersNeeded)
        nextLevel = parallelOps.cisBatch(datasetRdd, tasks)
        filteredNextLevel = filter(nextLevel, lambda a: inSample(a, threshold))
        res.add(filteredNextLevel)
        candidates = _expand(filteredNextLevel, sample)

    return res

def _calcSampleSize(threshod):
    return 6

def _assignTasks(candidates, workersNeeded):
    return dict()


if __name__=="__main__":
    transactionSet = [set(1,2,3,4), set(1,2,3), set(4,3), set(1), set(2,4)]
    sc = pyspark.SparkContext()
    transactionRdd = sc.parallelize(transactionSet)
    threshold = 2
    epsilon = 0.1
    res = alg(sc, transactionRdd, threshold, epsilon)
