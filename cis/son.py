from pyspark import SparkContext, SparkConf
from cis import apriori


def map_apriori(threshold):
    def apriori_func(iter):
        data = []
        for element in iter:
            data.append(element)
        _, supportData = apriori(data, threshold)
        for k, v in supportData.iteritems():
            yield (k, v)
    return apriori_func


def mapCandidatesCreator(candidates):
    def mapCountCandidates(iter):
        data = []
        for element in iter:
            data.append(set(element))
        for candidate in candidates:
            filtered = filter(lambda a: candidate.issubset(a))
            yield (candidate, len(filtered))
    return mapCountCandidates


def son(dataset, threshold):
    factorized_threshold = threshold / dataset.getNumPartitions()

    mapFunc = map_apriori(factorized_threshold)
    supports = dataset.mapPartitions(mapFunc)
    maximal_supports = supports.reduceByKey(lambda a,b: max(a,b))
    filtered_maximal_supports = maximal_supports.filter(lambda x: x[1] >= factorized_threshold)
    candidates = filtered_maximal_supports.map(lambda x: x[0]).collect()

    map_candidates_func = mapCandidatesCreator(candidates)
    frequents = dataset.mapPartitions(map_candidates_func).\
                        reduceByKey(lambda a,b: a+b).\
                        filter(lambda a: a[1] >= threshold).collect()

    return frequents

if __name__ == "__main__":
    son()