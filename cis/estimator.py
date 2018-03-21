import json


class Estimator(object):
    def __init__(self, sample):
        self._sample = sample

    def estimate_cis(self, itemset_list):
        def mapFunc(element):
            res = {}
            for item_set in itemset_list:
                if item_set.issubset(element):
                    key = json.dumps(sorted(list(item_set)))
                    if key not in res:
                        res[key] = [element, 1]
                    else:
                        res[key] = [res[key][0].intersection(element), res[key][1] + 1]
            for k, v in res.iteritems():
                if len(v) > 0:
                    yield (k, v)
        return {k: v for k, v in self._sample.flatMap(mapFunc).reduceByKey(lambda a,b: [a[0].intersection(b[0]), a[1] + b[1]]).collect()}

    def estimate_sizes(self, itemset_list):
        def mapFunc(element):
            for item_set in itemset_list:
                if item_set.issubset(element):
                    yield (json.dumps(sorted(list(item_set))), 1)
        return {k: v for k, v in self._sample.flatMap(mapFunc).reduceByKey(lambda a,b: a + b).collect()}

    def estimate(self, itemset_list):
        def mapFunc(element):
            for item_set in itemset_list:
                if item_set.issubset(element):
                    yield (json.dumps(sorted(list(item_set))), [element, 1])
        return self._sample.flatMap(mapFunc).reduceByKey(lambda a, b: [a[0].intersection(b[0]), a[1] + b[1]])

    def getSingletons(self):
        return self._sample.flatMap(lambda x: x).distinct()

    def estimate_commons(self, singletons, threshold):
        def mapFunc(itemset):
            for s in singletons:
                if s in itemset:
                    yield (s, 1)
        return self._sample.flatMap(mapFunc).reduceByKey(lambda a,b: a + b).filter(lambda x: x[1] > threshold)\
            .map(lambda x: x[0]).collect()
