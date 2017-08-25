import json


class Estimator(object):
    def __init__(self, sample):
        self._sample = sample

    def estimate_cis(self, itemset_list):
        def mapFunc(element):
            res = {}
            for item_set in itemset_list:
                if item_set.issubset(element):
                    key = json.dumps(list(item_set))
                    if key not in res:
                        res[key] = element
                    else:
                        res[key] = res[key].intersection(element)
            for k, v in res.iteritems():
                if len(v) > 0:
                    yield (k, v)
        return self._sample.flatMap(mapFunc).reduceByKey(lambda a,b: a.intersection(b)).collect()

    def estimate_sizes(self, itemset_list):
        def mapFunc(element):
            for item_set in itemset_list:
                if item_set.issubset(element):
                    yield (json.dumps(list(item_set)), 1)
        return self._sample.flatMap(mapFunc).reduceByKey(lambda a,b: a + b).collect()