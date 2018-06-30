import cPickle
import ast
import sys
import Queue
import time
import numpy as np
import json

from itertools import combinations

SYS_RECURESION_LIMIT = 10 ** 6

class Frequents(object):
    def __init__(self):
        self._singletons = []
        self._levels = []
        self._all = []

    def print_level(self, i):
        if i >= len(self._levels):
            raise Exception('Illegel level - %d' % i)
        res = []
        for freqneut_itemset in self._levels[i]:
            res.append('[%s, %d]' % (freqneut_itemset.items, freqneut_itemset.frequency))
        print ",".join(res)

    def add_level(self, item_set_list):
        new_level = [Node(itemset, frequency) for itemset, frequency in item_set_list]
        self._levels.append(new_level)
        self._all += new_level
        self._topLevel = self._levels[-1]

    def get_all(self):
        return self._all

    def _add_level(self, item_set_list):
        top_level_nodes = self._topLevel
        candidates = [Node(itemset, frequency) for itemset, frequency in item_set_list]
        new_level = []
        tmp = set()
        for new_item_set_node in candidates:
            for old_node in top_level_nodes:
                if old_node.items.issubset(new_item_set_node.items) and not \
                   new_item_set_node.items.issubset(old_node.items):
                    old_node.children.add(new_item_set_node)
                    new_item_set_node.parents.add(old_node)
                    value = hash(str(sorted(list(new_item_set_node.items))))
                    if value in tmp:
                        continue
                    else:
                        tmp.add(value)
                        new_level.append(new_item_set_node)
        if new_level:
            self._topLevel = new_level

    def expand(self):
        tmp = set()
        result = list()
        for item_set in [n.items for n in self._topLevel]:
            for item in [n.items for n in self._singletons]:
                value = item.copy().pop()
                if value not in item_set:
                    to_add = item_set.copy()
                    to_add.add(value)
                    h = hash(str(to_add))
                    if h not in tmp:
                        result.append(to_add)
                        tmp.add(h)
        return result

    def fastExpand(self, sc, partitions_num):
        top_level_items = [node.items for node in self._topLevel]
        single_items = [node.items.copy().pop() for node in self._singletons]
        top_level_rdd = sc.parallelize(top_level_items, partitions_num)

        def mapFunc(itemset):
            to_add = set(single_items).difference(itemset)
            for element in to_add:
                s = itemset.copy()
                s.add(element)
                yield (str(list(s)), None)

        res = top_level_rdd.flatMap(mapFunc).reduceByKey(lambda a, b: a).map(lambda x: set(ast.literal_eval(x[0]))).collect()
        return [itemset for itemset in res if itemset not in top_level_items]

    def frequentsDict(self):
        res = {}
        queue = Queue.Queue()
        for level in self._levels:
            for itemset in level:
                key = str(itemset.items)
                if key not in res:
                    res[key] = (itemset.items, itemset.frequency)
        return res

    def frequentItemsets(self):
        res = []
        tmp = []
        queue = Queue.Queue()
        for singleton in self._singletons:
            queue.put(singleton)
        while not queue.empty():
            itemset = queue.get()
            key = str(sorted(list(itemset.items)))
            if key not in tmp:
                tmp.append(key)
                res.append(itemset.items)
            for child in itemset.children:
                queue.put(child)
        return res

    @staticmethod
    def save(obj, location):
        sys.setrecursionlimit(SYS_RECURESION_LIMIT)
        cPickle.dump(obj, file(location, 'w'))

    @staticmethod
    def load(location):
        sys.setrecursionlimit(SYS_RECURESION_LIMIT)
        with open(location) as f:
            res = cPickle.loads(f.read())
        return res

    def _find_frequency(self, h, results):
        res = [(set(json.loads(k)), v) for k, v in results.iteritems()]
        itemset = set(json.loads(h))
        frequency = 10 ** 9  # initial value
        for i in res:
            if i[0].issubset(itemset):
                frequency = min(frequency, i[1])
        return frequency

    def calc_error(self, exacts, alpha=0.01):
        res = []
        wrong_cis_num = 0
        detected_cis_num = 0
        #exact_keys = exacts.result.keys()
        exact_keys = exacts.keys()
        approximated_keys = self.result.keys()
        freq_not_identified = 0.0
        approximated_keys_sets = [v[0] for v in exacts.values()]
        for s in exact_keys:
            if s not in approximated_keys:
                set_s = set(json.loads(s))
                if not any([set_s.issubset(y) for y in approximated_keys_sets]):
                    freq_not_identified += 1
        for h, freq in self.result.iteritems():
            approx_freq = freq
            if h not in exact_keys:
                exact_freq = self._find_frequency(h, exacts)
            else:
                exact_freq = exacts[h][1]
            res.append(float(approx_freq) / exact_freq)
        return freq_not_identified / len(exact_keys), len(approximated_keys) / float(len(exact_keys)), np.average(res)


    def iterate_over_subsets(self):
        frequency_dict = self.frequentsDict()
        for itemset, freq in frequency_dict.values():
            for i in xrange(1, len(itemset)):
                for sub in combinations(itemset, i):
                    yield set(sub), freq

class Node(object):
    def __init__(self, item_set, frequency):
        self.items = item_set
        self.frequency = frequency
        self.parents = set()
        self.children = set()