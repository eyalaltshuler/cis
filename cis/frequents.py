import cPickle
import ast
import sys
import Queue
import time
from itertools import combinations

SYS_RECURESION_LIMIT = 10 ** 6

class Frequents(object):
    def __init__(self, singletons):
        self._singletons = [Node(item, frequency) for item, frequency in singletons]
        self._levels = [self._singletons]
        self._topLevel = self._singletons

    def add_level(self, item_set_list):
        self._levels.append([Node(itemset, frequency) for itemset, frequency in item_set_list])
        self._topLevel = self._levels[-1]

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
        for singleton in self._singletons:
            queue.put(singleton)
        while not queue.empty():
            itemset = queue.get()
            key = str(itemset.items)
            if key not in res:
                res[key] = (itemset.items, itemset.frequency)
            for child in itemset.children:
                queue.put(child)
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

    def calc_error(self, exactLattice):
        errors = []
        wrong_cis_num = 0
        cache = set()
        for key, freq in self.frequentsDict().iteritems():
            h = hash(str(sorted(list(freq[0]))))
            if h not in cache:
                cache.add(h)
                if key not in exactLattice.frequentsDict().keys():
                    wrong_cis_num += 1
                    continue
                approx_freq = freq[1]
                exact_freq = exactLattice.frequentsDict()[key][1]
                errors.append(abs(float(exact_freq - approx_freq)) / exact_freq)
        return sum(errors) / len(errors), wrong_cis_num


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