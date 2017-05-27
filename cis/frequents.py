import cPickle
import ast
import sys
import Queue

SYS_RECURESION_LIMIT = 10 ** 6

class Frequents(object):
    def __init__(self, singletons):
        self._singletons = [Node(item, frequency) for item, frequency in singletons]
        self._topLevel = self._singletons

    def is_frequent(self, item_set, threshold):
        pass
            
    def get_frequency(self, item_set):
        pass

    def _find_smallest_containing_item_set(self, itemset, threshold):
        pass

    def add_level(self, item_set_list):
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
                res[key] = itemset.frequency
            for child in itemset.children:
                queue.put(child)
        return res

    def frequentItemset(self):
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

    def compare(self, other):
        pass


class Node(object):
    def __init__(self, item_set, frequency):
        self.items = item_set
        self.frequency = frequency
        self.parents = set()
        self.children = set()