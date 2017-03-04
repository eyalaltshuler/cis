import pickle

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
                    value = hash(str(new_item_set_node.items))
                    if value in tmp:
                        continue
                    else:
                        tmp.add(value)
                        new_level.append(new_item_set_node)
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

    @classmethod
    def save(cls, obj, location):
        pickle.dump(obj, location)

    @classmethod
    def load(cls, location):
        return pickle.loads(open(location))


class Node(object):
    def __init__(self, item_set, frequency):
        self.items = item_set
        self.frequency = frequency
        self.parents = set()
        self.children = set()