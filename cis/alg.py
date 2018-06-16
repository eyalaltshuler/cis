import utils
import frequents
import pyspark
from estimator import Estimator
import logging
import time
import json
import math


log = logging.getLogger()


def alg(sc, data_set_rdd, data_set_size, threshold, epsilon, randomized=True, alpha=0.1):
    data_set_rdd.cache()
    partitions_num = data_set_rdd.getNumPartitions()
    sample_size = _calculate_sample_size_2(threshold, data_set_size, epsilon, alpha)
    collected_sample = data_set_rdd.sample(False, float(sample_size) / data_set_size).collect()
    collected_sample2 = data_set_rdd.sample(False, float(sample_size) / data_set_size).collect()
    collected_sample3 = data_set_rdd.sample(False, float(sample_size) / data_set_size).collect()
    collected_sample4 = data_set_rdd.sample(False, float(sample_size) / data_set_size).collect()
    collected_sample5 = data_set_rdd.sample(False, float(sample_size) / data_set_size).collect()
    log.info('Using sample of size %d', sample_size)
    print 'Using sample of size %d' % sample_size
    print 'ratio - %f' % (float(sample_size) / data_set_size)
    scaled_threshold = float(threshold) * sample_size / data_set_size if randomized else threshold
    frequencies1 = _countElements(collected_sample, float(threshold) * sample_size / data_set_size)
    common_elements1 = set(frequencies1.keys())
    frequencies2 = _countElements(collected_sample2, float(threshold) * sample_size / data_set_size)
    common_elements2 = set(frequencies2.keys())
    frequencies3 = _countElements(collected_sample3, float(threshold) * sample_size / data_set_size)
    common_elements3 = set(frequencies3.keys())
    frequencies4 = _countElements(collected_sample4, float(threshold) * sample_size / data_set_size)
    common_elements4 = set(frequencies4.keys())
    frequencies5 = _countElements(collected_sample5, float(threshold) * sample_size / data_set_size)
    common_elements5 = set(frequencies5.keys())
    common_candidates = common_elements1.union(common_elements2).union(common_elements3).union(common_elements4).union(common_elements5)
    common_elements_set = set()
    for candidate in common_candidates:
        i = 0
        if candidate in common_elements1:
            i += 1
        if candidate in common_elements2:
            i += 1
        if candidate in common_elements3:
            i += 1
        if candidate in common_elements4:
            i += 1
        if candidate in common_elements5:
            i += 1
        if i >= 3:
            common_elements_set.add(candidate)
    # frequencies = _get_averages(frequencies1, frequencies2, frequencies3)
    # common_elements = [k for k in frequencies.keys() if frequencies[k] >= float(threshold) * sample_size / data_set_size]
    common_elements = list(common_elements_set)
    data_estimator = Estimator(sc.parallelize(collected_sample)) if randomized \
        else Estimator(data_set_rdd)

    # log.info('Estimating singletons frequencies')
    # start = time.time()

    # log.info('There are %d common elements', len(common_elements))
    # log.info('Common elements are - %s', common_elements)
    # end = time.time()
    # log.info('Singletons frequencies computation completed in %d seconds', end - start)
    # singletons = [(set([item]), frequencies[item] * data_set_size / sample_size) for item in common_elements]
    singletons = data_estimator.getSingletons()
    # common_elements = data_estimator.estimate(singletons)
    cis_tree = frequents.Frequents()
    # common_cached = data_estimator.estimate_commons(singletons.collect(), scaled_threshold)
    candidates = [set([i]) for i in common_elements]
    iteration = 1
    scaling_factor = data_set_size / sample_size if randomized else 1.0

    while candidates:
        log.info('Iteration %d starts. candidates set size is %d', iteration, len(candidates))
        log.info('Starting Estimating and filtering. There are %d candidates', len(candidates))
        start = time.time()
        next_level = data_estimator.estimate(candidates).filter(lambda pair: pair[1][1] >= scaled_threshold).map(lambda x: (x[1][0], int(min(x[1][1] * scaling_factor, data_set_size))))
        next_level.cache()
        cis_next_level = next_level.collect()
        cis_next_level = filter(lambda x: x[0].issubset(common_elements_set), cis_next_level)
        end = time.time()
        log.info('Estimation and filter done in %d seconds. Filtering candidates', end - start)
        if not cis_next_level:
            log.info('No candidates remained. Quiting iteration %d', iteration)
            break
        log.info('Adding new computed level to the resulting lattice, of size %d', len(cis_next_level))
        log.info('New level is - %s', cis_next_level)
        start = time.time()
        cis_tree.add_level(cis_next_level)
        end = time.time()
        log.info('Next level addition to lattice completed in %d seconds', end - start)
        start = time.time()
        candidates = _expand(next_level, common_elements, partitions_num)
        end = time.time()
        log.info('Fast expansion took %d seconds and created %d candidates, Iteration %d completed', end - start, len(candidates), iteration)
        log.info('New candidates are %s', candidates)

        iteration += 1

    if not randomized:
        cis_tree.result = [(itemset.items, itemset.frequency) for itemset in cis_tree.get_all()]
        cis_tree.result = {str(sorted(list(i[0]))): i[1] for i in cis_tree.result}
        return cis_tree
    # return cis_tree

    estimator = Estimator(data_set_rdd)
    final_itemsets = [itemset.items for itemset in cis_tree.get_all()]
    cis_tree.result = estimator.compute(final_itemsets).collect()
    cis_tree.result = {str(sorted(list(json.loads(i[0])))): i[1] for i in cis_tree.result}
    return cis_tree


def _expand(level, common_elements, partitions_num):
    def _flatMap(element):
        for common_element in common_elements:
            if common_element not in element[0]:
                res = element[0].copy()
                res.add(common_element)
                yield json.dumps(sorted(list(res)))
    return level.flatMap(_flatMap).distinct(partitions_num).map(lambda x: set(json.loads(x))).collect()


DELTA = 0.1


def _calculate_sample_size(_threshold, _data_set_size, _epsilon, alpha=0.01):
    return 2.0 * int(math.ceil((math.log(1 / _epsilon) * 2 * _data_set_size) / ((1 - alpha) * DELTA ** 2 * _threshold)))


def _calculate_sample_size_2(_threshold, _data_set_size, _epsilon, alpha=0.1):
    return int(math.ceil(20 *  (math.log(1 / _epsilon) * 2 * _data_set_size) / ((1 - alpha) ** 2 * _threshold)))


def _assign_tasks(candidate_to_workers, num_of_workers):
    pairs = [(candidate, required_workers) for candidate, required_workers in candidate_to_workers]
    pairs.sort(key=lambda x: x[1])
    tasks = [list() for _ in xrange(num_of_workers)]
    for candidate, required_workers in pairs:
        tasks.sort(key=lambda s: len(s))
        for i in xrange(required_workers):
            tasks[i].append(candidate)
    return tasks


def _get_averages(frequencies1, frequencies2, frequencies3):
    res = {}
    occurs = {}
    for k in frequencies1.keys():
        res[k] = frequencies1[k]
        occurs[k] = 1

    for k in frequencies2.keys():
        x = frequencies2.get(k, 0)
        res[k] = x + frequencies2[k]
        t = occurs.get(k, 0)
        occurs[k] = t + 1

    for k in frequencies3.keys():
        x = frequencies3.get(k, 0)
        res[k] = x + frequencies3[k]
        t = occurs.get(k, 0)
        occurs[k] = t + 1

    for i in res.keys():
        res[i] = float(res[i]) / occurs[i]

    return res


def _countElements(dataset, threshold):
    # return {k: v for k, v in dataset.flatMap(lambda t: [(e,1) for e in t]).reduceByKey(lambda a,b: a+b).filter(lambda x: x[1] >= threshold).collect()}
    res = {}
    for itemset in dataset:
        for item in itemset:
            if item in res:
                res[item] += 1
                continue
            res[item] = 1
    return {k: v for k, v in res.iteritems() if v >= threshold}
    # return res


if __name__ == "__main__":
    transactionSet = [set([1, 2, 3, 4]), set([1, 2, 3]), set([4, 3]), set([1]), set([2, 4])] * 1000000
    print 'Creating spark context object'
    sc = pyspark.SparkContext()
    transactionRdd = sc.parallelize(transactionSet)
    threshold = 400000
    epsilon = 0.001
    data_set_size = 5000000
    start = time.time()
    res = alg(sc, transactionRdd, data_set_size, threshold, epsilon)
    end = time.time()
    print 'alg computation took %s seconds' % (end - start)
    print 'Result is %s' % res
    sc.stop()