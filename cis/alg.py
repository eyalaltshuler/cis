import utils
import frequents
import pyspark
import estimator
import logging
import time
import json
import math


log = logging.getLogger()


def alg(sc, data_set_rdd, data_set_size, threshold, epsilon, randomized=True):
    data_set_rdd.cache()
    partitions_num = data_set_rdd.getNumPartitions()
    sample_size = _calculate_sample_size(threshold, data_set_size, epsilon) if randomized else data_set_size
    collected_sample = data_set_rdd.take(sample_size)
    log.info('Using sample of size %d', sample_size)
    sample = data_set_rdd.sample(False, float(sample_size) / data_set_size)
    sample.cache()
    data_estimator = estimator.Estimator(sample)
    scaled_threshold = float(threshold) * sample_size / data_set_size
    log.info('Estimating singletons frequencies')
    start = time.time()
    frequencies = _countElements(collected_sample, scaled_threshold)
    common_elements = frequencies.keys()
    log.info('There are %d common elements', len(common_elements))
    log.info('Common elements are - %s', common_elements)
    end = time.time()
    log.info('Singletons frequencies computation completed in %d seconds', end - start)
    singletons = [(set([item]), frequencies[item] * data_set_size / sample_size) for item in common_elements]
    cis_tree = frequents.Frequents()

    candidates = [singleton[0] for singleton in singletons]
    iteration = 1

    while candidates:
        log.info('Iteration %d starts. candidates set size is %d', iteration, len(candidates))

        log.info('Starting Estimating and filtering. There are %d candidates', len(candidates))
        start = time.time()
        next_level = data_estimator.estimate(candidates).filter(lambda pair: pair[1][1] >= scaled_threshold).map(lambda x: (x[1][0], x[1][1] * data_set_size / sample_size))
        next_level.cache()
        cis_next_level = next_level.collect()
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
ALPHA = 0.01
def _calculate_sample_size(threshold, data_set_size, epsilon):
    return int(math.ceil((math.log(1 / epsilon) * 2 * data_set_size) / ((1 - ALPHA) * DELTA ** 2 * threshold)))


def _assign_tasks(candidate_to_workers, num_of_workers):
    pairs = [(candidate, required_workers) for candidate, required_workers in candidate_to_workers]
    pairs.sort(key=lambda x: x[1])
    tasks = [list() for _ in xrange(num_of_workers)]
    for candidate, required_workers in pairs:
        tasks.sort(key=lambda s: len(s))
        for i in xrange(required_workers):
            tasks[i].append(candidate)
    return tasks


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