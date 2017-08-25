import utils
import frequents
import pyspark
import estimator
import logging
import time
import json
import math


log = logging.getLogger()


def alg(sc, data_set_rdd, threshold, epsilon, randomized=True):
    data_set_rdd.cache()
    start = time.time()
    # data_set_size = data_set_rdd.count()
    # data_set_size = data_set_rdd.countApprox(timeout=5000, confidence=0.95)
    data_set_size = 100000
    end = time.time()
    log.info('Counting data set size took %d seconds, size approximated as %d', end - start, data_set_size)
    partitions_num = data_set_rdd.getNumPartitions()
    sample_size = _calculate_sample_size(threshold, data_set_size, epsilon) if randomized else data_set_size
    log.info('Using sample of size %d', sample_size)
    sample = data_set_rdd.sample(False, float(sample_size) / data_set_size)
    sample.cache()
    data_estimator = estimator.Estimator(sample)
    scaled_threshold = float(threshold) * sample_size / data_set_size
    log.info('Estimating singletons frequencies')
    start = time.time()
    frequencies = _countElements(sample, scaled_threshold)
    common_elements = frequencies.keys()
    end = time.time()
    log.info('Singletons frequencies computation completed in %d seconds', end - start)
    # common_elements = filter(lambda k: frequencies[k] >= scaled_threshold, frequencies.keys())
    # log.info('Number of common elements - %d', len(common_elements))
    singletons = [(set([item]), frequencies[item] * data_set_size / sample_size) for item in common_elements]
    cis_tree = frequents.Frequents(singletons)

    candidates = [singleton[0] for singleton in singletons]
    iteration = 1

    while candidates:
        # log.info('Iteration %d starts. candidates set size is %d', iteration, len(candidates))

        # log.info('Startin Estimating and filtering.')
        # start = time.time()
        next_level = data_estimator.estimate(candidates).filter(lambda pair: pair[1][1] >= scaled_threshold).map(lambda x: (x[1][0], x[1][1] * data_set_size / sample_size))
        next_level.cache()
        cis_next_level = next_level.collect()
        # end = time.time()
        # log.info('Estimation and filter done in %d seconds. Filtering candidates', end - start)
        if not cis_next_level:
            # log.info('No candidates remained. Quiting iteration %d', iteration)
            break
        # log.info('Adding new computed level to the resulting lattice')
        # start = time.time()
        cis_tree.add_level(cis_next_level)
        # end = time.time()
        # log.info('Next level addition to lattice completed in %d seconds', end - start)
        # start = time.time()
        # candidates = cis_tree.fastExpand(level)
        candidates = _expand(next_level, common_elements)
        # end = time.time()
        #log.info('Fast expansion took %d seconds, Iteration %d completed', end - start, iteration)

        iteration += 1

    return cis_tree

def _expand(level, common_elements):
    def _flatMap(element):
        for common_element in common_elements:
            if common_element not in element[0]:
                yield element[0].add(common_element)
    return level.flatMap(_flatMap).collect()


def _alg(sc, data_set_rdd, threshold, epsilon, randomized=True):
    data_set_rdd.cache()
    start = time.time()
    # data_set_size = data_set_rdd.count()
    # data_set_size = data_set_rdd.countApprox(timeout=5000, confidence=0.95)
    data_set_size = 100000
    end = time.time()
    log.info('Counting data set size took %d seconds, size approximated as %d', end - start, data_set_size)
    partitions_num = data_set_rdd.getNumPartitions()
    sample_size = _calculate_sample_size(threshold, data_set_size, epsilon) if randomized else data_set_size
    log.info('Using sample of size %d', sample_size)
    sample = data_set_rdd.sample(False, float(sample_size) / data_set_size)
    sample.cache()
    data_estimator = estimator.Estimator(sample)
    scaled_threshold = float(threshold) * sample_size / data_set_size
    log.info('Estimating singletons frequencies')
    start = time.time()
    frequencies = _countElements(sample)
    end = time.time()
    log.info('Singletons frequencies computation completed in %d seconds', end - start)
    common_elements = filter(lambda k: frequencies[k] >= scaled_threshold, frequencies.keys())
    log.info('Number of common elements - %d', len(common_elements))
    singletons = [(set([item]), frequencies[item] * data_set_size / sample_size) for item in common_elements]
    cis_tree = frequents.Frequents(singletons)

    candidates = [singleton[0] for singleton in singletons]
    iteration = 1

    while candidates:
        log.info('Iteration %d starts. candidates set size is %d', iteration, len(candidates))

        log.info('Estimating candidates sizes')
        start = time.time()
        candidates_sizes = data_estimator.estimate_sizes(candidates)
        end = time.time()
        log.info('Candidates estimation done in %d seconds. Filtering candidates', end - start)

        start = time.time()
        candidates = filter(lambda candidate: candidates_sizes[json.dumps(sorted(list(candidate)))] >= scaled_threshold, candidates)
        end = time.time()
        log.info('Candidates filter done in %d seconds. Starting closure computation', end - start)

        if not candidates:
            log.info('No candidates remained. Quiting iteration %d', iteration)
            break

        start = time.time()
        next_level_of_cis = data_estimator.estimate_cis(candidates)
        end = time.time()
        log.info('Closure computation done in %d seconds', end - start)

        frequents_new_level = [(v, candidates_sizes[k] * data_set_size / sample_size) for k, v in next_level_of_cis.iteritems()]
        log.info('Adding new computed level to the resulting lattice')
        start = time.time()
        cis_tree.add_level(frequents_new_level)
        end = time.time()
        log.info('Next level addition to lattice completed in %d seconds', end - start)

        start = time.time()
        candidates = cis_tree.fastExpand(sc, partitions_num)
        end = time.time()
        log.info('Fast expansion took %d seconds, Iteration %d completed', end - start, iteration)

        iteration += 1

    return cis_tree

def old_alg(sc, data_set_rdd, threshold, epsilon, randomized=True, sample=None):
    data_set_rdd.cache()
    data_set_size = data_set_rdd.count()
    sample_size = _calculate_sample_size(threshold, data_set_size)
    data_estimator = estimator.Estimator(sc, sample, sample, 10000)
    if sample is None:
        sample = data_set_rdd.takeSample(False, sample_size)
    log.info('Computing singletons frequencies')
    threshold = threshold * float(10000) / data_set_size
    frequencies = utils.countElements(data_estimator._sample)
    log.info('Singletons frequencies computation done')
    common_elements = filter(lambda k: frequencies[k] >= threshold, frequencies.keys())
    log.info('Number of common elements - %d', len(common_elements))
    candidates = [set([item]) for item in common_elements]
    candidates_frequencies = [(candidate, frequencies[candidate.copy().pop()] * data_set_size / float(10000)) for candidate in candidates]
    cis_tree = frequents.Frequents(candidates_frequencies)

    # partitions_num = data_set_rdd.getNumPartitions()
    partitions_num = data_estimator._sample.getNumPartitions()

    iteration = 1
    while candidates:
        log.info('Iteration %d starts. candidates set size is %d', iteration, len(candidates))
        log.info('Estimating candidates sizes')
        start = time.time()
        candidates_size = data_estimator.estimate_sizes(candidates)
        end = time.time()
        log.info('Candidates estimation done in %d seconds. Filtering candidates', end - start)
        start = time.time()
        candidates_size = filter(lambda x: x[1] >= threshold, candidates_size)
        end = time.time()
        log.info('Candidates filter done in %d seconds. Sorting candidates according to their sizes', end - start)
        start = time.time()
        candidates_size.sort(key=lambda x: hash(str(x[0])))
        end = time.time()
        log.info('Candidates sort according to sizes done in %d seconds', end - start)
        if not candidates_size:
            log.info('No candidates remained. Quiting iteration %d', iteration)
            break
        log.info('Computing workers required for every candidate')
        start = time.time()
        candidates_to_workers = [(candidate, utils.requiredNumOfWorkers(data_set_size,
                                                                        size,
                                                                        partitions_num,
                                                                        epsilon, sample=False)) for candidate, size in candidates_size]
        end = time.time()
        log.info('Computation of required workers done in %d seconds', end - start)
        log.info('Assinging tasks to workers')
        start = time.time()
        tasks = _assign_tasks(candidates_to_workers, partitions_num)
        end = time.time()
        log.info('Workers tasks assignment completed in %d seconds. Starting closure computation', end - start)
        start = time.time()
        next_level_of_cis = data_estimator.estimate_cis(tasks)
        end = time.time()
        log.info('Closure computation done in %d seconds', end - start)
        start = time.time()
        next_level_of_cis.sort(key=lambda x: hash(str(x[0])))
        end = time.time()
        log.info('Next level sort done in %d seconds', end - start)
        frequents_new_level = [(next_level_of_cis[i][1], candidates_size[i][1] * data_set_size / float(10000)) for i in xrange(len(next_level_of_cis))]
        log.info('Adding new computed level to the resulting lattice')
        start = time.time()
        cis_tree.add_level(frequents_new_level)
        end = time.time()
        log.info('Next level addition to lattice completed in %d seconds', end - start)
        # candidates = cis_tree.expand()
        log.info('Lattice new level added. Expanding computation to new candidates')
        start = time.time()
        candidates = cis_tree.fastExpand(sc, partitions_num)
        end = time.time()
        log.info('Fast expansion took %d seconds', end - start)
        log.info('Candidates computation completed.')
        log.info('Iteration %d completed', iteration)
        iteration += 1
    return cis_tree


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
    return {k: v for k, v in dataset.flatMap(lambda t: [(e,1) for e in t]).reduceByKey(lambda a,b: a+b).filter(lambda x: x[1] >= threshold).collect()}