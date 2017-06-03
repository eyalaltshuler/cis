import utils
import frequents
import pyspark
import estimator
import logging

log = logging.getLogger()

def alg(sc, data_set_rdd, threshold, epsilon, randomized=True, sample=None):
    data_set_size = data_set_rdd.count()
    sample_size = _calculate_sample_size(threshold, data_set_size)
    if sample is None:
        sample = data_set_rdd.takeSample(False, sample_size)
    log.info('Computing singletons frequencies')
    frequencies = utils.countElements(data_set_rdd)
    log.info('Singletons frequencies computation done')
    common_elements = filter(lambda k: frequencies[k] >= threshold, frequencies.keys())
    log.info('Number of common elements - %d', len(common_elements))
    candidates = [set([item]) for item in common_elements]
    candidates_frequencies = [(candidate, frequencies[candidate.copy().pop()]) for candidate in candidates]
    cis_tree = frequents.Frequents(candidates_frequencies)
    data_estimator = estimator.Estimator(sample, data_set_rdd, data_set_size)
    partitions_num = data_set_rdd.getNumPartitions()

    iteration = 1
    while candidates:
        log.info('Iteration %d starts. candidates set size is %d', iteration, len(candidates))
        log.info('Estimating candidates sizes')
        candidates_size = data_estimator.estimate_sizes(candidates)
        log.info('Candidtes estimation done. Fitering candidates')
        candidates_size = filter(lambda x: x[1] >= threshold, candidates_size)
        log.info('Candidates filter done. Sorting candidates according to their sizes')
        candidates_size.sort(key=lambda x: hash(str(x[0])))
        if not candidates_size:
            log.info('No candidates remained. Quiting iteration %d', iteration)
            break
        log.info('Computing workers required for every candidate')
        candidates_to_workers = [(candidate, utils.requiredNumOfWorkers(data_set_size,
                                                                        size,
                                                                        partitions_num,
                                                                        epsilon, sample=randomized)) for candidate, size in candidates_size]
        log.info('Assiging tasks to workers')
        tasks = _assign_tasks(candidates_to_workers, partitions_num)
        log.info('Workers tasks assignment completed. Starting closure computation')
        next_level_of_cis = data_estimator.estimate_cis(tasks)
        log.info('Closure computation done')
        next_level_of_cis.sort(key=lambda x: hash(str(x[0])))
        frequents_new_level = [(next_level_of_cis[i][1], candidates_size[i][1]) for i in xrange(len(next_level_of_cis))]
        log.info('Adding new computed level to the resulting lattice')
        cis_tree.add_level(frequents_new_level)
        # candidates = cis_tree.expand()
        log.info('Lattice new level added. Expanding computation to new candidates')
        candidates = cis_tree.fastExpand(sc, partitions_num)
        log.info('Candidates computation completed.')
        log.info('Iteration %d completed', iteration)
        iteration += 1
    return cis_tree


def _calculate_sample_size(required_threshold, data_set_size):
    return min(4000, data_set_size)


def _assign_tasks(candidate_to_workers, num_of_workers):
    pairs = [(candidate, required_workers) for candidate, required_workers in candidate_to_workers]
    pairs.sort(key=lambda x: x[1])
    tasks = [list() for _ in xrange(num_of_workers)]
    for candidate, required_workers in pairs:
        tasks.sort(key=lambda s: len(s))
        for i in xrange(required_workers):
            tasks[i].append(candidate)
    return tasks