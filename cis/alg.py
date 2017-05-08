import utils
import frequents
import pyspark
import estimator


def alg(sc, data_set_rdd, threshold, epsilon):
    data_set_size = data_set_rdd.count()
    sample_size = _calculate_sample_size(threshold, data_set_size)
    sample = data_set_rdd.takeSample(False, sample_size)
    frequencies = utils.countElements(data_set_rdd)
    common_elements = filter(lambda k: frequencies[k] >= threshold, frequencies.keys())
    print 'Number of common elements - %d' % len(common_elements)
    candidates = [set([item]) for item in common_elements]
    candidates_frequencies = [(candidate, frequencies[candidate.copy().pop()]) for candidate in candidates]
    cis_tree = frequents.Frequents(candidates_frequencies)
    data_estimator = estimator.Estimator(sample, data_set_rdd, data_set_size)
    partitions_num = data_set_rdd.getNumPartitions()

    while candidates:
        print 'candidates set size is %d' % len(candidates)
        candidates_size = data_estimator.estimate_sizes(candidates)
        candidates_size = filter(lambda x: x[1] >= threshold, candidates_size)
        candidates_size.sort(key=lambda x: hash(str(x[0])))
        if not candidates_size:
            break
        candidates_to_workers = [(candidate, utils.requiredNumOfWorkers(data_set_size,
                                                                        size,
                                                                        partitions_num,
                                                                        epsilon, sample=False, one=True)) for candidate, size in candidates_size]
        tasks = _assign_tasks(candidates_to_workers, partitions_num)
        next_level_of_cis = data_estimator.estimate_cis(tasks)
        next_level_of_cis.sort(key=lambda x: hash(str(x[0])))
        frequents_new_level = [(next_level_of_cis[i][1], candidates_size[i][1]) for i in xrange(len(next_level_of_cis))]
        cis_tree.add_level(frequents_new_level)
        # candidates = cis_tree.expand()
        candidates = cis_tree.fastExpand(sc, partitions_num)
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