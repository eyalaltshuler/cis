import logging
import pyspark

import frequents

from cis import utils


def alg_naive(data_set_rdd, threshold):
    frequencies = utils.countElements(data_set_rdd)
    data_set_size = data_set_rdd.count()
    common_elements = filter(lambda k: frequencies[k] >= threshold, frequencies.keys())
    candidates = [set([item]) for item in common_elements]
    candidates_frequencies = [(candidate, frequencies[candidate.copy().pop()]) for candidate in candidates]
    cis_tree = frequents.Frequents(candidates_frequencies)
    partitions_num = data_set_rdd.getNumPartitions()

    while candidates:
        new_level = []
        for candidate in candidates:
            candidate_size, candidate_cis = utils.cis(data_set_rdd, candidate)
            if candidate_size >= threshold:
                new_level.append((candidate_cis, candidate_size))
        if not new_level:
            break
        cis_tree.add_level(new_level)
        candidates = cis_tree.expand()

    return cis_tree