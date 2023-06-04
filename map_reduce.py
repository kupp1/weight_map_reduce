import itertools
from typing import Callable, Sequence, NamedTuple, Any, List, Tuple

import dispy

WeightType = int
Pair = Tuple[Any, Any]
ReducePart = List[Pair]
ReduceChunk = List[Pair]


def get_key(pair):
    return pair[0]


def group_by(sequence, key_getter):
    import itertools
    sorted_seq = sorted(sequence, key=key_getter)

    def generator():
        for key, grouped_seq in itertools.groupby(sorted_seq, key=key_getter):
            yield key, list(grouped_seq)
    return generator()


def group_to_2d_list(sequence, key):
    result = []
    for _, grouped_seq in group_by(sequence, key):
        result.append(grouped_seq)
    return result


def group_to_dict(sequence, key):
    result = {}
    for key, grouped_seq in group_by(sequence, key):
        result[key] = grouped_seq
    return result


def sublists_len(seq):
    return sum(len(part) for part in seq)


def split(a, n):
    if len(a) < n:
        return [a]
    k, m = divmod(len(a), n)
    return list(a[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(n))


class Node(NamedTuple):
    host: str
    cores: int
    weight: WeightType


class WeightCluster(NamedTuple):
    cluster: dispy.JobCluster
    weight: WeightType
    nodes: List[Node]

    def get_total_cores(self):
        return sum(node.cores for node in self.nodes)

    def close(self):
        self.cluster.close()


def flatten_list(seq):
    return list(itertools.chain.from_iterable(seq))


def get_1st_n_parts_merged_and_rest(parts, n):
    return flatten_list(parts[:n]), parts[n:]


def reduce_part(reducer, part):
    reduced_pair = part[0]
    for pair in part[1:]:
        reduced_pair = reducer(reduced_pair, pair)
    return reduced_pair


def generate_reduce_chunk_wrapper(reducer):  # will be executed on nodes
    def reduce_chunk(chunk):
        results = []
        # part are groped by hash, so we need to re-group them be value
        parts = group_to_2d_list(chunk, key=lambda pair: get_key(pair))
        for part in parts:
            result = reduce_part(reducer, part)
            results.append(result)

        return results

    return reduce_chunk


class WeightMapReduce:
    def __init__(self, mapper: Callable[[Any], Sequence[Pair]],
                 reducer: Callable[[Pair, Pair], Pair],
                 nodes):
        self.mapper = mapper
        self.reducer = reducer
        self.nodes = nodes

    @staticmethod
    def close_weight_clusters(weight_clusters):
        for weight_cluster in weight_clusters:
            weight_cluster.close()

    @staticmethod
    def print_weight_clusters_stats(name, weight_clusters):
        print(f"\n{name} cluster stats:")
        for weight_cluster in weight_clusters:
            print(f"{weight_cluster.weight} weight nodes:", end="")
            weight_cluster.cluster.print_status()
            print()

    def execute(self, data):
        # map
        map_weight_clusters = self.create_weight_clusters(self.nodes, self.mapper)
        print("Starting map...")
        map_results = self.map(map_weight_clusters, data)
        print(f"Map finished with {len(map_results)} pairs")
        self.print_weight_clusters_stats("Map", map_weight_clusters)
        self.close_weight_clusters(map_weight_clusters)

        # reduce
        print("Starting reduce...")
        reduce_weight_clusters = self.create_weight_clusters(self.nodes,
                                                             generate_reduce_chunk_wrapper(self.reducer))
        reduce_results = self.reduce(reduce_weight_clusters, map_results)
        print(f"Reduce finished with {len(reduce_results)} pairs")
        self.print_weight_clusters_stats("Reduce", reduce_weight_clusters)
        self.close_weight_clusters(reduce_weight_clusters)

        return reduce_results

    def get_required_data_parts_count(self):
        return sum(node.cores * node.weight for node in self.nodes)

    def split_map_data_to_parts(self, data):
        parts = self.get_required_data_parts_count()
        return split(data, parts)

    @staticmethod
    def wait_for_flatten_jobs_results(jobs):
        results = []
        for job in jobs:
            result = job()
            if not result:
                print(job.exception)
                continue
            results.append(result)
        return list(itertools.chain.from_iterable(results))

    def distribute_map_data_over_clusters(self, map_weight_clusters, data):
        print("Start map data splitting...")
        data_parts = self.split_map_data_to_parts(data)
        print(f"Map data split to {len(data)} parts")

        map_jobs = []
        for weight_cluster in map_weight_clusters:
            rest_data_parts = data_parts
            for _ in range(weight_cluster.get_total_cores()):
                weight = weight_cluster.weight
                parts_to_submit, rest_data_parts = get_1st_n_parts_merged_and_rest(rest_data_parts, n=weight)
                job = weight_cluster.cluster.submit(parts_to_submit)
                map_jobs.append(job)
        return map_jobs

    def map(self, map_weight_clusters, data):
        map_jobs = self.distribute_map_data_over_clusters(map_weight_clusters, data)
        return self.wait_for_flatten_jobs_results(map_jobs)

    @staticmethod
    def partition_map_results_by_key(map_results) -> List[List[Pair]]:
        return group_to_2d_list(map_results, key=lambda result: hash(get_key(result)))

    def get_chunks_to_reduce_sorted_asc(self, map_results) -> List[ReduceChunk]:
        print("Start map result partitioning to reduce...")
        partitioned_map_results_sorted = group_to_2d_list(map_results, key=lambda result: hash(get_key(result)))
        partitioned_map_results_sorted.sort(key=len)

        total_reduce_parts = sublists_len(partitioned_map_results_sorted)
        required_chunks = self.get_required_data_parts_count()
        min_chunk_size_float = (1 / required_chunks) * total_reduce_parts
        min_chunk_size = int(min_chunk_size_float + 0.5)

        chunks = []  # will be roughly sorted desc because of partitioned_map_results_sorted
        current_chunk_start_index = 0
        current_chunk_end_index = 0
        current_chunk_len = 0
        parts_len = len(partitioned_map_results_sorted)
        while current_chunk_end_index != parts_len:
            current_chunk_len += len(partitioned_map_results_sorted[current_chunk_end_index])
            current_chunk_end_index += 1
            if current_chunk_len >= min_chunk_size:
                chunk_to_add = partitioned_map_results_sorted[current_chunk_start_index:current_chunk_end_index]
                chunks.append(flatten_list(chunk_to_add))
                current_chunk_start_index = current_chunk_end_index
                current_chunk_len = 0

        chunks.reverse()  # reverse sort to asc

        if current_chunk_start_index != parts_len:  # if last chunks wasn't added
            chunk_to_add = partitioned_map_results_sorted[current_chunk_start_index:]
            chunks.append(flatten_list(chunk_to_add))

        print(f"Partitioned to {len(chunks)} chunks")

        return chunks

    def distribute_reduce_parts_over_clusters(self, reduce_weight_clusters, map_results):
        reduce_chunks = self.get_chunks_to_reduce_sorted_asc(map_results)
        print(f"Map result are partitioned into {len(reduce_chunks)} chunks for reduce")

        weight_clusters_sorted_asc = sorted(reduce_weight_clusters, key=lambda wc: wc.weight, reverse=True)

        reduce_jobs = []
        weight_cluster_index = 0
        chunk_index = 0
        chunks_len = len(reduce_chunks)
        clusters_len = len(weight_clusters_sorted_asc)
        while chunk_index != chunks_len:
            weight_cluster = weight_clusters_sorted_asc[weight_cluster_index]
            parts_to_submit = min(weight_cluster.get_total_cores(), chunks_len - chunk_index)
            for _ in range(parts_to_submit):
                job = weight_cluster.cluster.submit(reduce_chunks[chunk_index])
                reduce_jobs.append(job)
                chunk_index += 1
            weight_cluster_index = (weight_cluster_index + 1) % clusters_len

        return reduce_jobs

    def reduce(self, reduce_weight_clusters, map_results):
        reduce_jobs = self.distribute_reduce_parts_over_clusters(reduce_weight_clusters, map_results)
        return self.wait_for_flatten_jobs_results(reduce_jobs)

    def create_weight_clusters(self, nodes, compute_callable) -> List[WeightCluster]:
        nodes_by_weight = group_to_dict(nodes, key=lambda node: node.weight)
        clusters = []
        reducer = self.reducer
        for weight, grouped_nodes in nodes_by_weight.items():
            nodes_hosts = [node.host for node in grouped_nodes]
            cluster = dispy.JobCluster(compute_callable, nodes=nodes_hosts, reentrant=True,
                                       depends=[reduce_part, group_by, group_to_2d_list, get_key, reducer])
            clusters.append(WeightCluster(cluster=cluster, weight=weight, nodes=grouped_nodes))
        return clusters
