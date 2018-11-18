# _RDD_ Examples

 

## Basic

| File                  | What's Illustrated    |
|-----------------------|-----------------------|
| Simple.scala    | create RDD, map operation, glom operation |
| Coputations.scala | checkpoint operation, show depencies of RDD |
| CombiningRDDs.scala | subtract , uion, intersection, cartesian, zip, zipPartitions |
| MoreOperations.scala | flatMap, groupBy, countByValue, max, min, first, sample, sortBy, take, takeSample |
| Partitions.scala | repartitions, coalesce, preferredLocations, |

## Pair RDD

| File                  | What's Illustrated    |
|-----------------------|-----------------------|
| PairRDD.scala | create Pair RDD, reduceByKey, aggregateByKey |
| OperationsOnPairRDD.scala | reduceByKey, aggregateByKey |

## Advanced

| File                  | What's Illustrated    |
|-----------------------|-----------------------|
| Accumulators.scala | User-defined Accumulators (StringSetAccumulator, CharCountingAccumulator) |
| HashJoin.scala | simply join/ join using a specific joiner |
| CustomPartitioner.scala | User-defined Partitioner (override numPartitions/ getPartition) |