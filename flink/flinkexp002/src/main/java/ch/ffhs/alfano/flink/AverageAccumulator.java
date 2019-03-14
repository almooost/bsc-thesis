package ch.ffhs.alfano.flink;

/**
 * AverageAccumulator
 * @author Samuel Alfano
 * The accumulator, which holds the state of the in-flight aggregate
 */
public class AverageAccumulator {
    long count;
    long sum;
}