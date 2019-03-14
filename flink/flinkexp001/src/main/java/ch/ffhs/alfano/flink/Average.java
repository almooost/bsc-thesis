package ch.ffhs.alfano.flink;

import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Average class for calculation
 * @author Samuel Alfano
 */
public class Average implements AggregateFunction<AmazonJsonObject, AverageAccumulator, AmazonJsonObject> {

    private AmazonJsonObject ajo;

    public AverageAccumulator createAccumulator() {
        return new AverageAccumulator();
    }

    public AverageAccumulator merge(AverageAccumulator a, AverageAccumulator b) {
        a.count += b.count;
        a.sum += b.sum;
        return a;
    }

    public AverageAccumulator add(AmazonJsonObject value, AverageAccumulator acc) {
        this.ajo = value;
        acc.sum += value.overall;
        acc.count++;
        return acc;
    }

    public AmazonJsonObject getResult(AverageAccumulator acc) {
        ajo.avg = acc.sum / (double) acc.count;
        return ajo;
    }
}
