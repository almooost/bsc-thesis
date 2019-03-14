package ch.ffhs.alfano.flink;

/**
 * AmazonJsonObject for Amazon Review JSON Data
 * @author Samuel Alfano
 */
public  class AmazonJsonObject {

    public String reviewerID;
    public String asin;
    public Long unixReviewTime;
    public Double overall;
    public Double avg;

    public AmazonJsonObject() {}

    public AmazonJsonObject(String reviewerID, String asin, Long unixReviewTime,Double overall) {
        this.reviewerID = reviewerID;
        this.asin = asin;
        this.unixReviewTime = unixReviewTime;
        this.overall = overall;
    }

    @Override
    public String toString() {
        return "reviewerID: " + reviewerID + ", asin: " + asin + ", unixReviewTime: " + unixReviewTime.toString() +
                ", overall: " +overall.toString() + ", avg: " + avg.toString();
    }

    /**
     * Needs to be implemented for keyBy("field") to work!
     * @return
     */
    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + unixReviewTime.intValue();
        result = 31 * result + reviewerID.hashCode();
        return result;
    }
}
