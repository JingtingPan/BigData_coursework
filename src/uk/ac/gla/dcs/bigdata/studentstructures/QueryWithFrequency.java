package uk.ac.gla.dcs.bigdata.studentstructures;

import uk.ac.gla.dcs.bigdata.providedstructures.Query;

import java.io.Serializable;
import java.util.List;

public class QueryWithFrequency implements Serializable {

    Query query;

    int[] totalTermFreqInCorpus;

    public QueryWithFrequency() {
        // Empty constructor
    }

    public QueryWithFrequency(Query query){
        super();
        this.query = query;
    }

    public void setQuery(Query query) {
        this.query = query;
    }

    public void setTotalTermFreqInCorpus(int[] totalTermFreqInCorpus) {
        this.totalTermFreqInCorpus = totalTermFreqInCorpus;
    }

    public Query getQuery() {
        return query;
    }

    public int[] getTotalTermFreqInCorpus() {
        return totalTermFreqInCorpus;
    }
}
