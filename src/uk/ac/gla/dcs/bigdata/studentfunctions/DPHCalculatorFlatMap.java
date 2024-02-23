package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.studentstructures.DocumentWithLength;
import uk.ac.gla.dcs.bigdata.studentstructures.QueryWithFrequency;

import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

public class DPHCalculatorFlatMap implements FlatMapFunction<DocumentWithLength, RankedResult> {

    int[] totalTermsFreqInCorpus;

    double averageDocLengthInCorpus;

    long totalDocsInCorpus;

    QueryWithFrequency queryWithFrequency;

    public DPHCalculatorFlatMap() {}

    public DPHCalculatorFlatMap(int[] totalTermsFreqInCorpus, double averageDocLengthInCorpus, long totalDocsInCorpus, QueryWithFrequency queryWithFrequency) {
        this.totalTermsFreqInCorpus = totalTermsFreqInCorpus;
        this.averageDocLengthInCorpus = averageDocLengthInCorpus;
        this.totalDocsInCorpus = totalDocsInCorpus;
        this.queryWithFrequency = queryWithFrequency;
    }

    @Override
    public Iterator<RankedResult> call(DocumentWithLength documentWithFrequency) throws Exception {
        //instantiate DPH scorer
        DPHScorer dphScorer = new DPHScorer();

        double totalDphScore;

        NewsArticle newsArticle = documentWithFrequency.getNewsArticle();

        List<RankedResult> rankedResults = new ArrayList<>();

        Query query = queryWithFrequency.getQuery();
        List<String> terms = query.getQueryTerms();
        totalDphScore = 0;
        for(int termIndex = 0; termIndex < terms.size();termIndex++) {

            String term = terms.get(termIndex);
            short termFreqInCurrentDoc = termFreqInCurrentDocCalculator(newsArticle, term);
            //calculate dph score for the term
            int totalTermFreqInCorpus = totalTermsFreqInCorpus[termIndex];
            totalDphScore += DPHScorer.getDPHScore(termFreqInCurrentDoc, totalTermFreqInCorpus, documentWithFrequency.getDocumentLength(), averageDocLengthInCorpus, totalDocsInCorpus);
        }

        double averageDphScore = totalDphScore/terms.size();
        if(Double.isNaN(averageDphScore)){
            averageDphScore = 0;
        }

        RankedResult rankedResult = new RankedResult(newsArticle.getId(), newsArticle, averageDphScore);
        rankedResults.add(rankedResult);

        return rankedResults.iterator();
    }

    // return the specific term frequency in the current document
    private short termFreqInCurrentDocCalculator(NewsArticle newsArticle, String term){
        List<ContentItem> contentItems = newsArticle.getContents();
        short termFrequency = 0;
        String paragraph = "";
        for (ContentItem contentItem : contentItems) {
            paragraph = contentItem.getContent();
            String[] words = paragraph.split("\\W+");

            for (String w : words) {
                if (w.equalsIgnoreCase(term)) {
                    termFrequency++;
                }
            }

        }

        return termFrequency;
    }
}
