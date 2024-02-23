package uk.ac.gla.dcs.bigdata.studentfunctions;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

import org.apache.spark.sql.Dataset;
import org.apache.spark.util.LongAccumulator;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;

public class DocumentStatisticsCalculatorFlatMap implements FlatMapFunction<NewsArticle,Integer>{



    LongAccumulator termFrequencyAccumulator;
    //private final Dataset<Query> queries;
    //private Query query;
    private String term;

    public DocumentStatisticsCalculatorFlatMap(LongAccumulator termFrequencyAccumulator, String term) {

        this.termFrequencyAccumulator = termFrequencyAccumulator;
        this.term = term;
    }




    @Override
    public Iterator<Integer>  call(NewsArticle newsArticle) throws Exception {


        List<Integer> lengths = new ArrayList<>();

        /*List<String> terms = query.getQueryTerms();
        for(String term : terms){

        }*/
        int termFrequencyInCurrentDocument = calculateTermFrequencyInCurrentDoc(newsArticle, term);
        lengths.add(termFrequencyInCurrentDocument);
        termFrequencyAccumulator.add(termFrequencyInCurrentDocument);


        return lengths.iterator();

    }

    private int calculateTermFrequencyInCurrentDoc(NewsArticle newsArticle, String term) {
        List<ContentItem> contentItems = newsArticle.getContents();
        int termFrequency = 0;
        String paragraph = "";
        for (ContentItem contentItem : contentItems) {
            paragraph = contentItem.getContent();
            String[] words = paragraph.split("\\W+");
            // 遍历单词数组，统计指定单词出现的次数
            for (String w : words) {
                if (w.equalsIgnoreCase(term)) {
                    termFrequency++;
                }
            }

        }

        return termFrequency;
    }
}
