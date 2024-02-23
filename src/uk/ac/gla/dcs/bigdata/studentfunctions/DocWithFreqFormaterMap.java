package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentstructures.DocumentWithLength;

import java.util.ArrayList;
import java.util.List;

public class DocWithFreqFormaterMap implements MapFunction<NewsArticle, DocumentWithLength> {
    @Override
    public DocumentWithLength call(NewsArticle newsArticle) throws Exception {
        List<ContentItem> contents = newsArticle.getContents();
        int docLength = 0;
        for(ContentItem contentItem : contents){
            String[] words = contentItem.getContent().split("\\s+");
            docLength += words.length;
            }
        return new DocumentWithLength(newsArticle, docLength);
    }
}
