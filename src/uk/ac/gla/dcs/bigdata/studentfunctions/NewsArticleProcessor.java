package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Dataset;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import java.util.List;

public class NewsArticleProcessor implements MapFunction<NewsArticle, NewsArticle> {

    @Override
    public NewsArticle call(NewsArticle article) throws Exception {
        TextPreProcessor processor = new TextPreProcessor();
        // Assume article.contents is a List<ContentItem> where each ContentItem represents a piece of the article
        article.getContents().forEach(contentItem -> {
            if ("paragraph".equals(contentItem.getSubtype())) {
                List<String> processedTerms = processor.process(contentItem.getContent());
                contentItem.setContent(String.join(" ", processedTerms)); // Update content with processed text
            }
        });
        return article;
    }

    // Method to apply this processing to a Dataset<NewsArticle>
    public static Dataset<NewsArticle> preprocessArticles(Dataset<NewsArticle> articles) {
        return articles.map(new NewsArticleProcessor(), Encoders.bean(NewsArticle.class));
    }
}

