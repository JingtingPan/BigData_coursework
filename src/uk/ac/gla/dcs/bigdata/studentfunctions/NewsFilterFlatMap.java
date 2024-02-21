package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
/**
 * Filter out news document with only "id", "title", "content" left.
 */
public class NewsFilterFlatMap implements FlatMapFunction<NewsArticle,NewsArticle>{


    @Override
    public Iterator<NewsArticle> call(NewsArticle newsArticle) throws Exception {

         String id = newsArticle.getId();
         String title = newsArticle.getTitle();
         List<ContentItem> contents = newsArticle.getContents();

         // to count the number of paragraphs
         int paragraphCounter = 0;
        // check if id or title field is null
         if(id != null && title != null){
                 if(contents != null && !contents.isEmpty()){
                     List<ContentItem> newContents = new ArrayList<>();
                     for(ContentItem contentItem : contents){
                         //check if subtype is null and add to the new item list is subtype is paragraph
                        if(contentItem.getSubtype() != null && contentItem.getSubtype().equals("paragraph")){
                            paragraphCounter++;
                            if(paragraphCounter <= 5)
                                newContents.add(contentItem);
                            // stop word removal

                        }
                     }
                     if(paragraphCounter == 0){
                         // return empty array if fails the check
                         List<NewsArticle> articlesList = new ArrayList<NewsArticle>(0);
                         return articlesList.iterator();
                     }

                     // update the contents field with the filtered list
                     newsArticle.setContents(newContents);
                     // return the newsArticle with updated contents
                     List<NewsArticle> articlesList = new ArrayList<NewsArticle>(1);
                     articlesList.add(newsArticle);
                     return articlesList.iterator();

             }
         }
        // return empty array if fails the check
        List<NewsArticle> articlesList = new ArrayList<NewsArticle>(0);
        return articlesList.iterator();
    }
}
