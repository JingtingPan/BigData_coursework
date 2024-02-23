package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

import org.apache.spark.util.LongAccumulator;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;

/**
 * Filter out news document with only "id", "title", "content" left.
 */
public class NewsFilterFlatMap implements FlatMapFunction<NewsArticle,NewsArticle>{

    LongAccumulator docLengthAccumulator;

    public NewsFilterFlatMap(LongAccumulator docLengthAccumulator) {
        this.docLengthAccumulator = docLengthAccumulator;
    }

    @Override
    public Iterator<NewsArticle> call(NewsArticle newsArticle) throws Exception {



         String id = newsArticle.getId();
         String title = newsArticle.getTitle();
         List<ContentItem> contents = newsArticle.getContents();
         TextPreProcessor processor = new TextPreProcessor();
         List<String> contentList = new ArrayList<>();
         String processedContent = "";
         int docLength;

         // to count the number of paragraphs
         int paragraphCounter = 0;
        // check if id or title field is null
         if(id != null && title != null){
                 if(contents != null && !contents.isEmpty()){
                     List<ContentItem> newContents = new ArrayList<>();
                     docLength = 0;
                     for(ContentItem contentItem : contents){
                         //check if subtype is null and add to the new item list is subtype is paragraph
                        if(contentItem != null && contentItem.getSubtype() != null && contentItem.getSubtype().equals("paragraph")){

                            paragraphCounter++;
                            if(paragraphCounter <= 5){
                                // text pre process
                                contentList = processor.process(contentItem.getContent());


                                if (contentList != null) {
                                    processedContent = String.join(" ", contentList);
                                    contentItem.setContent(processedContent);
                                    newContents.add(contentItem);
                                    //calculate document length
                                    docLength += contentList.size();
                                }else{
                                    //calculate document length
                                    processedContent = "";
                                    contentItem.setContent(processedContent);
                                    newContents.add(contentItem);
                                    docLength += 0;
                                }
                            }

                        }
                     }

                     docLengthAccumulator.add(docLength);

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
