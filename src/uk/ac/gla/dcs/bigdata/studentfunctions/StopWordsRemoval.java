package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;

/*
steps:
    1: define a set of stop words
    2: tokenize the text into words
    3: filter out stop words, remove the stop words and retain the others
    4: perform stemming on the remaining words
 */

public class StopWordsRemoval {

    // manually define the stop word set
    static List<String> customStopWordList = Arrays.asList("a", "an", "the", "is", "in", "on", "was", "are", "whenever", "to", "like");

    public void main(String[] args) {
        ContentItem contentItem = new ContentItem();
        processContentItem(contentItem);
    }

    // method to process a single ContentItem
    public void processContentItem(ContentItem contentItem){
        // get the content from contentItem
        String text = contentItem.getContent();
        // process the text
        String filteredText = processText(text);
        // output the filtered text
        //System.out.println("initial text: " + text);
        System.out.println("text after filtered: " + filteredText);
    }

    // method to process text
    public static String processText(String text) {
        StringBuilder filteredTextBuilder = new StringBuilder();

        // split text into paragraphs
        String[] paragraphs = text.split("Filtered Text:\n");

        for (String paragraph : paragraphs) {
            // continue if the paragraph is not empty
            if (!paragraph.trim().isEmpty()) {
                // retain "article" and store in a separate line
                paragraph = paragraph.replaceAll("(?i)(article\\d+)", "\n$1");
                // remove "Filtered Text:"
                paragraph = paragraph.replaceAll("Filtered Text:", "");
                // remove HTML tags
                paragraph = paragraph.replaceAll("\\<.*?\\>", "");
                // 分词
                String[] words = paragraph.split("\\s+");
                // filtering and stemming
                List<String> filteredWords = filterAndStemWords(words);
                // add filtered words to filtered text
                filteredTextBuilder.append(String.join(" ", filteredWords)).append("\n");
            }
        }
        return filteredTextBuilder.toString().trim();
    }

    // filtering and stemming method
    public static List<String> filterAndStemWords(String[] words) {
        List<String> filteredWords = new ArrayList<>();
        boolean keepQuotes = true;
        for (String word : words) {
            // remove non-ascii character
            word = word.replaceAll("[^a-zA-Z]", "");
            // convert to lowercase
            word = word.toLowerCase();
            // deal with the problem with double quotation mark
            if (word.equals("\"") && !keepQuotes) {
                keepQuotes = true;
                continue;
            }
            if (word.equals("\"") && keepQuotes) {
                keepQuotes = false;
                continue;
            }
            if (!customStopWordList.contains(word)) {
                // stemming
                word = stemWord(word);
                filteredWords.add(word);
            }
        }
        return filteredWords;
    }


    public static String stemWord(String word) {
        if (word.endsWith("s")) {
            // deal with s
            word = word.substring(0, word.length() - 1);
        } else if (word.endsWith("es")) {
            // deal with es
            word = word.substring(0, word.length() - 2);
        } else if (word.endsWith("ing")) {
            // ing to singular
            word = word.substring(0, word.length() - 3);
        } else if (word.endsWith("ed")) {
            // deal with ed
            word = word.substring(0, word.length() - 2);
        } else if (word.endsWith("ly")) {
            // deal with ly
            word = word.substring(0, word.length() - 2);
        }
        return word;
    }

}
