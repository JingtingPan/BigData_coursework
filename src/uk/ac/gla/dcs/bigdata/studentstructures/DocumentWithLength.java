package uk.ac.gla.dcs.bigdata.studentstructures;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

import java.io.Serializable;

public class DocumentWithLength implements Serializable {
    NewsArticle newsArticle;

    int documentLength;

    public DocumentWithLength(){}
    public DocumentWithLength(NewsArticle newsArticle, int documentLength) {
        this.newsArticle = newsArticle;
        this.documentLength = documentLength;
    }

    public NewsArticle getNewsArticle() {
        return newsArticle;
    }

    public void setNewsArticle(NewsArticle newsArticle) {
        this.newsArticle = newsArticle;
    }

    public int getDocumentLength() {
        return documentLength;
    }

    public void setDocumentLength(int documentLength) {
        this.documentLength = documentLength;
    }
}
