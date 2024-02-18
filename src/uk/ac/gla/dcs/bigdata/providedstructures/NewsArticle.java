package uk.ac.gla.dcs.bigdata.providedstructures;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;

/**
 * Represents a single news article from the source Washington Post Corpus
 * @author Richard
 *
 */
public class NewsArticle implements Serializable {

	private static final long serialVersionUID = 7860293794078412243L;

	String id; // unique article identifier
	String article_url; // url pointing to the online article
	String title; // article title
	String author; // article author
	long published_date; // publication date as a unix timestamp (ms)
	List<ContentItem> contents; // the contents of the article body
	String type; // type of the article
	String source; // news provider

	// New field to store processed contents
	private List<String> processedContents = new ArrayList<>();

	// Existing constructors, getters, and setters remain unchanged

	public List<String> getProcessedContents() {
		return processedContents;
	}

	public void setProcessedContents(List<String> processedContents) {
		this.processedContents = processedContents;
	}

	/**
	 * Processes the title and the first five paragraphs of the article,
	 * removes stopwords, applies stemming, and stores the result.
	 * @param processor The text processor to use for content processing.
	 */
	public void processContents(TextPreProcessor processor) {
		if (this.contents == null) {
			this.processedContents = new ArrayList<>();
			return;
		}

		List<String> allProcessedText = new ArrayList<>();
		// Process title
		allProcessedText.addAll(processor.process(this.title));

		// Process the first 5 paragraphs
		int paragraphsProcessed = 0;
		for (ContentItem item : this.contents) {
			if ("paragraph".equals(item.getSubtype()) && paragraphsProcessed < 5) {
				allProcessedText.addAll(processor.process(item.getContent()));
				paragraphsProcessed++;
			}
		}

		this.processedContents = allProcessedText;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getArticle_url() {
		return article_url;
	}

	public void setArticle_url(String article_url) {
		this.article_url = article_url;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getAuthor() {
		return author;
	}

	public void setAuthor(String author) {
		this.author = author;
	}

	public long getPublished_date() {
		return published_date;
	}

	public void setPublished_date(long published_date) {
		this.published_date = published_date;
	}

	public List<ContentItem> getContents() {
		return contents;
	}

	public void setContents(List<ContentItem> contents) {
		this.contents = contents;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}
	
	
}
