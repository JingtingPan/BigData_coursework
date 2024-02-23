package uk.ac.gla.dcs.bigdata.apps;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.util.LongAccumulator;
import scala.Int;
import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;

import uk.ac.gla.dcs.bigdata.providedstructures.*;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentfunctions.*;

import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentstructures.DocumentWithLength;
import uk.ac.gla.dcs.bigdata.studentstructures.QueryWithFrequency;


/**
 * This is the main class where your Spark topology should be specified.
 * 
 * By default, running this class will execute the topology defined in the
 * rankDocuments() method in local mode, although this may be overriden by
 * the spark.master environment variable.
 * @author Richard
 *
 */
public class AssessedExercise {

	
	public static void main(String[] args) {
		
		File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can get an absolute path for it
		System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark finds it
		
		// The code submitted for the assessed exerise may be run in either local or remote modes
		// Configuration of this will be performed based on an environment variable
		String sparkMasterDef = System.getenv("spark.master");
		if (sparkMasterDef==null) sparkMasterDef = "local[2]"; // default is local mode with two executors
		
		String sparkSessionName = "BigDataAE"; // give the session a name
		
		// Create the Spark Configuration 
		SparkConf conf = new SparkConf()
				.setMaster(sparkMasterDef)
				.setAppName(sparkSessionName);
		
		// Create the spark session
		SparkSession spark = SparkSession
				  .builder()
				  .config(conf)
				  .getOrCreate();
	
		
		// Get the location of the input queries
		String queryFile = System.getenv("bigdata.queries");
		if (queryFile==null) queryFile = "data/queries.list"; // default is a sample with 3 queries
		
		// Get the location of the input news articles
		String newsFile = System.getenv("bigdata.news");
		if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v3.example.json"; // default is a sample of 5000 news articles
		
		// Call the student's code
		//StopWordsRemoval swr = new StopWordsRemoval();
		List<DocumentRanking> results = rankDocuments(spark, queryFile, newsFile);
		
		// Close the spark session
		spark.close();
		
		// Check if the code returned any results
		if (results==null) System.err.println("Topology return no rankings, student code may not be implemented, skiping final write.");
		else {
			
			// We have set of output rankings, lets write to disk
			
			// Create a new folder 
			File outDirectory = new File("results/"+System.currentTimeMillis());
			if (!outDirectory.exists()) outDirectory.mkdir();
			
			// Write the ranking for each query as a new file
			for (DocumentRanking rankingForQuery : results) {
				rankingForQuery.write(outDirectory.getAbsolutePath());
			}
		}
		

	}

	
	public static List<DocumentRanking> rankDocuments(SparkSession spark, String queryFile, String newsFile) {

		// Load queries and news articles
		Dataset<Row> queriesjson = spark.read().text(queryFile);
		Dataset<Row> newsjson = spark.read().text(newsFile); // read in files as string rows, one row per article

		// Perform an initial conversion from Dataset<Row> to Query and NewsArticle Java objects
		Dataset<Query> queries = queriesjson.map(new QueryFormaterMap(), Encoders.bean(Query.class)); // this converts each row into a Query
		Dataset<NewsArticle> news = newsjson.map(new NewsFormaterMap(), Encoders.bean(NewsArticle.class)); // this converts each row into a NewsArticle

		//----------------------------------------------------------------
		// Your Spark Topology should be defined here
		//----------------------------------------------------------------

		//news filter
		// counting the number of the total articles
		long numDocs = news.count();
		System.out.println("number of news: "+ numDocs); // 5000


		//// initialise the accumulator for total document length
		LongAccumulator docLengthAccumulator = spark.sparkContext().longAccumulator();
		// initialise the NewsFilterFlatMap object
		NewsFilterFlatMap newsFilterFlatMap = new NewsFilterFlatMap(docLengthAccumulator);

		// filters the new articles
		Dataset<NewsArticle> filteredNews = news.flatMap(newsFilterFlatMap, Encoders.bean(NewsArticle.class));
		// count the number of articles after filtering
		long numFilteredDocs = filteredNews.count();
		long totalDocLengthInCorpus = docLengthAccumulator.value();
		System.out.println("number of articles after filtering: "+ numFilteredDocs); // 4798
		System.out.println("Total term Frequency in corpus: " + totalDocLengthInCorpus );
		// collect the string into list
		List<NewsArticle> filteredNewsList = filteredNews.collectAsList();

		// display the first few rows of the queries dataset
		queries.show();

		// iterate the first 5 filtered articles and print the content
		for (int articleIndex = 0; articleIndex < 2; articleIndex++) {
			System.out.println("article" + articleIndex);

			NewsArticle article = filteredNewsList.get(articleIndex);

			List<ContentItem> contentItems = article.getContents();

			for (ContentItem contentItem : contentItems) {
				// Print filtered text
				//System.out.println("Filtered Text:");
				System.out.println(contentItem.getContent());

			}
		}

		Dataset<QueryWithFrequency> queryWithFrequency = queries.map(new QueryWithFrequencyFormaterMap(), Encoders.bean(QueryWithFrequency.class));


		List<QueryWithFrequency> queryList = queryWithFrequency.collectAsList();
		int termCounts = 0;
		//iterate the queries

		for(int queryIndex = 0; queryIndex < queryList.size(); queryIndex++){
			//iterate the terms in a query
			QueryWithFrequency queryWithFreq = queryList.get(queryIndex);
			Query query = queryWithFreq.getQuery();
			List<String> terms = query.getQueryTerms();
			int[] termsFreq = new int[terms.size()];
			for(int termIndex = 0; termIndex < terms.size();termIndex++) {
				String term = terms.get(termIndex);

				LongAccumulator termFrequencyAccumulator = spark.sparkContext().longAccumulator();
				DocumentStatisticsCalculatorFlatMap docStatsCalculatorFlatMap = new DocumentStatisticsCalculatorFlatMap(termFrequencyAccumulator, term);
				// 在 flatMap 中应用 DocumentLengthCalculator，以计算每个文档的长度并累加到累加器中
				Dataset<Integer> docLengths = filteredNews.flatMap(docStatsCalculatorFlatMap, Encoders.INT());

				// 执行操作以触发计算
				docLengths.count();

				//System.out.println(term);
				// 在关闭 SparkSession 前获取累加器的值
				termsFreq[termIndex] = termFrequencyAccumulator.value().intValue();
				System.out.println("Total term Frequency in corpus: " + termFrequencyAccumulator.value());
			}

			queryWithFreq.setTotalTermFreqInCorpus(termsFreq);
			//System.out.println(queryWithFreq.getTotalTermFreqInCorpus()[0]);
		}

		double averageDocLengthInCorpus = (double) totalDocLengthInCorpus /numFilteredDocs;


		Dataset<DocumentWithLength> documentWithFrequency = filteredNews.map(new DocWithFreqFormaterMap(), Encoders.bean(DocumentWithLength.class));

		filteredNews.show();
		documentWithFrequency.show();

		for(int queryIndex = 0; queryIndex < queryList.size(); queryIndex++){
			QueryWithFrequency queryWithFreq = queryList.get(queryIndex);
			int[] totaltermsfreq = queryWithFreq.getTotalTermFreqInCorpus();
			//convert a single query dph score list for all the docs to a rankedresult dataset
			DPHCalculatorFlatMap dphCalculatorFlatMap = new DPHCalculatorFlatMap(totaltermsfreq, averageDocLengthInCorpus, numFilteredDocs, queryWithFreq);
			Dataset<RankedResult> rankedResults = documentWithFrequency.flatMap(dphCalculatorFlatMap, Encoders.bean(RankedResult.class));


		}






		return null; // replace this with the list of DocumentRanking output by your topology
	}

	
}
