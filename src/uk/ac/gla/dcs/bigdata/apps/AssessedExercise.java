package uk.ac.gla.dcs.bigdata.apps;

import java.io.File;
import java.util.ArrayList;
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
		System.out.println("number of articles after filtering: "+ numFilteredDocs);
		System.out.println("Total document length in corpus: " + totalDocLengthInCorpus );
		// collect the string into list
		List<NewsArticle> filteredNewsList = filteredNews.collectAsList();


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
				//to calculate the term frequency
				Dataset<Integer> docLengths = filteredNews.flatMap(docStatsCalculatorFlatMap, Encoders.INT());

				// execute count to activate action
				docLengths.count();

				//System.out.println(term);
				// get the value of accumulator
				termsFreq[termIndex] = termFrequencyAccumulator.value().intValue();
				System.out.println("Total term Frequency in corpus: " + termFrequencyAccumulator.value());
			}

			queryWithFreq.setTotalTermFreqInCorpus(termsFreq);
			//System.out.println(queryWithFreq.getTotalTermFreqInCorpus()[0]);
		}

		queryWithFrequency.show();
		double averageDocLengthInCorpus = (double) totalDocLengthInCorpus /numFilteredDocs;


		Dataset<DocumentWithLength> documentWithLength = filteredNews.map(new DocWithFreqFormaterMap(), Encoders.bean(DocumentWithLength.class));


		List<DocumentRanking> documentRankingsList = new ArrayList<>();
		//iterate through each query to get the first 10 document
		for(int queryIndex = 0; queryIndex < queryList.size(); queryIndex++){
			QueryWithFrequency queryWithFreq = queryList.get(queryIndex);
			int[] totaltermsfreq = queryWithFreq.getTotalTermFreqInCorpus();
			//convert a single query dph score list for all the docs to a rankedresult dataset
			DPHCalculatorFlatMap dphCalculatorFlatMap = new DPHCalculatorFlatMap(totaltermsfreq, averageDocLengthInCorpus, numFilteredDocs, queryWithFreq);

			Dataset<RankedResult> rankedResults = documentWithLength.flatMap(dphCalculatorFlatMap, Encoders.bean(RankedResult.class));
			//sorted rankedResult by the score order
			Dataset<RankedResult> sortedRankedResults = rankedResults.orderBy(rankedResults.col("score").desc());

			List<RankedResult> top30ResultList = sortedRankedResults.limit(30).collectAsList();
			//create a flatmap with top 30 rankedResult as input
			RedundancyFilterFlatMap redundancyFilterFlatMap = new RedundancyFilterFlatMap(top30ResultList);
			Dataset<RankedResult> filteredResults = sortedRankedResults.limit(10).flatMap(redundancyFilterFlatMap, Encoders.bean(RankedResult.class));
			sortedRankedResults.limit(10).show();
			filteredResults.show();

			List<RankedResult> resultList = filteredResults.sort().collectAsList();
			//create a DocumentRanking object with filtered and sorted RankedResult list
			DocumentRanking documentRanking = new DocumentRanking(queryWithFreq.getQuery(), resultList);

			documentRankingsList.add(documentRanking);

		}


		return documentRankingsList; // replace this with the list of DocumentRanking output by your topology
	}

	
}
