package uk.ac.gla.dcs.bigdata.apps;

import java.io.File;
import java.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.*;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;

public class AssessedExercise {

	public static void main(String[] args) {

		File hadoopDIR = new File("resources/hadoop/");
		System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath());

		SparkConf conf = new SparkConf().setAppName("BigDataAE").setMaster("local[*]");
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		String queryFile = "data/queries.list";
		String newsFile = "data/TREC_Washington_Post_collection.v3.example.json";

		List<DocumentRanking> results = rankDocuments(spark, queryFile, newsFile);

		spark.close();

		if (results != null) {
			File outDirectory = new File("results/" + System.currentTimeMillis());
			outDirectory.mkdirs();
			results.forEach(ranking -> ranking.write(outDirectory.getAbsolutePath()));
		}
	}

	public static List<DocumentRanking> rankDocuments(SparkSession spark, String queryFile, String newsFile) {
		Dataset<Query> queries = spark.read().text(queryFile).map(new QueryFormaterMap(), Encoders.bean(Query.class));
		Dataset<NewsArticle> news = spark.read().text(newsFile).map(new NewsFormaterMap(), Encoders.bean(NewsArticle.class));

		// Calculate average document length and total term frequencies
		// Correctly broadcasting values without Encoders
		Broadcast<Double> avgDocLength = spark.sparkContext().broadcast(calculateAverageDocumentLength(news), scala.reflect.ClassTag$.MODULE$.apply(Double.class));
		Broadcast<Map<String, Long>> totalTermFreqs = spark.sparkContext().broadcast(calculateTotalTermFrequencies(news), scala.reflect.ClassTag$.MODULE$.apply(Map.class));

		JavaPairRDD<Query, NewsArticle> queryArticlePairs = queries.toJavaRDD().cartesian(news.toJavaRDD());

		JavaRDD<DocumentRanking> documentRankings = queryArticlePairs.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Query, NewsArticle>>, Query, RankedResult>() {
			@Override
			public Iterator<Tuple2<Query, RankedResult>> call(Iterator<Tuple2<Query, NewsArticle>> input) {
				List<Tuple2<Query, RankedResult>> results = new ArrayList<>();
				while (input.hasNext()) {
					Tuple2<Query, NewsArticle> pair = input.next();
					Query query = pair._1();
					NewsArticle article = pair._2();
					double score = 0.0;
					for (String term : query.getQueryTerms()) {
						long tf = article.getProcessedContents().stream().filter(t -> t.equals(term)).count();
						long df = totalTermFreqs.getValue().getOrDefault(term, 0L);
						// Correcting the method call to match the expected parameter types.
						double dphScore = DPHScorer.getDPHScore(
								(short) tf,
								(int) df,  // Cast total term frequency (df) to int as required
								article.getProcessedContents().size(),
								avgDocLength.getValue(),
								totalTermFreqs.getValue().size() // Use the map size for the total number of unique terms, which might not be correct. Adjust as needed.
						);
						score += dphScore;
					}
					score /= query.getQueryTerms().size();
					results.add(new Tuple2<>(query, new RankedResult(article.getId(), article, score)));
				}
				return results.iterator();
			}
		}).groupByKey().mapToPair(tuple -> {
			Query query = tuple._1();
			List<RankedResult> rankedResults = new ArrayList<>();
			tuple._2().forEach(rankedResults::add);
			rankedResults.sort(Comparator.comparingDouble(RankedResult::getScore).reversed());
			return new Tuple2<>(query, rankedResults.subList(0, Math.min(10, rankedResults.size())));
		}).flatMap(tuple -> {
			List<DocumentRanking> rankings = new ArrayList<>();
			Query query = tuple._1();
			List<RankedResult> rankedResults = tuple._2();
			rankings.add(new DocumentRanking(query, rankedResults));
			return rankings.iterator();
		});

		return documentRankings.collect();
	}

	private static double calculateAverageDocumentLength(Dataset<NewsArticle> news) {
		JavaRDD<NewsArticle> newsRDD = news.toJavaRDD();
		double totalLength = newsRDD.mapToDouble(article -> article.getProcessedContents().size()).sum();
		long docCount = newsRDD.count();
		return totalLength / docCount;
	}

	private static Map<String, Long> calculateTotalTermFrequencies(Dataset<NewsArticle> news) {
		JavaRDD<NewsArticle> newsRDD = news.toJavaRDD();
		return newsRDD.flatMap(article -> Arrays.asList(article.getProcessedContents().toArray(new String[0])).iterator())
				.mapToPair(term -> new Tuple2<>(term, 1L))
				.reduceByKey(Long::sum)
				.collectAsMap();
	}
}
