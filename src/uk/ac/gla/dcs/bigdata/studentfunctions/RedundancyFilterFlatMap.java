package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;
import uk.ac.gla.dcs.bigdata.studentstructures.DocumentWithLength;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class RedundancyFilterFlatMap implements FlatMapFunction<RankedResult, RankedResult> {

    private transient TextDistanceCalculator textDistanceCalculator;

    private List<RankedResult> rankedResultList;

    private final int maxOutputSize = 10;
    private int counter = 0;

    //track how many replace has been done(replace start with 11th)
    private int replaceCounter = 10;

    //private final List<RankedResult> output = new ArrayList<>();

    public RedundancyFilterFlatMap(){}

    public RedundancyFilterFlatMap(List<RankedResult> rankedResultList) {
        this.rankedResultList = rankedResultList;
    }

    @Override
    public Iterator<RankedResult> call(RankedResult currentResult) throws Exception {


        for (int resultIndex = 0; resultIndex < rankedResultList.size(); resultIndex++){
                RankedResult comparedResult = rankedResultList.get(resultIndex);
                if(textDistanceCalculator == null) textDistanceCalculator = new TextDistanceCalculator();
                //skip the same doc while looping
                if (!comparedResult.getDocid().equals(currentResult.getDocid())) {
                    if(textDistanceCalculator.similarity(comparedResult.getArticle().getTitle(), currentResult.getArticle().getTitle()) < 0.5){
                        // if there is similar article and the current article have a higher DPH score, keep the current one
                        if(currentResult.getScore() >= comparedResult.getScore()){
                            List<RankedResult> output = new ArrayList<>(1);
                            output.add(currentResult);
                            return output.iterator();

                        }else{
                            List<RankedResult> output = new ArrayList<>(1);
                            RankedResult replaceResult = rankedResultList.get(replaceCounter);
                            //check if the replaceResult is similar to any of the document in top 10 list
                            for(int replaceResultIndex = 0; replaceResultIndex < 10; replaceResultIndex++){
                               comparedResult = rankedResultList.get(replaceResultIndex);
                                if(textDistanceCalculator == null) textDistanceCalculator = new TextDistanceCalculator();
                                    if(textDistanceCalculator.similarity(comparedResult.getArticle().getTitle(), replaceResult.getArticle().getTitle()) < 0.5){
                                        replaceCounter ++;
                                        replaceResult = rankedResultList.get(replaceCounter);
                                        replaceResultIndex = 0;


                                    }else{
                                        replaceCounter++;
                                        output.add(replaceResult);
                                        return output.iterator();
                                    }
                            }

                        }
                    }else{
                        //if no similar article just add the current one
                        List<RankedResult> output = new ArrayList<>(1);
                        output.add(currentResult);
                        counter ++;
                        return output.iterator();
                    }

                }


            }
        List<RankedResult> output = new ArrayList<>(0);
        counter ++;
        return output.iterator();

    }
}
