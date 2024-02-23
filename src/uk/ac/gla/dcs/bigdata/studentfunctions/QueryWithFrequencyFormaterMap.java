package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import scala.Int;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentstructures.QueryWithFrequency;

import java.sql.Array;
import java.util.ArrayList;
import java.util.List;

public class QueryWithFrequencyFormaterMap implements MapFunction<Query, QueryWithFrequency> {

    @Override
    public QueryWithFrequency call(Query query) throws Exception {

        return new QueryWithFrequency(query);
    }
}
