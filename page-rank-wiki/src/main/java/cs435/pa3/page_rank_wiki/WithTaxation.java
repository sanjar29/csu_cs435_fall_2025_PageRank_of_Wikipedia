package cs435.pa3.page_rank_wiki;

import java.util.ArrayList;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;

/**
 * Class to calculate page ranks for all pages with taxation. Used for multiple iterations.
 */
public class WithTaxation {
    /**
     * Static method to calculate page ranks for all pages with taxation. Used for multiple iterations.
     * @param inData Original data formated from main containing initial page ranks and page data
     * @param inCount Total count of pages in dataset
     * @return scala.Tuple2 containing page Id and page data including page rank
     */
    public static JavaPairRDD<Long, scala.Tuple3<String, Double, String>> getPageRank(JavaPairRDD<Long, scala.Tuple3<String, Double, String>> inData, Double inCount){
        // Types declared here for readability
        JavaPairRDD<Long, Double> calculatePageRank;
        ArrayList<scala.Tuple2<Long, Double>> adjacentPageRank = new ArrayList<>();
        JavaPairRDD<Long, Double> adjacentPageRanks;
        JavaPairRDD<Long, Double> taxAdjacentPageRanks;
        JavaPairRDD<Long, scala.Tuple2<Double, scala.Tuple3<String, Double, String>>> updatedPageRank;
        JavaPairRDD<Long, scala.Tuple3<String, Double, String>> returnPageRank;

        JavaPairRDD<Long, scala.Tuple3<String, Double, String>> data = inData;
        Double count = inCount;
        // Propagates page ranking to adjacent pages
        calculatePageRank = data.flatMapToPair(link -> {
            String adjacentPages = link._2._3().trim();
            String[] pagesArray = adjacentPages.split("\\s+");
            int numberOfPages = pagesArray.length;
            Double pageRank = link._2._2();
            for (int i = 0; i < numberOfPages; i++) {
                // First tuple value is destination page ID and second is updated page rank of current page
                adjacentPageRank.add(new scala.Tuple2<>(Long.valueOf(pagesArray[i]), pageRank * (1D/Double.valueOf(numberOfPages))));
            }
            return adjacentPageRank.iterator();
        });
        adjacentPageRanks = calculatePageRank.reduceByKey((value1, value2) -> value1 + value2);
        // Calculates page rank with taxation
        taxAdjacentPageRanks = adjacentPageRanks.mapToPair(page -> {
            Double taxPageRank = (0.85 * page._2())+((1-0.85)/count);
            return new scala.Tuple2(page._1(), taxPageRank);
        });
        updatedPageRank = taxAdjacentPageRanks.join(data);
        // Final map to return new page rank data for all pages
        returnPageRank = updatedPageRank.mapToPair(page -> {
            Double newPageRank = page._2._1();
            return new scala.Tuple2<>(page._1(), new scala.Tuple3<>(page._2._2._1(), newPageRank,page._2._2._3()));
        });
        return returnPageRank;
    }
}