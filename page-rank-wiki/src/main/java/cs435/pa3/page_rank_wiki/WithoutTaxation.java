package cs435.pa3.page_rank_wiki;

import java.util.ArrayList;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;

/**
 * Class to calculate page ranks for all pages without taxation. Used for multiple iterations.
 * @implNote Similar to WithTaxation but without additional calculation of taxation
 */
public class WithoutTaxation {
    /**
     * Static method to calculate page ranks for all pages without taxation. Used for multiple iterations.
     * @param inData Original data formated from main containing initial page ranks and page data
     * @param inCount Total count of pages in dataset
     * @return scala.Tuple2 containing page Id and page data including page rank
     */
    public static JavaPairRDD<Long, scala.Tuple3<String, Double, String>> getPageRank(JavaPairRDD<Long, scala.Tuple3<String, Double, String>> inData, Double inCount) {
        JavaPairRDD<Long, scala.Tuple3<String, Double, String>> data = inData;
        Double count = inCount;
        // Propagates page ranking to adjacent pages
        JavaPairRDD<Long, Double> calculatePageRank = data.flatMapToPair(link -> {
            String adjacentPages = link._2._3().trim();
            String[] pagesArray = adjacentPages.split("\\s+");
            int numberOfPages = pagesArray.length;
            Double pageRank = link._2._2();
            ArrayList<scala.Tuple2<Long, Double>> adjacentPageRank = new ArrayList<>();
            for (int i = 0; i < numberOfPages; i++) {
                // First tuple value is destination page ID and second is updated page rank of current page
                adjacentPageRank.add(new scala.Tuple2(Long.valueOf(pagesArray[i]), pageRank * (1D/Double.valueOf(numberOfPages))));
            }
            return adjacentPageRank.iterator();
        });
        JavaPairRDD<Long, Double> adjacentPageRanks = calculatePageRank.reduceByKey((value1, value2) -> value1 + value2);
        JavaPairRDD<Long, scala.Tuple2<Double, scala.Tuple3<String, Double, String>>> updatedPageRank = adjacentPageRanks.join(data);
        // Final map to return new page rank data for all pages
        JavaPairRDD<Long, scala.Tuple3<String, Double, String>> returnPageRank = updatedPageRank.mapToPair(page -> {
            Double newPageRank = page._2._1();
            return new scala.Tuple2<>(page._1(), new scala.Tuple3<>(page._2._2._1(), newPageRank,page._2._2._3()));
        });
        return returnPageRank;
    }
}