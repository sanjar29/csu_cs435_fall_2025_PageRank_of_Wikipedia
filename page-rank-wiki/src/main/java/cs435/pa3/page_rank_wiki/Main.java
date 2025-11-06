package cs435.pa3.page_rank_wiki;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;

public class Main {
    public static void main(String[] args) {
        String titlePath = args[0];
        String linkPath = args[1];
        String outpath = args[2];
        SparkSession spark = SparkSession
                .builder()
                .appName("Page Rank Wiki")
                .master("local[*]")
                .getOrCreate();
        if(!titlePath.isEmpty() && !linkPath.isEmpty()) {
            Dataset<Row> titleData = spark.read().text(titlePath);
            Dataset<Row> linkData = spark.read().text(linkPath);
        }
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        JavaRDD<String> titleLines = sc.textFile(titlePath);
        JavaRDD<scala.Tuple2<Long, String>> linkLines = sc.textFile(linkPath).map(
                link -> {
                    String[] linkLine = link.split(":");
                    return new scala.Tuple2<>(Long.parseLong(linkLine[0]), linkLine[1]);
                }
        );
        Double count = Double.valueOf(titleLines.count());
        Double pageRank = 1 / count;
        JavaPairRDD<Long, String> titleData = titleLines.zipWithIndex().mapToPair(title -> new scala.Tuple2<>(title._2 + 1,title._1));
        JavaPairRDD<Long, String> linkData = linkLines.mapToPair(link -> new scala.Tuple2<>(link._1, link._2));
        JavaPairRDD<Long, scala.Tuple2<String, String>> data = titleData.join(linkData);
        JavaPairRDD<Long, scala.Tuple3<String, Double, String>> dataWithPageRank = data.mapToPair(link -> {
            return new scala.Tuple2<>(link._1(), new scala.Tuple3<>(link._2._1(), pageRank, link._2._2()));
        });
        JavaPairRDD<Long, scala.Tuple3<String, Double, String>> withoutTaxPageRank = WithoutTaxation.getPageRank(dataWithPageRank, count);
        for(int i = 0; i < 10; i++){
            withoutTaxPageRank = WithoutTaxation.getPageRank(withoutTaxPageRank, count);
            JavaPairRDD<String, Double> formattedOutput = withoutTaxPageRank.mapToPair(page -> {
                String article = page._2._1();
                Double finalPageRank = page._2._2();
                return new scala.Tuple2(article, finalPageRank);
            });
            JavaPairRDD<String, Double> sortedFormattedOutput = formattedOutput.sortByKey(false);
            Integer iteration = i;
            sortedFormattedOutput.saveAsTextFile(outpath + "/WithoutTaxation" + iteration.toString());
        }
        JavaPairRDD<Long, scala.Tuple3<String, Double, String>> taxPageRank = WithTaxation.getPageRank(dataWithPageRank, count);
        for(int i = 0; i < 10; i++){
            taxPageRank = WithTaxation.getPageRank(taxPageRank, count);
            JavaPairRDD<String, Double> formattedTaxOutput = taxPageRank.mapToPair(page -> {
                String article = page._2._1();
                Double finalPageRank = page._2._2();
                return new scala.Tuple2(article, finalPageRank);
            });
            JavaPairRDD<String, Double> sortedFormattedTaxOutput = formattedTaxOutput.sortByKey(false);
            Integer iteration = i;
            sortedFormattedTaxOutput.saveAsTextFile(outpath + "/WithTaxation" + iteration.toString());
        }
        sc.stop();
    }
}
