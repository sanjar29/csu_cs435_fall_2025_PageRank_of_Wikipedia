package cs435.pa3.page_rank_wiki;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class Main {
    public static void main(String[] args) {
        String titlePath = args[0];
        String linkPath = args[1];
        SparkSession spark = SparkSession
                .builder()
                .appName("Page Rank Wiki")
                .master("local[*]")
                .getOrCreate();
        if(!titlePath.isEmpty() && !linkPath.isEmpty()) {
            Dataset<Row> titleData = spark.read().text(titlePath);
            titleData.show();
            Dataset<Row> linkData = spark.read().text(linkPath);
            linkData.show();
        }
    }
}
