package cs435.pa3.page_rank_wiki;

import java.io.Serializable;
// import java.util.ArrayList;
// import java.util.Arrays;
// import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
// import java.util.concurrent.ExecutionException;
// import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
// import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
// import org.apache.spark.sql.Dataset;
// import org.apache.spark.sql.Row;
// import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class PageRank implements Serializable {

    public static void main(String[] args) {

        if (args.length < 2) {
            System.err.println("Usage: page-rank-wiki <links-filename> <titles-filename>");
            System.exit(1);
        }

        PageRank pageRank = new PageRank(args[0], args[1]);
        pageRank.runJob();

    }

    private static final String APP_NAME = "page-rank-wiki";
    private static final int NUMBER_OF_ITERATIONS = 25;

    private static final String outputDir = "../data/output/";

    private String linksFileName;
    private String titlesFileName;

    public PageRank() {
        super();
        this.linksFileName = "";
        this.titlesFileName = "";
    }

    public PageRank(String linksFileName, String titlesFileName) {
        this();
        this.linksFileName = linksFileName;
        this.titlesFileName = titlesFileName;
    }

    public void runJob() {

        // SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster(MasterURL.getLocal());
        // JavaSparkContext sc = new JavaSparkContext(conf);

        SparkSession spark = SparkSession
                .builder()
                .appName(APP_NAME).master(MasterURL.getLocal())
                .getOrCreate();

        JavaRDD<String> linksFile = spark.read().textFile(this.linksFileName).javaRDD();

        JavaRDD<String> titlesFile = spark.read().textFile(this.titlesFileName).javaRDD();

        JavaPairRDD<String, Long> titlesIndexed = titlesFile.zipWithIndex();

        JavaPairRDD<Long, String> titlesLookup = titlesIndexed.mapToPair(tuple -> {
            Long indexKey = tuple._2() + 1;
            String value = tuple._1();
            return new Tuple2<>(indexKey, value);
        });

        String titlesOutput = String.format("%s", outputDir);

        titlesIndexed.saveAsTextFile(titlesOutput);

        long totalCount = titlesFile.count();

        JavaRDD<WikiPage> wikiPageRecords = linksFile.map(new WikiPageParser(titlesLookup));

        JavaRDD<Rank> ranks = wikiPageRecords.map(new Function<WikiPage, Rank>() {

            @Override
            public Rank call(WikiPage wikiPage) throws Exception {
                try {
                    double initialValue = (1.0 / totalCount);
                    Rank rank = new Rank(wikiPage.getPageId(), initialValue);
                    return rank;
                } catch (Exception e) {
                    throw e;
                }
            }

        });

        JavaPairRDD<Long, WikiPage> wikiPageIndexed = wikiPageRecords
                .mapToPair(new PairFunction<WikiPage, Long, WikiPage>() {

                    @Override
                    public Tuple2<Long, WikiPage> call(WikiPage wikiPage) throws Exception {
                        return new Tuple2<Long, WikiPage>(wikiPage.getPageId(), wikiPage);
                    }

                });

        JavaPairRDD<Long, Double> rankIndexed = ranks.mapToPair(new PairFunction<Rank, Long, Double>() {

            @Override
            public Tuple2<Long, Double> call(Rank rank) throws Exception {
                return new Tuple2<Long, Double>(rank.getPageId(), rank.getRankValue());
            }

        });

        JavaPairRDD<Long, Tuple2<WikiPage, Double>> joinedRDD = wikiPageIndexed.join(rankIndexed);

        // --------------------------------------------

        JavaPairRDD<Long, Double> contributionsRDD = joinedRDD
                .flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Tuple2<WikiPage, Double>>, Long, Double>() {

                    @Override
                    public Iterator<Tuple2<Long, Double>> call(Tuple2<Long, Tuple2<WikiPage, Double>> entry)
                            throws Exception {
                        Links destinations = entry._2()._1().getLinks();
                        Double currentRank = entry._2()._2();

                        int outDegree = destinations.getCount();

                        if (outDegree == 0) {
                            return new LinkedList<Tuple2<Long, Double>>().iterator();
                        }

                        double contribution = currentRank / outDegree;

                        List<Tuple2<Long, Double>> results = new LinkedList<>();
                        for (Long destinationPageId : destinations.getLinkedPages()) {
                            results.add(new Tuple2<Long, Double>(destinationPageId, contribution));
                        }
                        return results.iterator();
                    }

                });

        String outputPath = String.format("%s%s", outputDir, "output.txt");

        contributionsRDD.saveAsTextFile(outputPath);

        // --------------------------------------------

        // List<Tuple2<Long, Double>> emptyList = Collections.emptyList();
        // JavaRDD<Tuple2<Long, Double>> emptyRDD = sc.parallelize(emptyList);

        // JavaPairRDD<Long, Double> contributions = JavaPairRDD.fromJavaRDD(emptyRDD);

        // for (int n = 0; n < NUMBER_OF_ITERATIONS; n++) {

        // JavaPairRDD<Long, Double> contribution = joinedRDD
        // .mapToPair(new PairFunction<Tuple2<Long, Tuple2<WikiPage, Double>>, Long,
        // Double>() {

        // @Override
        // public Tuple2<Long, Double> call(Tuple2<Long, Tuple2<WikiPage, Double>>
        // input)
        // throws Exception {
        // double contribution = 1.0 / input._2()._1().getLinks().getCount();
        // Tuple2<Long, Double> result = new Tuple2<Long, Double>(input._1(),
        // contribution);
        // return result;
        // }

        // });
        // contributions.join(contribution);
        // }

        // contributions.reduceByKey(new Function2<Double, Double, Double>() {

        // @Override
        // public Double call(Double v1, Double v2) throws Exception {
        // return v1 + v2;
        // }

        // });

        // // JavaRDD<WikiPage> rankedPages = wikiPageRecords.map(null)
        // ranks = ranks.sortBy(new Function<Rank,Double>() {

        // @Override
        // public Double call(Rank v1) throws Exception {
        // return v1.getRankValue();
        // }

        // }, false, NUMBER_OF_ITERATIONS);

        // -----------------------------------------------

        // Dataset<Row> linksRows = spark.read().format("csv").option("header", true)
        // .option("inferSchema", true)
        // .load(this.linksFileName);
        // linksRows.cache();
        // linksRows = linksRows.na().drop();

        // Dataset<Row> titlesRows = spark.read().format("csv").option("header", true)
        // .option("inferSchema", true)
        // .load(this.titlesFileName);
        // titlesRows.cache();
        // titlesRows = titlesRows.na().drop();

        // JavaRDD<Row> titlesRDD = titlesRows.javaRDD();

        // titlesRDD = titlesRDD.map(new Function<Row, Row>() {

        // @Override
        // public Row call(Row v1) throws Exception {
        // Object[] rowObjects = new Object[] { v1.get(0), v1._2() + 1 };
        // return RowFactory.create(rowObjects);
        // }

        // });
        // // JavaPairRDD<Row, Long> indexedTitles = titlesRDD.zipWithIndex();
        // // titlesRDD = indexedTitles.map(
        // // new Function<Tuple2<Row, Long>, Row>() {
        // // @Override
        // // public Row call(Tuple2<Row, Long> t) throws Exception {
        // // Object[] rowObjects = new Object[] { t._1().get(0), t._2() + 1 };
        // // return RowFactory.create(rowObjects);
        // // }

        // // });

        // titlesRDD.map(new Function<Row, Void>() {

        // @Override
        // public Void call(Row v1) throws Exception {
        // StringBuilder builder = new StringBuilder();
        // for (int n = 0; n < v1.size(); n++) {
        // if (n > 0) {
        // builder.append(", ");
        // }
        // builder.append(v1.get(n));
        // }
        // System.out.println(builder.toString());
        // return null;
        // }

        // });

        // // final Pattern SPACE = Pattern.compile(" ");

        // // JavaRDD<String> lines =
        // spark.read().textFile(this.linksFileName).javaRDD();

        // // JavaRDD<String> words = lines.flatMap(s ->
        // // Arrays.asList(SPACE.split(s)).iterator());

        // // JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s,
        // 1));

        // // JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 +
        // i2);

        // // List<Tuple2<String, Integer>> output = counts.collect();
        // // for (Tuple2<?, ?> tuple : output) {
        // // System.out.println(tuple._1() + ": " + tuple._2());
        // // }

        spark.stop();
    }

}
