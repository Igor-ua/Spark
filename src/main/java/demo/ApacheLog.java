package demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;

/**
 * Apache Log Parser
 * Log format:
 * ip_adress - - [dd/MMM/yyyy:HH:mm:ss Z] "HTTP-method URL HTTP-protocol-version HTTP-status-code request-size"
 * 192.168.100.200 - - [04/May/2015:11:28:00 +0300] "POST /scripts/checker?browser_id=1 HTTP/1.1" 200 503
 *
 */
public class ApacheLog {

    public static void main(String[] args) throws InterruptedException, IOException {
        SparkConf conf = new SparkConf()
                .setAppName("Apache access.log")
                .setMaster("local")
                .set("spark.driver.host", "localhost");    //это тоже необходимо для локального режима

        JavaSparkContext sc = new JavaSparkContext(conf);

        // Spark can cache datasets in memory to speed up reuse
        // Load our input data
        JavaRDD<String> rdd = sc.textFile("access.log")
                .cache();

        String after = "03/May/2015";
        String before  = "05/May/2015";

        ipStatistics(rdd);
        checkForErrors(rdd, after, before);

        Thread.sleep(10000000L);
    }

    public static void checkForErrors(JavaRDD<String> rdd, String after, String before) {

        long errors = rdd
                .map(line -> line.split(" "))
                .filter(elements -> Utils.isInRange(elements[3].substring(1,12), after, before))
                .filter(elements -> !elements[8].contains("200"))
                .count();

        System.out.println("Errors: " + errors);
    }

    public static void ipStatistics(JavaRDD<String> rdd) {

        JavaPairRDD<String, Integer> ips = rdd
                .map(line -> line.split(" "))
                .mapToPair(fields -> new Tuple2<>(fields[0], 1));

        JavaPairRDD<Integer, String> result = ips
                .reduceByKey((a, b) -> a + b)
                .mapToPair(f -> new Tuple2<>(f._2, f._1))
                .sortByKey(false);

        result.saveAsTextFile("ip_count");

        System.out.println("IP count: " + ips.count());
    }
}
