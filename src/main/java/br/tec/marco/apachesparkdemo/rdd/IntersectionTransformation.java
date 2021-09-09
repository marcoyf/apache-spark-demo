package br.tec.marco.apachesparkdemo.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class IntersectionTransformation {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("sameHosts").setMaster("local[1]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> julyFirstLogs = sc.textFile("/home/marcoyf/eclipse-workspace/apache-spark-demo/src/main/resources/nasa_19950701.log");
        JavaRDD<String> augustFirstLogs = sc.textFile("/home/marcoyf/eclipse-workspace/apache-spark-demo/src/main/resources/nasa_19950801.log");

        JavaRDD<String> julyFirstHosts = julyFirstLogs.map(line -> line.split("\t")[0]);

        JavaRDD<String> augustFirstHosts = augustFirstLogs.map(line -> line.split("\t")[0]);

        JavaRDD<String> intersection = julyFirstHosts.intersection(augustFirstHosts);

        JavaRDD<String> cleanedHostIntersection = intersection.filter(host -> !host.equals("host"));

        cleanedHostIntersection.saveAsTextFile("out/nasa_logs_same_hosts.log");
        
        sc.close();
    }
}
