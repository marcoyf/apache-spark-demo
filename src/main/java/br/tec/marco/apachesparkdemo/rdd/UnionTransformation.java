package br.tec.marco.apachesparkdemo.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class UnionTransformation {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("unionLogs").setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> julyFirstLogs = sc.textFile("/home/marcoyf/eclipse-workspace/apache-spark-demo/src/main/resources/nasa_19950701.log");
        JavaRDD<String> augustFirstLogs = sc.textFile("/home/marcoyf/eclipse-workspace/apache-spark-demo/src/main/resources/nasa_19950801.log");

        JavaRDD<String> aggregatedLogLines = julyFirstLogs.union(augustFirstLogs);

        JavaRDD<String> cleanLogLines = aggregatedLogLines.filter(UnionTransformation::isNotHeader);

        JavaRDD<String> sample = cleanLogLines.sample(true, 0.1);

        sample.saveAsTextFile("out/sample_nasa_logs.log");
        
        sc.close();
    }

    private static boolean isNotHeader(String line) {
        return !(line.startsWith("host") && line.contains("bytes"));
    }
}
