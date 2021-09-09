package br.tec.marco.apachesparkdemo.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.Map;

public class FlatMapTransformation {

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        
        SparkConf conf = new SparkConf()
        		.setAppName("wordCounts")
        		.setMaster("local[3]"); // running on local box with up to 3 CPU cores
        		//.setMaster("local"); // running on local box with 1 core/thread
        		//.setMaster("local[*]"); // running on local box with all available cores
        
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("/home/marcoyf/eclipse-workspace/apache-spark-demo/src/main/resources/word_count.txt");
        
        // map vs flatMap transformations
        // map: use it for 1 to 1 relationships
        // flatMap: use it for 1 to many relationships
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        Map<String, Long> wordCounts = words.countByValue();

        for (Map.Entry<String, Long> entry : wordCounts.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
        
        sc.close();
    }
}
