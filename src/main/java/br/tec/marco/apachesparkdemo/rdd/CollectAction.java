package br.tec.marco.apachesparkdemo.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class CollectAction {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("collect").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> inputWords = Arrays.asList("spark", "hadoop", "spark", "hive", "pig", "cassandra", "hadoop");
        JavaRDD<String> wordRdd = sc.parallelize(inputWords);

        // the entire dataset must fit in memory on a single machine as it all needs to be copied to the driver when the collect action is called
        // collect action should NOT be used on large datasets
        List<String> words = wordRdd.collect();

        for (String  word : words) {
            System.out.println(word);
        }
        
        sc.close();
    }
}
