package br.tec.marco.apachesparkdemo.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class ReduceAction {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkConf conf = new SparkConf().setAppName("reduce").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> inputIntegers = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> integerRdd = sc.parallelize(inputIntegers);

        Integer product = integerRdd.reduce((x, y) -> x * y);
        
        // (1 * 2) 	(3 * 4) 	(5)
        // (2) 		(12)
        // 						(12 * 5)
        // 						(60)
        // (2 * 60)
        // (120)
        System.out.println("product is :" + product);
        
        sc.close();
    }
}
