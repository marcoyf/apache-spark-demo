package br.tec.marco.apachesparkdemo.rdd;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class FilterMapTransformations {
	
	// a regular expression which matches commas but not commas within double quotations, like this: ,"Sao Filipe, Fogo Island",
	public static final String COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("airports").setMaster("local[2]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> airports = sc.textFile("/home/marcoyf/eclipse-workspace/apache-spark-demo/src/main/resources/airports.txt");

        JavaRDD<String> airportsInBrazil = airports.filter(line -> line.split(COMMA_DELIMITER)[3].equals("\"Brazil\""));

        JavaRDD<String> airportsNameAndCityNames = airportsInBrazil.map(line -> {
                    String[] splits = line.split(COMMA_DELIMITER);
                    return StringUtils.join(new String[]{splits[1], splits[2]}, ",");
                }
        );
        // since we configured the app to run in two worker threads, it will generate two output files
        airportsNameAndCityNames.saveAsTextFile("out/airports_in_brazil.txt");
        
        sc.close();
    }
}
