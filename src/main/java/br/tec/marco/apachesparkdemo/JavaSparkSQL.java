/**
 * 
 */
package br.tec.marco.apachesparkdemo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

/**
 * @author marcoyf
 *
 */
public class JavaSparkSQL {
	
	public static void main(String[] args) {
		
		//SparkSession spark = SparkSession.builder().master("local[*]").getOrCreate();
		SparkSession spark = SparkSession.builder().getOrCreate();
		Dataset<Row> df = spark.read().json("/home/marcoyf/eclipse-workspace/apache-spark-demo/src/main/resources/people.json");
		
		df.show();
		df.select("name").show();
		df.select(col("name"), col("age").plus(1)).show();
		
		spark.stop();
	}
}