package crawl.warc;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
/**
 * 
 * This works JavaRDD<Map<String, String>> to save 
 * JAVARDD<JSONOBJECT>
 * 
 *
 */
public class App {

	public static void main(String[] args) {
		final SparkConf conf = new SparkConf().setAppName("test");
		conf.set("es.index.auto.create", "true");
		conf.set("es.nodes", "127.0.0.1");
		conf.set("es.port", "9200");
		final JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> lines = sc.textFile(args[0]);
		JavaRDD<String> linesFilter = lines.filter(new Function<String, Boolean>() {

			@Override
			public Boolean call(String val) throws Exception {
				// TODO Auto-generated method stub
				if (StringUtils.isBlank(val)) {
					return false;
				} else {
					return true;
				}
			}
		});
		JavaRDD<Map<String, String>> li = linesFilter.map(new Function<String, Map<String, String>>() {

			@Override
			public Map<String, String> call(String mapvalues) throws Exception {
				// TODO Auto-generated method stub
					Map<String, String> map = new HashMap<>();
					map.put("hadoop" + new Random().nextInt(10) , mapvalues);
					return map;
					
				
			}

			
		});
		linesFilter.saveAsTextFile(args[2]);

		lines.saveAsTextFile(args[3]);
		/*JavaPairRDD<String, String> linesPair= lines.mapToPair(new PairFunction<String, String, String>() {

			@Override
			public Tuple2<String, String> call(String value) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<String, String>(new Random().nextInt(10) + "hadoop", value);
			}
		});*/
		
		
		
		/*Map<String, String> numbers = new HashMap<>();
		numbers.put("1", "test");
		Map<String, String> airports = new HashMap<>();
		airports.put("OTP", "Otopeni" );
		airports.put("SFO", "San Fran");
		
		List list = new ArrayList<>();
		list.add(numbers);
		list.add(airports);
		JavaRDD<Map<String, String>> javaRDD = sc.parallelize(list);*/

		
		li.saveAsTextFile(args[1]);
		JavaEsSpark.saveToEs(li, "spark1/docs"); 
		
		//JavaEsSpark.saveToEs(lines, "sparktest/docs");
		//sc.close();
	}

}
