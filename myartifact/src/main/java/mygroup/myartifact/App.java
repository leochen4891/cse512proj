package mygroup.myartifact;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) {
		JavaSparkContext sc = null;

		try {
			System.out.println("Hello World!");
			SparkConf conf = new SparkConf().setAppName("MyAppName").setMaster("local");
			sc = new JavaSparkContext(conf);

			// JavaRDD<String> lines = sc.textFile("/home/user/input.txt");
			// JavaRDD<Integer> lineLengths = lines.map(s -> s.length());
			// int totalLength = lineLengths.reduce((a, b) -> a + b);
			// System.out.println("total length of all lines in LOCAL file
			// -------> " + totalLength);

			// 1. read the lines from the input file in HDFS
			JavaRDD<String> lines1 = sc.textFile("hdfs://192.168.184.165:54310/test_data.dat");
			
			// 2. perform some calculation
			JavaRDD<Integer> lineLengths1 = lines1.map(s -> s.length());
			int totalLength1 = lineLengths1.reduce((a, b) -> a + b);
			
			// 3. output to a file
			System.out.println("total length of all lines in HDFS file -------> " + totalLength1 + "\n");

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (null != sc)
				sc.close();
		}

	}
}
