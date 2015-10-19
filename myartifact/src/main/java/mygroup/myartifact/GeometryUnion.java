package mygroup.myartifact;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.JavaRDD;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

public class GeometryUnion {
	private static final String HDFS_ROOT_PATH = "hdfs://192.168.184.165:54310/";
	private static final String LOCAL_PATH = "/home/user/";

	private static final String INPUT_FILE = "union_input.csv";
	private static final String OUTPUT_FILE = "union_output.csv";

	public static void main(String[] args) {
		JavaSparkContext sc = null;
		InputStream inputStream = null;
		OutputStream outputStream = null;

		try {
			System.out.println("Geometry Union Start!");

			// 0. copy local file to HDFS
			HDFSFileCopy.copyToHDFS(INPUT_FILE, INPUT_FILE, HDFS_ROOT_PATH, LOCAL_PATH);

			// 1. read the lines from the input file in HDFS
			boolean local = false;
			if (local) {
				sc = new JavaSparkContext("local", "GeometryUnion");
			} else {
				sc = new JavaSparkContext("spark://192.168.184.165:7077", "GeometryUnion",
						"/home/user/spark-1.5.0-bin-hadoop2.6",
						new String[] { "target/myartifact-0.1.jar", "lib/jts/lib/jts-1.8.jar" });
			}
			JavaRDD<String> lines = sc.textFile(HDFS_ROOT_PATH + INPUT_FILE);

			// 2. Geometry Union
			// 2.1 map: convert each line of string to a polygon
			JavaRDD<Geometry> polygons = lines.map(new Function<String, Geometry>() {
				private static final long serialVersionUID = -1928298089452870258L;

				public Geometry call(String s) {
					return UnionJTS.getRectangleFromLeftTopAndRightBottom(s);
				}
			});

			// 2.2 reduce: combine every 2 polygons
			Geometry finalPolygon = polygons.reduce(new Function2<Geometry, Geometry, Geometry>() {
				private static final long serialVersionUID = -1967342595615519573L;

				@Override
				public Geometry call(Geometry arg0, Geometry arg1) throws Exception {
					return arg0.union(arg1);
				}
			});

			// 3. output to an HDFS file
			Configuration configuration = new Configuration();
			FileSystem hdfs = FileSystem.get(new URI(HDFS_ROOT_PATH), configuration);
			outputStream = hdfs.create(new Path(HDFS_ROOT_PATH + OUTPUT_FILE), new Progressable() {
				public void progress() {
					// System.out.print(".");
				}
			});
			PrintWriter pw = new PrintWriter(outputStream);
			for (Coordinate cor : finalPolygon.getCoordinates()) {
				pw.println(cor.x + ", " + cor.y);
				System.out.println(cor.x + ", " + cor.y);
			}
			pw.flush();
			pw.close();

			// 4. copy the output file from HDFS to local disk
			HDFSFileCopy.copyFromHDFS(OUTPUT_FILE, OUTPUT_FILE, HDFS_ROOT_PATH, LOCAL_PATH);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(inputStream);
			IOUtils.closeStream(outputStream);
			if (null != sc)
				sc.close();
		}
	}

}
