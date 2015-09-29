package mygroup.myartifact;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

/**
 * This class helps to move files between local and hdfs
 *
 */
public class HDFSFileCopy {
	public static void main(String[] args) {
		try {
			String hdfspath = "hdfs://192.168.184.165:54310";

			String src = "/home/user/union_input.csv";
			String dst = "/union_input.csv";
			copyToHDFS(src, dst, hdfspath);

			src = dst;
			dst = "/home/user/output.dat";
			copyFromHDFS(src, dst, hdfspath);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void copyToHDFS(String src, String dst, String hdfspath) throws IOException, URISyntaxException {
		InputStream inputStream = new BufferedInputStream(new FileInputStream(src));

		Configuration configuration = new Configuration();
		FileSystem hdfs = FileSystem.get(new URI(hdfspath), configuration);
		OutputStream outputStream = hdfs.create(new Path(hdfspath + dst), new Progressable() {
			public void progress() {
				// System.out.print(".");
			}
		});
		try {
			IOUtils.copyBytes(inputStream, outputStream, 4096, false);
		} finally {
			IOUtils.closeStream(inputStream);
			IOUtils.closeStream(outputStream);
		}
		System.out.println("local:" + src + "  ---->  hdfs:" + dst);
	}

	public static void copyFromHDFS(String src, String dst, String hdfspath) throws IOException, URISyntaxException {
		Configuration configuration = new Configuration();
		FileSystem hdfs = FileSystem.get(new URI(hdfspath), configuration);
		FSDataInputStream inputStream = hdfs.open(new Path(src));

		OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(new File(dst)));
		try {
			IOUtils.copyBytes(inputStream, outputStream, 4096, false);
		} finally {
			IOUtils.closeStream(inputStream);
			IOUtils.closeStream(outputStream);
		}
		System.out.println("hdfs:" + src + "  ---->  local:" + dst);
	}
}