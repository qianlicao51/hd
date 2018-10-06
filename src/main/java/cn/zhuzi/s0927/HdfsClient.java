package cn.zhuzi.s0927;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

/**
 * @Title: HdfsClient.java
 * @Package cn.zhuzi.s0927
 * @Description: TODO(hdfs)
 * @author 作者 grq
 * @version 创建时间：2018年9月27日 下午10:11:52
 *
 */
public class HdfsClient {
	public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "grq");
		System.out.println(fs);
	}

	FileSystem fs;

	@Before
	public void bef() throws IOException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "grq");
	}

	/**
	 * 文件上传
	 * 
	 * @throws IOException
	 */
	@Test
	public void putFile() throws IOException {
		fs.copyFromLocalFile(new Path("c://1.jpg"), new Path("/1.jpg"));
		fs.close();
	}

	@Test
	public void getFile() throws IllegalArgumentException, IOException {
		fs.copyToLocalFile(new Path("/1.jpg"), new Path("d://11.jpg"));
	}

	@Test
	public void mkdir() throws IllegalArgumentException, IOException {
		fs.mkdirs(new Path("/img/jpg"));
	}

	@Test
	public void del() throws IllegalArgumentException, IOException {
		fs.delete(new Path("/img/jpg"), true);

	}

	/**
	 * 重命名文件
	 * 
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void renameFile() throws IllegalArgumentException, IOException {
		fs.rename(new Path("/1.jpg"), new Path("/12.jpg"));
	}

	/**
	 * 文件信息
	 * 
	 * @throws FileNotFoundException
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void listFile() throws FileNotFoundException, IllegalArgumentException, IOException {

		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

		while (listFiles.hasNext()) {
			LocatedFileStatus next = listFiles.next();
			System.out.println(next.getPath() + "<>" + next.getReplication());
		}

	}

	/**
	 * IO流上传文件
	 * 
	 * @throws IOException
	 */
	@Test
	public void ioUp() throws IOException {

		FileInputStream inputStream = FileUtils.openInputStream(Paths.get("c://1.jpg").toFile());
		FSDataOutputStream outputStream = fs.create(new Path("/123.jpg"));

		org.apache.commons.io.IOUtils.copy(inputStream, outputStream);
		org.apache.commons.io.IOUtils.closeQuietly(outputStream);
		org.apache.commons.io.IOUtils.closeQuietly(inputStream);
	}

	/**
	 * IO流下载
	 */
	@Test
	public void ioDown() throws IllegalArgumentException, IOException {
		FSDataInputStream open = fs.open(new Path("/123.jpg"));
		org.apache.commons.io.IOUtils.copy(open, FileUtils.openOutputStream(new File("d://1234.jpg")));
		org.apache.commons.io.IOUtils.closeQuietly(open);
	}

	
	
	
}
