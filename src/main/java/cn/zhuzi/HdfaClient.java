package cn.zhuzi;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author 作者 grq
 * @version 创建时间： 2018年6月28日 下午4:03:00
 *
 */
public class HdfaClient {

	FileSystem fs;

	@Before
	public void before() throws IOException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		fs = FileSystem.get(new URI("hdfs://ym2:9000"), conf, "grq");

	}

	@Test
	public void upFile() throws IOException {
		/* 上传并删除本地文件 */
		fs.copyFromLocalFile(true, new Path("c:/1.jpg"), new Path("/1.jpg"));
		System.out.println("HdfaClient.upFile()");
	}

	/**
	 * 下载文件
	 * 
	 * @throws IOException
	 */
	@Test
	public void downFile() throws IOException {
		fs.copyToLocalFile(true, new Path("/1.jpg"), new Path("c:/1.jpg"));
	}

	@After
	public void aftet() throws IOException {
		fs.close();
	}

	/*************************************************************
	 * 目录操作
	 * 
	 * @throws IOException
	 * @throws IllegalArgumentException
	 */
	@Test
	public void mkdir() throws IllegalArgumentException, IOException {
		fs.mkdirs(new Path("/g/r/q"));
	}

	@Test
	public void delDir() throws IllegalArgumentException, IOException {
		fs.delete(new Path("/grq"));
	}

	/**
	 * 列出文件 ，不包括文件夹
	 * 
	 * @throws IOException
	 */
	@Test
	public void listDir() throws IOException {
		// 第二个参数 if the subdirectories need to be traversed recursively
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
		while (listFiles.hasNext()) {
			// 获取单个文件状态
			LocatedFileStatus fileStatus = listFiles.next();
			String name = fileStatus.getPath().getName();
			long len = fileStatus.getLen();
			FsPermission permission = fileStatus.getPermission();
			System.out.println("name---" + name + "   len" + len);
		}
	}

	/**
	 * 上传文件
	 * 
	 * @throws IOException
	 */
	@Test
	public void copyStream() throws IOException {

		FileInputStream inputStream = new FileInputStream(new File("e:/2.jpg"));

		FSDataOutputStream outputStream = fs.create(new Path("/img/1.jpg"));
		try {
			int copy = org.apache.commons.io.IOUtils.copy(inputStream, outputStream);

		} catch (Exception e) {
			// TODO: handle exception
		} finally {
			IOUtils.closeQuietly(inputStream);
			IOUtils.closeQuietly(outputStream);
		}
	}

	@Test
	public void downFileByStream() throws IllegalArgumentException, IOException {

		FSDataInputStream open = fs.open(new Path("/img/1.jpg"));
		FileOutputStream fileOutputStream = new FileOutputStream(new File("c:/2.jpg"));

		IOUtils.copy(open, fileOutputStream);
		IOUtils.closeQuietly(fileOutputStream);
		IOUtils.closeQuietly(open);
	}
}
