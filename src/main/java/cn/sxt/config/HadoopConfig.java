package cn.sxt.config;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.ibatis.io.Resources;
import org.joda.time.DateTime;

public class HadoopConfig {

	static Configuration conf = new Configuration(true);

	public static String inPath = "D:/soft/bd/hdata/in/";
	public static String outPath = "D:/soft/bd/hdata/out/";

	/**
	 * 获取 resource 文件路劲下面的文件
	 * 
	 * @param path
	 * @return
	 */
	public static String getInputPath(String path) {
		File asFile = null;
		try {
			asFile = Resources.getResourceAsFile(path);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return asFile.getAbsolutePath();
	}

	public static String getOutPath() {
		String dateTimeStr = new DateTime().toString("yyyy-MM-dd_HH_mm_ss");
		return outPath + dateTimeStr;
	}

}
