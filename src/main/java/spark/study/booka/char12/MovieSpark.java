/**  
 * All rights Reserved, Designed By grq
 * @Title:  MovieSpark.java   
 * @Package spark.study.booka.char12   
 * @Description:    TODO(电影推荐)   
 * @author: grq  
 * @date:   2019年1月2日 下午8:10:19   
 * @version V1.0 
 * @Copyright: 2019 grq All rights reserved. 
 */
package spark.study.booka.char12;

import java.io.IOException;

import org.apache.ibatis.io.Resources;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * @author MI
 *
 */
public class MovieSpark {
	static JavaSparkContext sc;
	static String base_path = null;
	/**
	 * Spark运行模式 1.local --Eclipse 开发本地模式 多用于测试
	 * <p>
	 * 2.standalone --Spark 自带的资源调度框架
	 * <p>
	 * 支持分布式搭建，Spark任务可以依赖standalone调度资源
	 * <p>
	 * 3.yarn -- Hadoop 生态圈资源框架 Spark也支持yarn
	 */
	static {

		sc = new JavaSparkContext(SparkSession.builder().appName("JavaSparkPi").master("local").getOrCreate().sparkContext());
		/**
		 * 使用mybatis 获取资源文件路径
		 */
		try {
			base_path = Resources.getResourceAsFile("data/spark/movie").getAbsolutePath();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
