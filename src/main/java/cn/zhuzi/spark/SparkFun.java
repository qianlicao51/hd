package cn.zhuzi.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * @Title: SparkFun.java向spark传递函数
 * @Package cn.zhuzi.spark
 * @Description: TODO(向spark传递函数 )
 * @author 作者 grq
 * @version 创建时间：2018年11月15日 下午7:07:57 向spark传递函数
 */
public class SparkFun {
	static String base_path = "E:/had/spark/";
	static String readmeFile = "README.md";
	static SparkConf conf;
	static JavaSparkContext sc;
	/**
	 * 初始化sparkContext
	 * 
	 */
	static {
		// TODO 创建SparkContext
		// 的最基本方法，只需要传递2个参数https://www.cnblogs.com/Forever-Road/p/7351245.html
		/**
		 * 1:集群URL 告诉spark如何连接到集群上，这个实例中使用local，这个特殊的值可以让spark运行在
		 * 单机单线程上而不需要连接到集群
		 * <p/>
		 * 2:应用名：当连接到一个集群时，这个值可以帮助你在集群管理器的用户界面中找到你的应用
		 */
		conf = new SparkConf().setMaster("local").setAppName("sparkDemo");
		sc = new JavaSparkContext(conf);
	}

	/**
	 * Java中使用匿名内部类进行函数传递
	 */
	public static void fun1() {
		JavaRDD<String> textFile = sc.textFile(base_path);
		JavaRDD<String> filter = textFile.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String v1) throws Exception {
				return v1.contains("Python");
			}
		});
	}

	/**
	 * 在Java8中使用lamdba表达式进行函数传递
	 */
	public static void fun3() {
		JavaRDD<String> lines = sc.textFile(base_path);
		lines.filter(s -> s.contains("Python"));
	}

	/**
	 * 在Java中使用具名类进行函数传递
	 * <p/>
	 * 顶级具名类通常在组织大型程序时显得比较清晰，使用顶级函数的另一个好处是 可以给他们的构造函数添加参数
	 */
	public static void fun2() {

		JavaRDD<String> lines = sc.textFile(base_path);
		JavaRDD<String> filter = lines.filter(new ContainsStr("Python"));
	}

}

class ContainsStr implements Function<String, Boolean> {
	private String query;

	public ContainsStr(String query) {
		this.query = query;
	}

	@Override
	public Boolean call(String v1) throws Exception {

		return v1.contains(query);
	}

}