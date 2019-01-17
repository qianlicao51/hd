/**  
 * All rights Reserved, Designed By grq
 * @Title:  MusicAnalyze.java   
 * @Package spark.study.bookb.char03   
 * @Description:    TODO(用一句话描述该文件做什么)   
 * @author: grq  
 * @date:   2019年1月17日 上午10:00:11   
 * @version V1.0 
 * @Copyright: 2019 grq All rights reserved. 
 */
package spark.study.bookb.char03;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;

import cn.zhuzi.spark.SparkUtils;
import scala.Tuple2;

/**
 * @author MI
 *
 */
public class MusicAnalyze {

	private static String base_path = "D:\\搜狗高速下载\\profiledata_06-May-2005";

	/**
	 * 文件的每行包含一个用户 ID、一个艺术家 ID 和播放次数，用空格分隔
	 * <p>
	 * userid artistid playcount
	 */

	private static String user_artist_data = "/user_artist_data.txt";

	/**
	 * 包含艺术家 ID 和名字，它们用制表符分隔
	 * <p>
	 * artistid artist_name
	 */
	private static String artist_data = "/artist_data.txt";
	/**
	 * 拼写错误的艺术家 ID 或非标准的艺术家 ID 映射为艺术家的正规名字。其 中每行有两个 ID，用制表符分隔
	 */
	private static String artist_alias = "/artist_alias.txt";
	static JavaSparkContext sc = SparkUtils.getContext();

	public static void main(String[] args) {

		JavaPairRDD<Integer, String> artistData = artistData();
		// userid artistid playcount
		JavaRDD<String> rawUserArtistData = userArtits();

		// 广播别名
		Map<Integer, Integer> artistAlias = artistAlias();
		Broadcast<Map<Integer, Integer>> bArtistAlias = sc.broadcast(artistAlias);

		// 次方法是参考的 https://blog.csdn.net/Next__One/article/details/78331474 看着像java8 的流
		// 数据转换
		JavaRDD<Rating> trainData = rawUserArtistData.map(line -> {
			List<Integer> list = Arrays.asList(line.split(" ")).stream().map(x -> Integer.parseInt(x)).collect(Collectors.toList());
			Integer finalArtistID = bArtistAlias.getValue().getOrDefault(list.get(1), list.get(1));
			// 如果艺术家存在别名，取得艺术家别名，否则取得原始名字
			return new Rating(list.get(0), finalArtistID, list.get(2));
		}).cache();

		// 构建模型
		// 模型用两个不同的 RDD，它们分别表示“用户 - 特征”和“产品 - 特征”这两个大型矩阵
		MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(trainData), 10, 5, 0.01, 1);
		// 查看特征变量
		model.userFeatures().toJavaRDD().foreach(f -> {
			System.out.println(f._1.toString() + f._2[0] + f._2.toString());
		});

		sc.close();
	}

	/**
	 * 艺术家 ID 和名字 。但是简单地把文件解 析成二元组 (Int,String) 会出错
	 * <p>
	 * 文件里有少量行看起来是非法的：有些行没有制表符，有些行不小心加入了换行符。这些行会导致 NumberFormatException
	 * <p>
	 * map函数要求每个输入的必须严格返回一个值，因此这里不能用这个函数。另一个可行的方法是用 filter()
	 * 方法删除那些无法解析的行，但这会重复解析逻辑。当需要将每个元素映射为零个、一个或更多结果时，我们应该使用 flatMap()
	 * 函数，因为它将每个输入对应的零个或多个结果组成的集合简单展开，然后放入到一个更大的 RDD 中。它可 以和 Scala 集合一起使用，也可以和 Scala
	 * 的 Option 类一起使用。 Option 代表一个值可以不存在，有点儿像只有 1 或 0 的一个简单集合，1 对应子类 Some ，0 对应子类
	 * None 。因此 在以下代码中，虽然 flatMap 中的函数本可以简单返回一个空 List ，或一个只有一个元素 的 List ，但使用 Some 和
	 * None 更合理，这种方法简单明了
	 * <p>
	 * 我使用的是java 可能没有 none 和 some这方法 仍旧使用过滤
	 * <p>
	 * 
	 * @return
	 * 
	 * @return
	 */
	private static JavaPairRDD<Integer, String> artistData() {
		JavaRDD<String> textFile = sc.textFile(base_path + artist_data);
		JavaRDD<String> filter = textFile.filter(new Function<String, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String v1) throws Exception {
				int length = v1.split("\t").length;
				try {
					Integer.parseInt(v1.split("\t")[0]);
					return length == 2;
				} catch (Exception e) {
					// System.out.println("非法数据 " + v1);
				}
				return false;
			}
		});
		JavaPairRDD<Integer, String> toPair = filter.mapToPair(t -> new Tuple2<Integer, String>(Integer.parseInt(t.split("\t")[0]), t.split("\t")[1]));
		return toPair;

	}

	/**
	 * 数据分析 user_artist_data
	 * 
	 * @return
	 */
	private static JavaRDD<String> userArtits() {
		JavaRDD<String> rawUserArtistData = sc.textFile(base_path + user_artist_data, 1);
		JavaDoubleRDD mapToDouble = rawUserArtistData.mapToDouble(t -> Double.parseDouble(t.split(" ")[0]));
		System.out.println(mapToDouble.stats());
		return rawUserArtistData;

	}

	/**
	 * 将拼写错误的艺术家ID或非标准的艺术家ID映射为艺术家的正规名
	 * 
	 * @return
	 */
	public static Map<Integer, Integer> artistAlias() {
		JavaRDD<String> rawArtistAlias = sc.textFile(base_path + artist_alias, 1);
		rawArtistAlias = rawArtistAlias.filter(new Function<String, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String v1) throws Exception {
				String[] split = v1.split("\t");
				try {
					Integer.parseInt(split[0]);
					Integer.parseInt(split[1]);
					return split.length == 2;
				} catch (Exception e) {
				}
				return false;
			}
		});
		// 转换为键值对
		JavaPairRDD<Integer, Integer> mapToPair = rawArtistAlias.mapToPair(t -> new Tuple2<Integer, Integer>(Integer.parseInt(t.split("\t")[0]), Integer.parseInt(t.split("\t")[1])));
		// 注意这个方法(map) 与mapToPair
		JavaRDD<Tuple2<Integer, Integer>> map = rawArtistAlias.map(t -> new Tuple2<Integer, Integer>(Integer.parseInt(t.split(" ")[0]), Integer.parseInt(t.split(" ")[1])));
		return mapToPair.collectAsMap();
	}
}
