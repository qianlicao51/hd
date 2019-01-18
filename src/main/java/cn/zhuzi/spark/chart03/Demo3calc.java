package cn.zhuzi.spark.chart03;

import cn.zhuzi.spark.SparkUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author 作者 grq
 * @version 创建时间：2018年11月15日 下午9:03:56
 * @Title: Demo3calc.java
 * @Package cn.zhuzi.spark
 * @Description: TODO(书第三章 代码实例 ， 代码尽量能是2份 ， 一份是java7之前 ， 另一份是Java8lamdba)
 */
public class Demo3calc {
    static JavaSparkContext sc = SparkUtils.getContext();

    public static void main(String[] args) {

        calcMap();
    }

    /**
     * 两次执行
     */
    private void doucleCalc() {
        JavaRDD<Integer> lines = sc.parallelize(Arrays.asList(1, 2, 3, 4));
        JavaRDD<Integer> result = lines.map(x -> x * 5);
        result.persist(StorageLevel.DISK_ONLY());
        System.out.println(result.count());
        System.out.println(StringUtils.join(result.collect(), ","));
    }

    /*
     *
     */
    public static void calcMap() {
        JavaRDD<Integer> lines = sc.parallelize(Arrays.asList(1, 2, 3, 4));
        JavaDoubleRDD mapToDouble = lines.mapToDouble(x -> x * x);
        System.out.println(mapToDouble.collect());// [1.0, 4.0, 9.0, 16.0]
        System.out.println(mapToDouble.mean());// 7.5 这个值好像是上面的平均值
    }

    /**
     * map
     */
    public static void map() {
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
        JavaRDD<Integer> map = rdd.map(x -> x + 1);
        System.out.println(map.collect());

        sc.close();
    }

    /**
     * Java 中的 flatMap() 将行数据切分为单词
     */
    public static void faltMap() {
        JavaRDD<String> lines = sc.parallelize(Arrays.asList("hello Spark"));
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String t) throws Exception {
                return Arrays.asList(t.split(" ")).iterator();
            }
        });
        System.out.println(words.first());// hello
        System.out.println(words.collect().toString());// [hello, Spark]
    }

    /**
     * Java 中的 flatMap() 将行数据切分为单词 Lamdba版本
     */
    public static void faltMapLamdba() {
        JavaRDD<String> lines = sc.parallelize(Arrays.asList("hello Spark"));
        JavaRDD<String> words = lines.flatMap(w -> Arrays.asList(w.split(" ")).iterator());
        System.out.println(words.first());// hello
        System.out.println(words.collect().toString());// [hello, Spark]

    }

    /**
     * Java 版计算 RDD 中各值的平方
     */
    private static void calc() {

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
        JavaRDD<Integer> result = rdd.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                return v1 * v1;
            }
        });
        System.out.println(StringUtils.join(result.collect(), "  "));
        sc.close();
    }

    /**
     * Java 版计算 RDD 中各值的平方
     */
    private static void calcLamdba() {
        JavaSparkContext sc = SparkUtils.getContext();
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
        JavaRDD<Integer> result = rdd.map(x -> x * x);
        System.out.println(StringUtils.join(result.collect(), "  "));
        sc.close();
    }

}
