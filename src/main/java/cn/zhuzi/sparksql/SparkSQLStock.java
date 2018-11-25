package cn.zhuzi.sparksql;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.apache.ibatis.io.Resources;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * @Title: SparkSQLStock.java
 * @Package cn.zhuzi.sparksql
 * @Description: TODO(订单查询)
 * @author 作者 grq
 * @version 创建时间：2018年11月25日 上午1:35:41
 *
 */
public class SparkSQLStock {
	static SparkSession sparkSession;
	/**
	 * 数据文件位置
	 */
	static String base_path;
	static {
		sparkSession = SparkSession.builder().appName("SparkSQL_demo").master("local").getOrCreate();
		try {
			/**
			 * 使用mybatis 获取资源文件路径
			 */
			base_path = Resources.getResourceAsFile("data/order/").getAbsolutePath();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings({ "resource", "serial" })
	public static void sumamountBean() {

		JavaSparkContext context = new JavaSparkContext(sparkSession.sparkContext());
		JavaRDD<StockDetail> javaRDD = context.textFile(base_path + "/tblStockDetail.txt").map(new Function<String, StockDetail>() {
			@Override
			public StockDetail call(String v1) throws Exception {
				String[] split = v1.split(",");
				StockDetail detail = new StockDetail();
				detail.setOrdernumber(split[0]);
				detail.setRownum(Integer.valueOf(split[1]));
				detail.setItemid(split[2]);
				detail.setQty(Integer.valueOf(split[3]));
				detail.setPrice(Double.valueOf(split[4]));
				detail.setAmount(Double.valueOf(split[5]));
				return detail;
			}
		});

		Encoder<StockDetail> encodeStockDetail = Encoders.bean(StockDetail.class);
		Dataset<StockDetail> dataset = sparkSession.createDataFrame(javaRDD, StockDetail.class).as(encodeStockDetail);
		dataset.createOrReplaceTempView("temp_detail");
		// cast(sum(online_time) as bigint) num
		sparkSession.sql("select  cast( sum(amount) as bigint)   sum_bean from temp_detail").show();
	}

	public static void main(String[] args) {

		Dataset<Row> dateDS = readFileToDS(base_path + "/tblDate.txt");
		dateDS.createOrReplaceTempView("tblDate");

		String scheamDate = "dateID,theyearmonth,theyear,themonth,thedate,theweek,theweeks,thequot,thetenday,thehalfmonth";
		Dataset<Row> datasetDate = rddToDateSet(sparkSession, base_path + "/tblDate.txt", scheamDate, ",");

		String schemStock = "ordernumber,locationid,dateID";
		Dataset<Row> datasetStock = rddToDateSet(sparkSession, base_path + "/tblStock.txt", schemStock, ",");

		String schemStockDetail = "ordernumber,rownum,itemid,qty,price,amount";
		Dataset<Row> datasetStockDetail = rddToDateSet(sparkSession, base_path + "/tblStockDetail.txt", schemStockDetail, ",");

		datasetDate.createOrReplaceTempView("tbldate");
		datasetStock.createOrReplaceTempView("tblStock");
		datasetStockDetail.createOrReplaceTempView("tblStockDetail");
		sparkSession.sql("select cast( sum(amount) as bigint)  as sum from tblStockDetail  ").show();
		sumamountBean();
		// |6.813666225920284E7|科学计数法
		// https://blog.csdn.net/zjy15203167987/article/details/80885530
		// |68136662| hive不使用科学计数法
		// https://blog.csdn.net/u010670689/article/details/44748131
		Double num = 6.813666225920284E7;
		String str = new BigDecimal(num.toString()).toString();
		System.out.println(str);
		// -------------------------------------------------------------------------------
		// TODO 所有订单中每年的销售单数、销售总额
		// sparkSession.sql("select c.theyear,count(distinct a.ordernumber),sum(b.amount) from tblStock a, tblStockDetail b,tbldate c where a.ordernumber=b.ordernumber and a.dateID=c.dateID group by c.theyear order by  c.theyear").show();
		// +-------+---------------------------+---------------------------+
		// |theyear|count(DISTINCT ordernumber)|sum(CAST(amount AS DOUBLE))|
		// +-------+---------------------------+---------------------------+
		// | 2004| 1094| 3268115.4991999995|
		// | 2005| 3828| 1.325756415E7|
		// | 2006| 3772| 1.3680982900000002E7|
		// | 2007| 4885| 1.6719354559999995E7|
		// | 2008| 4861| 1.4674295300000004E7|
		// | 2009| 2619| 6323697.189999997|
		// | 2010| 94| 210949.65999999995|
		// +-------+---------------------------+---------------------------+

		// TODO 所有订单中季度销售额前10位

		// sparkSession.sql("select c.theyear, c.thequot,sum(b.amount) as sumofamount from tblStock a, tblStockDetail b,tbldate c  where a.ordernumber=b.ordernumber and a.dateID=c.dateID group by c.theyear,c.thequot order by sumofamount desc ,c.theyear, c.thequot limit 10").show();
		// +-------+-------+------------------+
		// |theyear|thequot| sumofamount|
		// +-------+-------+------------------+
		// | 2008| 1| 5253757.530000003|
		// | 2007| 4| 4615041.960000002|
		// | 2007| 1| 4447827.399999999|
		// | 2006| 1| 3920100.530000003|
		// | 2008| 2| 3888346.21|
		// | 2007| 3| 3871545.770000001|
		// | 2007| 2| 3784939.43|
		// | 2006| 4|3693956.3400000003|
		// | 2005| 1| 3595189.900000002|
		// | 2005| 3|3306439.8400000012|
		// +-------+-------+------------------+

		// TODO 列出销售金额在100000以上的单据
		// sparkSession.sql("select a.ordernumber,sum(b.amount) as cal_amount from tblStock a, tblStockDetail b where a.ordernumber=b.ordernumber group by a.ordernumber having  cal_amount>100000").show();
		// +-------------+------------------+
		// | ordernumber| cal_amount|
		// +-------------+------------------+
		// |HMJSL00009958| 159126.0|
		// |HMJSL00009024|119084.80000000008|
		// +-------------+------------------+
		// TODO 所有订单每年最大金额订单的销售额
	}

	/**
	 * 读取文件，转为Dataset
	 * 
	 * @param sparkSession
	 * @param filePath
	 *            文件路劲
	 * @param schemaString
	 *            字段分隔符是逗号
	 * @param fileSplit
	 *            文件字段分隔符
	 * @return
	 */
	private static Dataset<Row> rddToDateSet(SparkSession sparkSession, String filePath, String schemaString, String fileSplit) {
		List<StructField> fields = new ArrayList<StructField>(16);
		for (String fieldName : schemaString.split(",")) {
			StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
			fields.add(field);
		}
		StructType schema = DataTypes.createStructType(fields);
		JavaRDD<Row> rowRDD = sparkSession.sparkContext().textFile(filePath, 1).toJavaRDD().map(new Function<String, Row>() {
			@Override
			public Row call(String record) throws Exception {
				return RowFactory.create(record.split(fileSplit));
			}
		});
		return sparkSession.createDataFrame(rowRDD, schema);
	}

	/**
	 * 读取txt文件为DataSet
	 * 
	 * @param string
	 */
	private static Dataset<Row> readFileToDS(String path) {
		Dataset<Row> dataset = sparkSession.read().text(path);
		return dataset;
	}

}
