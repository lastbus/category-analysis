package com.bl.bigdata.category

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.bl.bigdata.util.SparkFactory
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{Row, SQLContext}

/**
 * Created by HJT20 on 2016/8/30.
 */
class CategoryGoodsDist {

  def cateDist(): Unit ={
    val sc = SparkFactory.getSparkContext("CategoryGoodsDist")
    /*val url = "jdbc:mysql://10.201.129.74:3306/recommend_system?user=root&password=bl.com"
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val df = sqlContext.jdbc(url, "bl_category_performance_category_monthly_sales")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)*/
    val hiveContext = new HiveContext(sc)
    val df = hiveContext.sql("select category_sid, category_name, month, sales, sales_amount, sales_distinct_goods_amount, " +
      "fifty_percent_cnr, eighty_percent_cnr, shelf_sales_ratio, cdate from category.bl_category_performance_category_monthly_sales")
    val dataframe = df.rdd.map(row => {
      var v1 = 0.0
      var v2 = 0.0
      if (!row.get(7).isInstanceOf[Double]) {
        v1 = 0.0
      }
      else {
        v1 = row.getDouble(7)
      }
      if (!row.get(8).isInstanceOf[Double]) {
        v2 = 0.0
      }
      else {
        v2 = row.getDouble(8)
      }
      (row.getInt(0), row.getString(1), row.getString(2), v1, v2)
    })
      .map{case(category_sid, category_name, month, eighty_percent_cnr, shelf_sales_ratio) =>
        (category_sid, category_name, month, eighty_percent_cnr, shelf_sales_ratio)
      }
/*
    val schema = StructType(StructField("category_sid", IntegerType) :: StructField("category_name", StringType)
      :: StructField("month", StringType) :: StructField("eighty_percent_cnr", org.apache.spark.sql.types.DoubleType)
      :: StructField("shelf_sales_ratio", org.apache.spark.sql.types.DoubleType) :: Nil)
    val tmpT = sqlContext.createDataFrame(dataframe, schema).registerTempTable("tmp_category_conf")*/
    import hiveContext.implicits._
    val tmpT = df.toDF().registerTempTable("tmp_category_conf")

    val dateFormat = new SimpleDateFormat("yyyy-MM")
    val cal = Calendar.getInstance()
    cal.add(Calendar.MONTH, -4)
    val endCal = Calendar.getInstance()
    val startMonth = dateFormat.format(cal.getTime)
    val endMonth = dateFormat.format(endCal.getTime)

    //产品线配置
/*    val bl_category_performance_conf = sqlContext.jdbc(url, "bl_category_performance_conf")
    val bl_category_performance_conf_detail = sqlContext.jdbc(url, "bl_category_performance_conf_detail")
    bl_category_performance_conf.registerTempTable("bl_category_performance_conf")
    bl_category_performance_conf_detail.registerTempTable("bl_category_performance_conf_detail")*/

    val conf = hiveContext.sql("select c.components, cd.sub_comp,c.weight*cd.weight weight from category.bl_category_performance_conf c join category.bl_category_performance_conf_detail cd on c.components=cd.components")
    val epc_weight = conf.rdd.filter(row => row.getString(1).equals("eighty_percent_cnr")).first().getDouble(2)
    val ssr_weight = conf.rdd.filter(row => row.getString(1).equals("shelf_sales_ratio")).first().getDouble(2)

    val bc_epc_weight = sc.broadcast(epc_weight)
    val bc_ssr_weight = sc.broadcast(ssr_weight)

   val res = hiveContext.sql(s"select category_sid,category_name,AVG(eighty_percent_cnr),avg(shelf_sales_ratio) from tmp_category_conf where month>'$startMonth' group by category_sid,category_name")
    val prodLineRdd = res.map(row => {
      val avg_epc = row.getDouble(2)
      val avg_ssr = row.getDouble(3)
      var avg_epc_score = 0.0
      //80%销售
      if (avg_epc < 0.2 || avg_epc > 0.8) {
        avg_epc_score = 0.2 * bc_epc_weight.value
      }
      else if ((0.4 < avg_epc && avg_epc <= 0.8)) {
        avg_epc_score = 0.5 * bc_epc_weight.value
      }
      else if (0.2 <= avg_epc && avg_epc <= 0.4) {
        avg_epc_score = 1 * bc_epc_weight.value
      }

      //动销比
      var avg_ssr_score = 0.0
      if (avg_ssr < 0.2 || avg_ssr > 0.8) {
        avg_ssr_score = 0.2 * bc_ssr_weight.value
      }
      else if ((0.2 <= avg_ssr && avg_ssr <= 0.4) || (0.6 <= avg_ssr && avg_ssr <= 0.8)) {
        avg_ssr_score = 0.5 * bc_ssr_weight.value
      }
      else if (0.4 < avg_ssr && avg_ssr < 0.6) {
        avg_ssr_score = 1 * bc_ssr_weight.value
      }
      (row.getInt(0), (row.getString(1), row.getDouble(2), avg_epc_score, row.getDouble(3), avg_ssr_score))
    })


    //价格分布
    val pcor_weight = conf.rdd.filter(row => row.getString(1).equals("price_correlation")).first().getDouble(2)
    val bc_pcor_weight = sc.broadcast(pcor_weight)

    val person = (sp: Seq[(Double, Double)]) => {
      if (sp.isEmpty) 0.0
      else {
        val n = sp.length
        val x = sp.foldLeft((0.0, 0.0, 0.0, 0.0, 0.0))((a: (Double, Double, Double, Double, Double), b: (Double, Double)) =>
          (a._1 + b._1 * b._2, a._2 + b._1, a._3 + b._2, a._4 + b._1 * b._1, a._5 + b._2 * b._2))
        val numerator = n * x._1 - x._2 * x._3
        val denominator = (n * x._4 - x._2 * x._2) * (n * x._5 - x._3 * x._3)
        if (denominator == 0) 0.0 else numerator / Math.sqrt(denominator)
      }

    }


    val bl_category_performance_category_price_conf = hiveContext.sql(
      "select category_sid, range_no, low_price, high_price, type, sale_sum from  category.bl_category_performance_category_price_conf")

    val sumRdd = bl_category_performance_category_price_conf.distinct.map(row => {
      val category_sid = row.getInt(0)
      val range_no = row.getInt(1)
      val p_type = row.getInt(4)
      val sale_sum = row.getInt(5)
      ((category_sid, p_type), sale_sum)
    }).reduceByKey(_ + _)


    val rawSaleSumRdd = bl_category_performance_category_price_conf.distinct.map(row => {
      val category_sid = row.getInt(0)
      val range_no = row.getInt(1)
      val p_type = row.getInt(4)
      val sale_sum = row.getInt(5)
      ((category_sid, p_type), (range_no, sale_sum))
    })

    val normRdd =  rawSaleSumRdd.join(sumRdd).map(x=>{
      val category_sid = x._1._1
      val p_type = x._1._2
      val range_no = x._2._1._1
      val sale_sum = x._2._1._2
      val total = x._2._2
      ((category_sid, range_no), Seq((p_type, sale_sum*1.0/total)))
    }).reduceByKey(_ ++ _).mapValues(x => {
      if (x.size == 1 && x(0)._1 == 0) {
        Seq((x(0)._2, 0.0))
      }
      else if (x.size == 1 && x(0)._1 == 1) {
        Seq((0.0, x(0)._2))
      }
      else {
        val sx = x.sortWith((a, b) => a._1 < b._1)
        Seq((sx(0)._2, sx(1)._2))
      }
    }).map(x => {
      (x._1._1, x._2)
    }).reduceByKey(_ ++ _).mapValues(sp => {
      if (person(sp).isNaN || person(sp).isInfinite || person(sp) < 0.0 || person(sp) > 1.0) {
        (0.0, 0.0)
      }
      else {
        (person(sp), bc_pcor_weight.value * person(sp))
      }
    })

    import hiveContext.implicits._
    val outRdd = normRdd.leftOuterJoin(prodLineRdd)
      .map(x => {
        val cid = x._1
        val ss = x._2._1
        val pl = x._2._2
        if (pl.isEmpty) {
          (cid, "", 0.0, 0.0, 0.0, 0.0, ss._1, ss._2, (ss._2))
        }
        else {
          (cid, pl.get._1, pl.get._2, pl.get._3, pl.get._4, pl.get._5, ss._1, ss._2, (pl.get._3 + pl.get._5 + ss._2))
        }

      })
      .map{
        case(category_sid, category_name, avg_eighty_percent_cnr, eighty_percent_cnr_score, shelf_sales_ratio,
        shelf_sales_ratio_score, price_correlation,price_correlation_score, org_score) =>
          CategoryPerformanceProductLineScore(category_sid, category_name, avg_eighty_percent_cnr, eighty_percent_cnr_score, shelf_sales_ratio,
            shelf_sales_ratio_score, price_correlation,price_correlation_score, org_score)
      }.toDF().registerTempTable("lineScoreTable")
    hiveContext.sql("insert overwrite table category.bl_category_performance_product_line_score select * from lineScoreTable")

   /* val plSchema = StructType(StructField("category_sid", IntegerType)
      :: StructField("category_name", StringType)
      :: StructField("avg_eighty_percent_cnr", org.apache.spark.sql.types.DoubleType)
      :: StructField("eighty_percent_cnr_score", org.apache.spark.sql.types.DoubleType)
      :: StructField("shelf_sales_ratio", org.apache.spark.sql.types.DoubleType)
      :: StructField("shelf_sales_ratio_score", org.apache.spark.sql.types.DoubleType)
      :: StructField("price_correlation", org.apache.spark.sql.types.DoubleType)
      :: StructField("price_correlation_score", org.apache.spark.sql.types.DoubleType)
      :: StructField("org_score", org.apache.spark.sql.types.DoubleType)
      :: Nil)

    sqlContext.createDataFrame(outRdd, plSchema).insertIntoJDBC(url, "bl_category_performance_product_line_score", true)*/

  }

}

object CategoryGoodsDist {
  def main(args: Array[String]): Unit = {
    val cgd = new CategoryGoodsDist
    cgd.cateDist()
  }
}

case class CategoryPerformanceProductLineScore(category_sid: Int , category_name: String, avg_eighty_percent_cnr: Double, eighty_percent_cnr_score: Double, shelf_sales_ratio: Double, shelf_sales_ratio_score: Double, price_correlation: Double,price_correlation_score: Double, org_score: Double)