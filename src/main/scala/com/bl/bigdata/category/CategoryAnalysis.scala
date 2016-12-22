package com.bl.bigdata.category

import com.bl.bigdata.util.SparkFactory
import org.apache.spark.sql.hive.HiveContext


/**
 * Created by HJT20 on 2016/8/10.
 */
class CategoryAnalysis {

  def categeryAnalysis(): Unit = {
    val sc = SparkFactory.getSparkContext("categoryOperation")
   /* val url = "jdbc:mysql://10.201.129.74:3306/recommend_system?user=root&password=bl.com"
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val cateTreeDf = sqlContext.jdbc(url, "dim_management_category_tree")*/
    val hiveContext = new HiveContext(sc)
    val cateTreeDf = hiveContext.sql("select * from category.dim_management_category_tree")
    val cateTreeRdd = cateTreeDf.rdd.map(row=>{
      var parent_sid= 0L
      if(!row.get(1).toString.equalsIgnoreCase("null"))
        {
          parent_sid = row.getString(1).toInt
        }
      (row.getString(0).toInt,(parent_sid.toInt,row.getLong(2).toInt,row.getString(3)))
    })


    val saleDf = hiveContext.sql("select * from category.bl_category_performance_category_sale_score")
    val saleScoreRdd = saleDf.map(row=>{
      (row.getInt(0),Map(1 -> row.getDouble(13)))
    })


    val productDf = hiveContext.sql("select * from category.bl_category_performance_product_line_score")
    val productRdd = productDf.map(row=>{
      (row.getInt(0),Map(2 -> row.getDouble(8)))
    })


    val popDf = hiveContext.sql("select * from category.bl_category_performance_category_popularity_score")
    val popRdd = popDf.map(row=>{
      (row.getInt(0),Map(3 -> row.getDouble(16)))
    })


    val operDf = hiveContext.sql("select * from category.bl_category_performance_category_operation_score")
    val operRdd = operDf.map(row=>{
      (row.getInt(0),Map(4 -> row.getDouble(13)))
    })

   val cateScoreRdd =  saleScoreRdd.union(productRdd).union(popRdd).union(operRdd).reduceByKey(_  ++ _)

    val outScoreRdd = cateTreeRdd.join(cateScoreRdd).map(x=>{
      val category_sid = x._1
      val cate_info = x._2._1
      val parent_sid = cate_info._1
      val level = cate_info._2
      val category_name = cate_info._3

      val cate_score = x._2._2

      if(!cate_score.isEmpty) {
        var sales_volume = 0.0
        if (!cate_score.get(1).isEmpty) {
          sales_volume = cate_score.get(1).get
        }

        var configuration = 0.0
        if (!cate_score.get(2).isEmpty) {
          configuration = cate_score.get(2).get
        }

        var drainage = 0.0
        if (!cate_score.get(3).isEmpty) {
          drainage = cate_score.get(3).get
        }

        var operation = 0.0
        if (!cate_score.get(4).isEmpty) {
          operation = cate_score.get(4).get
        }

        val category_score = sales_volume + configuration + operation + drainage
        (category_sid, category_name, parent_sid, level, sales_volume, drainage, configuration, operation, category_score)
      }
      else
        {
          (category_sid, category_name, parent_sid, level, 0.0,  0.0,  0.0,  0.0,  0.0)

        }
    })
/*

   val sortArray =  outScoreRdd.map(x=>{
      x._9.toInt
    }).collect().distinct.sortWith(_ > _)
*/

  /*  val bc_sortArray = sc.broadcast(sortArray)*/

    import hiveContext.implicits._
    /*val rankOutRdd = outScoreRdd.map{case(category_sid,category_name,parent_sid,level,sales_volume,drainage,configuration,operation,category_score)=>{
      val sa = bc_sortArray.value
      (category_sid.toInt,category_name,parent_sid.toInt,level.toInt,sa.indexOf(category_score.toInt),0,sales_volume,drainage,configuration,operation,category_score)
    }}*/
    val result = outScoreRdd.map(s => (s._4, Seq((s._1, s._2, s._3, s._4,  s._5, s._6, s._7, s._8, s._9)))).reduceByKey(_ ++ _).
      map(s => (s._1, s._2.sortWith((x, y) => x._9 > y._9).zipWithIndex))
      .flatMap(s => (s._2.map(s => (s._1._1.toInt, s._1._2, s._1._3.toInt, s._1._4.toInt, s._2, 0, s._1._5, s._1._6, s._1._7, s._1._8, s._1._9))))
      .map{case (category_sid, category_name, parent_sid, level, rank, ranking_change, sales_volume, drainage, configuration, operation, performance) =>
        CategoryPerformanceScore(category_sid, category_name, parent_sid, level, rank, ranking_change, sales_volume, drainage, configuration, operation, performance)
      }.toDF().registerTempTable("score")
    hiveContext.sql("insert overwrite table category.bl_category_performance_score select * from score")

   /* val scoreSchema = StructType(
      StructField("category_sid", IntegerType)
        :: StructField("category_name", StringType)
        :: StructField("parent_sid", org.apache.spark.sql.types.IntegerType)
        :: StructField("level", org.apache.spark.sql.types.IntegerType)
        :: StructField("rank", org.apache.spark.sql.types.IntegerType)
        :: StructField("ranking_change", org.apache.spark.sql.types.IntegerType)
        :: StructField("sales_volume", org.apache.spark.sql.types.DoubleType)
        :: StructField("drainage", org.apache.spark.sql.types.DoubleType)
        :: StructField("configuration", org.apache.spark.sql.types.DoubleType)
        :: StructField("operation", org.apache.spark.sql.types.DoubleType)
        :: StructField("performance", org.apache.spark.sql.types.DoubleType)
       // :: StructField("cdate", org.apache.spark.sql.types.DateType)
        :: Nil)
    sqlContext.createDataFrame(rankOutRdd,scoreSchema).insertIntoJDBC(url,"bl_category_performance_score",true)
*/
  }


}

object CategoryAnalysis {
  def main(args: Array[String]): Unit = {
    val ca = new CategoryAnalysis
    ca.categeryAnalysis()
  }
}

case class CategoryPerformanceScore(category_sid: Int, category_name: String, parent_sid: Int, level: Int, rank: Int, ranking_change: Int, sales_volume: Double, drainage: Double, configuration: Double, operation: Double, performance: Double)

