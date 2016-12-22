package com.bl.bigdata.category

import com.bl.bigdata.util.SparkFactory
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
 * Created by HJT20 on 2016/9/8.
 */
class CategoryYhdPrice {

  def yhdPrice(): Unit ={
    val sc = SparkFactory.getSparkContext("category_match")
/*    val url = "jdbc:mysql://10.201.129.74:3306/recommend_system?user=root&password=bl.com"
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)*/
    val hiveContext = new HiveContext(sc)
    val yhdItemDf = hiveContext.sql("SELECT * FROM category.yhd_items WHERE seller LIKE '%1号店自营%' ")

    val yhdCateItems = yhdItemDf.map(row=>{
      val cateUrl =row.getString(1)
      (cateUrl,row)
    })

    val blYhdCate = hiveContext.sql("select * from category.bl_category_performance_basic")
    blYhdCate.registerTempTable("blYhdCateTbl")
    val matCate = hiveContext.sql("select category_sid,yhd_category_url from blYhdCateTbl where yhd_category_url is not null")
      matCate.registerTempTable("matCateTbl")
    val blPriceDf = hiveContext.sql("select * from category.bl_category_performance_category_price_conf")
    blPriceDf.registerTempTable("bl_price_tbl")
    val yhdCateBLPriceDf = hiveContext.sql("select * from bl_price_tbl,matCateTbl where bl_price_tbl.category_sid = matCateTbl.category_sid and type=0").distinct

    val yhdCateBlPriceRdd = yhdCateBLPriceDf.map(row=>{
      val yhdCateUrl  = row.getString(7)
      (yhdCateUrl,row)
    })

    val blPYhdIRdd = yhdCateBlPriceRdd.join(yhdCateItems)
    val temRdd = blPYhdIRdd.mapValues{case(price,yhdItem)=>{
     val low_price =  price.getDouble(2)
      val high_price = price.getDouble(3)
      val yhdItemPrice = yhdItem.getDouble(4)
      val yhdSaleSum = yhdItem.getInt(5)
      var lp = 0
      var ss = 0
      if(low_price <= yhdItemPrice && yhdItemPrice < high_price )
        {
          lp = 1
          ss = yhdSaleSum
        }
      (price,(lp,ss))

    }
    }.map(x=>
    {
      ((x._1,x._2._1),x._2._2)
    }).reduceByKey((a,b)=>(a._1 + b._1,a._2+b._2))

    import hiveContext.implicits._
    val outRdd = temRdd.map(x=>
    {
      val yhd_url = x._1._1
      val row = x._1._2
      val yhd_tup = x._2
      val category_sid = row.getInt(0)
      val range_no =  row.getInt(1)
      val low_price = row.getDouble(2)
      val high_price = row.getDouble(3)
      val p_type = row.getInt(4)
      val sale_sum = row.getInt(5)
      val goods_sum = yhd_tup._1
      val comt_sum = yhd_tup._2
      YhdPriceDist(category_sid,range_no,low_price,high_price,p_type,sale_sum,yhd_url,goods_sum,comt_sum)
    }).toDF().registerTempTable("tmp")
    hiveContext.sql("insert overwrite table category.bl_category_performance_category_yhd_price_dist select * from tmp")

    /*val outSchema = StructType(
      StructField("category_sid", IntegerType)
        :: StructField("range_no", org.apache.spark.sql.types.IntegerType)
        :: StructField("low_price", org.apache.spark.sql.types.DoubleType)
        :: StructField("high_price", org.apache.spark.sql.types.DoubleType)
        :: StructField("type", org.apache.spark.sql.types.IntegerType)
        :: StructField("sale_sum", org.apache.spark.sql.types.IntegerType)
        :: StructField("yhd_cate_url", org.apache.spark.sql.types.StringType)
        :: StructField("goods_sum", org.apache.spark.sql.types.IntegerType)
        :: StructField("comt_sum", org.apache.spark.sql.types.IntegerType)
        :: Nil)

    sqlContext.createDataFrame(outRdd,outSchema).insertIntoJDBC(url,"bl_category_performance_category_yhd_price_dist",true)*/


  }
}

object CategoryYhdPrice {
  def main(args: Array[String]): Unit = {
    val cyp = new CategoryYhdPrice
    cyp.yhdPrice()
  }
}

case class YhdPriceDist(category_sid: Int,range_no: Int,low_price: Double,high_price: Double, p_type: Int,sale_sum: Int, yhd_url: String,goods_sum: Int,comt_sum: Int)
