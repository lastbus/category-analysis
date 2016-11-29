package com.bl.bigdata.category

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.bl.bigdata.util.SparkFactory
import com.rockymadden.stringmetric.similarity.JaccardMetric
import org.apache.spark.sql.hive.HiveContext


/**
 * Created by HJT20 on 2016/9/8.
 */
class CategoryYhdItemMatch {
  def itemmatch(): Unit = {
    val sc = SparkFactory.getSparkContext("category_match")
    /*val url = "jdbc:mysql://10.201.129.74:3306/recommend_system?user=root&password=bl.com"
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)*/
    val hiveContext = new HiveContext(sc)
    val yhdItemDf = hiveContext.sql("select * from category.yhd_items")
    val topYhdItems = yhdItemDf.map(row => {
      val comment = row.getInt(5)
      val cateUrl = row.getString(1)
      (cateUrl, Seq((comment, row)))
    }).reduceByKey(_ ++ _).mapValues(x => x.sortWith((a, b) => a._1 > b._1)).mapValues(x => x.take(50)).mapValues(x => {
      x.map { case (cmt, row) => row
      }
    }).flatMapValues(x => x).distinct()

    //yhd brands
    val yhdBrandDf = hiveContext.sql("select * from category.YHD_CATEGORY")
    val yhdBrandRdd = yhdBrandDf.map(row => {
      (row.getString(2), row.getString(5))
    })

    val yhdBrdItem = topYhdItems.join(yhdBrandRdd)

    val blYhdCate = hiveContext.sql("select * from category.bl_category_performance_basic")
    blYhdCate.registerTempTable("blYhdCateTbl")
    val matCate = hiveContext.sql("select category_sid,yhd_category_url from blYhdCateTbl where yhd_category_url is not null")
    val byMateCateRdd = matCate.map(row => (row.getInt(0), row.getString(1)))

    val BLGoodsSql = "SELECT DISTINCT category_id,sid,goods_sales_name,cn_name,sale_price FROM recommendation.goods_avaialbe_for_sale_channel WHERE sale_status=4 and category_id is not null"
    val BLGoodsRdd = hiveContext.sql(BLGoodsSql).rdd.map(row => (row.getLong(0).toInt, row))

    val blYhdRdd = byMateCateRdd.join(BLGoodsRdd).map(x => {
      val blcateId = x._1
      val yhdUrl = x._2._1
      val blgoods = x._2._2
      (yhdUrl, (blcateId, blgoods))
    }).join(yhdBrdItem)

    import hiveContext.implicits._
    val mmd = blYhdRdd.map(x => {
      val yhdCUrl = x._1
      val yhdItem = x._2._2._1
      val yhdBrands = x._2._2._2.toLowerCase
      val yhdName = yhdItem.getString(3).toLowerCase
      val yhdItemUrl = yhdItem.getString(2)
      val yhdItemPrice = yhdItem.getDouble(4)

      val blCateId = x._2._1._1
      val blgoods = x._2._1._2
      val blGoodsId = blgoods.getString(1)
      val blGoodsName = blgoods.getString(2)
      val blBrandTmp = blgoods.getString(3)
      var blBrand = ""
      if ("".equalsIgnoreCase(blBrandTmp)){
         blBrand = blgoods.getString(3).toLowerCase
      }else {
         blBrand = blBrandTmp
      }
      val blGoodsPrice = blgoods.getDouble(4)
      val pdif = Math.abs((yhdItemPrice - blGoodsPrice) / blGoodsPrice)

      if (blBrand != null && yhdName != null && yhdName.contains(blBrand) && pdif < 0.3) {
        val regEx1 = "[\\u4e00-\\u9fa5||()||（）||/]"
        val regEx2 = "[a-z||A-Z||0-9||/||\\s+]"
        val yhdBrand = yhdBrands.split("#").filter(_.contains(blBrand))
        //去除品牌相关
        var tmpYhdName = yhdName
        var tmpBlName = blGoodsName
        tmpBlName = blGoodsName.replaceAll(blBrand, "")
        if (yhdBrand.size > 0) {
          val barr = yhdBrand(0).split("/")
          tmpYhdName = yhdName.replaceAll(barr(0), "")
          if (barr.length == 2) {
            tmpYhdName = yhdName.replaceAll(barr(0), "")
            tmpYhdName = tmpYhdName.replaceAll(barr(1), "")
            tmpBlName = tmpBlName.replaceAll(barr(1), "")
          }
        }
        val tmpYhdWords = tmpYhdName.replaceAll(regEx2, "")
        val tmpBlWords = tmpBlName.replaceAll(regEx2, "")

        val tmpYhdad = yhdName.replaceAll(regEx1, " ").toLowerCase
        val tmpBlad = blGoodsName.replaceAll(regEx1, " ").toLowerCase
        var label = new Array[Int](tmpBlWords.size)
        var index = 0
        tmpBlWords.foreach(ch => {
          if (tmpYhdWords.contains(ch)) {
            val index2 = tmpYhdWords.indexOf(ch)
            label.update(index, index2)
          }
          index += 1
        })
        var wc = 0.0
        var tmpwc = 0.0
        for (i <- 0 to (label.size - 2)) {
          if (label(i + 1) - label(i) == 1) {
            if(tmpwc>1)
            {
              tmpwc += 2.0
            }
            else
            {
              tmpwc += 1.0
            }

          }
          else {
            wc = wc + tmpwc
            tmpwc = 0.0
          }
        }


        var ts = 0.0
        if(tmpBlWords!=null && tmpBlWords.size>0)
          ts = ts + (wc / tmpBlWords.length)
        else
          ts = ts + wc/10.0
        var strc = 0.0
        if (tmpBlad != null && tmpYhdad != null) {
          val adArray1 = tmpBlad.split("\\s+")
          val adArray2 = tmpYhdad.split("\\s+")
          adArray1.foreach(str => {
            if (adArray2.contains(str)) {
              strc += 2.0
            }
          })
        }
        ts = ts + strc
        ((yhdCUrl, yhdItemUrl, yhdName), Seq((blCateId, blGoodsId, blGoodsName, ts)))
      } else null
    }).filter(_ != null).reduceByKey(_ ++ _).mapValues(x => x.sortWith((a, b) =>  a._4 > b._4)).mapValues(x => x.take(1))
      .map{data => (data._2(0)._1, data._2(0)._2.toInt,data._2(0)._3, data._1._1, data._1._2, data._1._3,data._2(0)._4)}
      .map{case(bl_goods_category,bl_goods_sid,bl_goods_name,yhd_category_url,yhd_goods_url,yhd_goods_name,degree) =>
        BlYhdItems(bl_goods_category,bl_goods_sid,bl_goods_name,yhd_category_url,yhd_goods_url,yhd_goods_name,degree)
      }.toDF().registerTempTable("tmp")
      hiveContext.sql("insert overwrite table category.bl_yhd_items select * from tmp")


 /*   var conn: Connection = null
    var ps: PreparedStatement = null
    val usql  = "insert into bl_yhd_items(bl_goods_category,bl_goods_sid,bl_goods_name,yhd_category_url,yhd_goods_url," +
      "yhd_goods_name,degree) values (?,?,?,?,?,?,?)"
    try {
      mmd.foreachPartition(partition => {
        val driver = "com.mysql.jdbc.Driver"
        Class.forName(driver)
        conn = DriverManager.getConnection("jdbc:mysql://10.201.129.74:3306/recommend_system", "root", "bl.com")
        partition.foreach { data =>
          ps = conn.prepareStatement(usql)
          ps.setInt(1,data._2(0)._1)
          ps.setInt(2,data._2(0)._2.toInt)
          ps.setString(3,data._2(0)._3)
          ps.setString(4,data._1._1)
          ps.setString(5,data._1._2)
          ps.setString(6,data._1._3)
          ps.setDouble(7,data._2(0)._4)
          ps.executeUpdate()
        }
      }
      )
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }*/



  }
}

object CategoryYhdItemMatch {
  def main(args: Array[String]) {
    val cym = new CategoryYhdItemMatch
    cym.itemmatch()
  }
}

case class  BlYhdItems(bl_goods_category: Int,bl_goods_sid: Int,bl_goods_name: String,yhd_category_url: String,yhd_goods_url: String,yhd_goods_name: String,degree: Double)
