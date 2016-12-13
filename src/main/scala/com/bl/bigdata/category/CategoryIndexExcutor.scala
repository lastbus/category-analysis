package com.bl.bigdata.category

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by YQ85 on 2016/12/13.
  */
class categoryIndexExcutor {

}

object categoryIndexExcutor {
  def main(args : Array[String]): Unit = {
    val no = args(0).toInt
    val sparkConf = new SparkConf().setAppName("categoryIndex")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    no match {
      case 0 =>
        //全部
        //人气相关
        println("category_performance_month_sale_detail")
        categoryIndex.categorySale(hiveContext)
        //sku相关
        println("category_performance_month_sku")
        categoryIndex.categorySku(hiveContext)
        //品牌相关
        println("category_performance_month_brand")
        categoryIndex.categoryBrand(hiveContext)
        //销售相关
        println("category_performance_month_sale_detail")
        categoryIndex.categorySale(hiveContext)
      case 1 =>
        //人气相关
        println("category_performance_month_sale_detail")
        categoryIndex.categorySale(hiveContext)
      case 2 =>
        //sku相关
        println("category_performance_month_sku")
        categoryIndex.categorySku(hiveContext)
      case 3 =>
        //品牌相关
        println("category_performance_month_brand")
        categoryIndex.categoryBrand(hiveContext)
      case 4 =>
        //销售相关
        println("category_performance_month_sale_detail")
        categoryIndex.categorySale(hiveContext)
    }
  }
}
