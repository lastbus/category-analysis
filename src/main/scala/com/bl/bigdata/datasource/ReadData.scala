package com.bl.bigdata.datasource

import com.bl.bigdata.util.SparkFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by MK33 on 2016/4/8.
  */
object ReadData {


  def readHive(sc: SparkContext, sql: String): RDD[Array[String]] = {
    val hiveContext = SparkFactory.getHiveContext
    hiveContext.sql(sql).rdd.map(row => if (row.anyNull) null else row.toSeq.map(_.toString).toArray)
      .filter(_ != null)
  }

  def readLocal(sc: SparkContext, path: String, delimiter: String = "\t"): RDD[Array[String]] = {
    sc.textFile(path).map(line => line.split(delimiter))
  }

  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("test"))
    val readIn = readLocal(sc, "D:\\2\\dim_category")
    readIn.map{ case Array(one, two, three, four, five, six, seven, eight) =>
      (one, two, three, four, five, six, seven, eight)}
      .collect().foreach(println)
  }

}
