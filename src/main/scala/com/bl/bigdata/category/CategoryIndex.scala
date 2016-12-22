package com.bl.bigdata.category

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by YQ85 on 2016/12/6.
  */
object categoryIndex {
  def main(args : Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("categoryIndex")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)

    categoryPopularity(hiveContext)
    categorySku(hiveContext)
    categoryBrand(hiveContext)
    categorySale(hiveContext)
  }
  val sdf = new SimpleDateFormat("yyyy-MM-dd")
  val cal = Calendar.getInstance()
  cal.add(Calendar.DATE, -1)
  val now = sdf.format(cal.getTime)
  //人气相关
  def categoryPopularity(hiveContext: HiveContext) : Unit = {
    val goodsTotalSql = "SELECT a.goods_sid, a.com_sid, a.brand_sid, a.mgm_level1_id, a.mgm_level1_name, a.mgm_level2_id, a.mgm_level2_name, " +
      "a.mgm_level3_id, a.mgm_level3_name, a.mgm_level4_id, a.mgm_level4_name, a.mgm_level5_id, a.sale_level5_name FROM mdata.c_goods_info_snap" +
      s" a WHERE to_date(data_date) = '$now' AND online_ind = '1' "
    val goodsTotal = hiveContext.sql(goodsTotalSql).rdd.map(row =>
      (row.getString(0), (row.getString(1), row.getString(2),
        if (row.isNullAt(3)) "-1" else row.getString(3), row.getString(4),
        if (row.isNullAt(5)) "-1" else row.getString(5), row.getString(6),
        if (row.isNullAt(7)) "-1" else row.getString(7), row.getString(8),
        if (row.isNullAt(9)) "-1" else row.getString(9), row.getString(10),
        if (row.isNullAt(11)) "-1" else row.getString(11), row.getString(12))))

    val pvUvSql = "SELECT DISTINCT split(page_id, '_')[2] goods_sid, cookie_id, substring(event_date, 0, 7) month FROM sourcedata.s13_api_page_scan_event WHERE  page_id LIKE '%PC_商品详情页%' OR page_id LIKE 'APP_商品详情页%' OR page_id LIKE '%H5_商品详情页%'"

    //pv和uv
    val pvuv = hiveContext.sql(pvUvSql).rdd.map(row => (
      if (row.isNullAt(0) || row.get(0).toString.equalsIgnoreCase("null")) "0" else row.getString(0),
      row.getString(1), row.getString(2))).filter(s => !s._1.equals("0"))
      .map(s => (s._1, (s._2, s._3))).join(goodsTotal)
      .map(s => (s._1, s._2._1, s._2._2._1, s._2._2._3, s._2._2._4, s._2._2._5, s._2._2._6, s._2._2._7, s._2._2._8, s._2._2._9, s._2._2._10, s._2._2._11, s._2._2._12))
      .map{case (goods_sid, (cookie_id, month),  com_sid, l1, l1Name, l2, l2Name, l3, l3Name, l4, l4Name, l5, l5Name) =>
        Array((l1, l1Name, 1), (l2, l2Name, 2), (l3, l3Name, 3), (l4, l4Name, 4), (l5, l5Name, 5)).filter(!_._1.equals("-1"))
          .map(s => ((s._1, s._2, s._3, com_sid, month), (Seq(cookie_id), Set(cookie_id))))}
        .flatMap(s => s).reduceByKey((x, y) => (x._1 ++ y._1, x._2 ++ y._2))
      .mapValues(s => (s._1.size.toDouble, s._2.size.toDouble))

    val sql = "SELECT a.member_id, a.order_no, a.industry_sid, substring(a.sale_time, 0, 7) month, b.sale_price, b.sale_sum, b.goods_code " +
      "FROM mdata.c_order_snap a INNER JOIN mdata.c_order_detail b ON a.order_no = b.order_no AND b.online_order_ind = '1' WHERE " +
      "a.online_order_ind = '1' AND a.order_valid_ind = '1' AND a.order_va_ind = '1'  AND a.order_type_code <> '25'"

    val saleRdd = hiveContext.sql(sql).rdd.map(row => (
      if (row.isNullAt(0) || row.get(0).toString.equalsIgnoreCase("null")) "0" else row.getString(0),row.getString(1),
      if (row.isNullAt(2) || row.get(2).toString.equalsIgnoreCase("null")) "0" else row.getString(2), row.getString(3),
      if (row.isNullAt(4) || row.get(4).toString.equalsIgnoreCase("null")) 0.0 else row.getDouble(4),
      if (row.isNullAt(5) || row.get(5).toString.equalsIgnoreCase("null")) 0.0 else row.getDouble(5), row.getString(6)))
      .map(s => (s._7, (s._1, s._2, s._3, s._4, s._5, s._6)))

    //order_conversion_rate和buy_conversion_rate
    val rate = saleRdd.join(goodsTotal)
        .map(s => (s._1, s._2._1, s._2._2._3, s._2._2._4, s._2._2._5, s._2._2._6, s._2._2._7, s._2._2._8, s._2._2._9, s._2._2._10, s._2._2._11, s._2._2._12))
      .map{case (goods_sid, (member_id, order_no, com_sid, month, sale_price, sale_sum), l1, l1Name, l2, l2Name, l3, l3Name, l4, l4Name, l5, l5Name) =>
        Array((l1, l1Name, 1), (l2, l2Name, 2), (l3, l3Name, 3), (l4, l4Name, 4), (l5, l5Name, 5)).filter(!_._1.equals("-1"))
          .map(s => ((s._1, s._2, s._3, com_sid, month), (Set(order_no), Set(member_id))))
      }.flatMap(s => s).reduceByKey((x, y) => (x._1 ++ y._1, x._2 ++ y._2))
        .mapValues(s => (s._1.size.toDouble, s._2.size.toDouble))
      .join(pvuv).mapValues(s => (
      if (s._2._2 == 0.0) 0.0 else s._1._1 / s._2._1,
      if (s._2._2 == 0.0) 0.0 else s._1._2 / s._2._1))

    import hiveContext.implicits._
    val result = pvuv.leftOuterJoin(rate)
      .map{case ((category_id, category_name, lev, com_sid, month), ((pv, uv), rate)) =>
        if (rate.isEmpty) {
          Popularity(category_id.toInt, category_name, lev, com_sid, month, pv, uv, 0.0, 0.0)
        } else {
          Popularity(category_id.toInt, category_name, lev, com_sid, month, pv, uv, rate.get._1, rate.get._2)
        }
      }.toDF().registerTempTable("tmp")
    hiveContext.sql("insert overwrite table category.category_performance_month_popularity select * from tmp")
  }
  //sku相关
  def categorySku(hiveContext: HiveContext) : Unit = {
    val goodsTotalSql = "SELECT a.goods_sid, a.com_sid, a.brand_sid, a.mgm_level1_id, a.mgm_level1_name, a.mgm_level2_id, a.mgm_level2_name, " +
      "a.mgm_level3_id, a.mgm_level3_name, a.mgm_level4_id, a.mgm_level4_name, a.mgm_level5_id, a.sale_level5_name FROM mdata.c_goods_info_snap" +
      s" a WHERE to_date(data_date) = '$now' AND online_ind = '1' "
    val goodsTotal = hiveContext.sql(goodsTotalSql).rdd.map(row =>
      (row.getString(0), (row.getString(1), row.getString(2),
        if (row.isNullAt(3)) "-1" else row.getString(3), row.getString(4),
        if (row.isNullAt(5)) "-1" else row.getString(5), row.getString(6),
        if (row.isNullAt(7)) "-1" else row.getString(7), row.getString(8),
        if (row.isNullAt(9)) "-1" else row.getString(9), row.getString(10),
        if (row.isNullAt(11)) "-1" else row.getString(11), row.getString(12))))

    val sql = "SELECT a.member_id, a.order_no, a.industry_sid, substring(a.sale_time, 0, 7) month, b.sale_price, b.sale_sum, b.goods_code " +
      "FROM mdata.c_order_snap a INNER JOIN mdata.c_order_detail b ON a.order_no = b.order_no AND b.online_order_ind = '1' WHERE " +
      "a.online_order_ind = '1' AND a.order_valid_ind = '1' AND a.order_va_ind = '1'  AND a.order_type_code <> '25'"

    val saleRdd = hiveContext.sql(sql).rdd.map(row => (
      if (row.isNullAt(0) || row.get(0).toString.equalsIgnoreCase("null")) "0" else row.getString(0),row.getString(1),
      if (row.isNullAt(2) || row.get(2).toString.equalsIgnoreCase("null")) "0" else row.getString(2), row.getString(3),
      if (row.isNullAt(4) || row.get(4).toString.equalsIgnoreCase("null")) 0.0 else row.getDouble(4),
      if (row.isNullAt(5) || row.get(5).toString.equalsIgnoreCase("null")) 0.0 else row.getDouble(5), row.getString(6)))
      .map(s => (s._7, (s._1, s._2, s._3, s._4, s._5, s._6)))

    val skuSql= "SELECT DISTINCT a.goods_sid, a.sale_status, a.start_dt, a.end_dt, b.sale_stock_sum, b.active_code, b.shop_sid FROM pdata.t02_pcm_chan_sale_h a JOIN pdata.t02_pcm_stock_h b ON a.goods_sid = b.goods_sid "
    val skuRdd = hiveContext.sql(skuSql).rdd.map(row => (row.getString(0), row.getDouble(1), row.getDate(2), row.getDate(3), row.getDouble(4),
      if (row.isNullAt(5) || row.get(5).toString.equalsIgnoreCase("null")) "null" else row.getString(5),
      if (row.isNullAt(6) || row.get(6).toString.equalsIgnoreCase("null")) "null" else row.getString(6)))
    val sdfSku = new SimpleDateFormat("yyyy-MM")

    //sku_for_sale
    val skuForSale = skuRdd.filter(s => s._2 == 4.0 && s._6.equals("null") && s._7.equals("null") && s._5 > 0)
      .map(s => (s._1, s._3, s._4))
      .map(s => (s._1, s._2.toString.substring(0, 7), s._3.toString.substring(0, 7))).distinct()
      .filter(s => s._2.matches("\\d{4}-\\d{2}") && s._3.matches("\\d{4}-\\d{2}"))
      .map{case (goods_sid, start_month, end_month) =>
        import scala.collection.mutable.Set
        val month = Set(start_month)
        if (start_month.equals(end_month) ) {
          month.add(end_month)
        } else if (end_month.equals("3000-12")){
          val start = Calendar.getInstance()
          val end = Calendar.getInstance()
          start.setTime(sdfSku.parse(start_month))
          start.set(start.get(Calendar.YEAR), start.get(Calendar.MONTH), 1)
          end.setTime(sdfSku.parse(sdfSku.format(new Date())))
          end.set(end.get(Calendar.YEAR), end.get(Calendar.MONTH), 2)
          val curr = start
          while (curr.before(end)) {
            month.add(sdfSku.format(curr.getTime()))
            curr.add(Calendar.MONTH, 1)
          }
        } else {
          val start = Calendar.getInstance()
          val end = Calendar.getInstance()
          start.setTime(sdfSku.parse(start_month))
          start.set(start.get(Calendar.YEAR), start.get(Calendar.MONTH), 1)
          end.setTime(sdfSku.parse(end_month))
          end.set(end.get(Calendar.YEAR), end.get(Calendar.MONTH), 2)
          val curr = start
          while (curr.before(end)) {
            month.add(sdfSku.format(curr.getTime()))
            curr.add(Calendar.MONTH, 1)
          }
        }
        val array = month.toArray
        for (i <- 0 until array.size) yield
          (goods_sid, array(i))
      }.flatMap(s => s).distinct().join(goodsTotal)
      .map(s => (s._1, s._2._1, s._2._2._1, s._2._2._3, s._2._2._4, s._2._2._5, s._2._2._6, s._2._2._7, s._2._2._8, s._2._2._9, s._2._2._10, s._2._2._11, s._2._2._12))
      .map{case (goods_sid, month,  com_sid, l1, l1Name, l2, l2Name, l3, l3Name, l4, l4Name, l5, l5Name) =>
        Array((l1, l1Name, 1), (l2, l2Name, 2), (l3, l3Name, 3), (l4, l4Name, 4), (l5, l5Name, 5)).filter(!_._1.equals("-1"))
          .map(s => ((s._1, s._2, s._3, com_sid, month), 1))}
      .flatMap(s => s).reduceByKey(_+_)


    // sku总数
    val skuTotalSql = "select distinct goods_sid, start_dt, end_dt FROM pdata.t02_pcm_chan_sale_h"
    val sku = hiveContext.sql(skuTotalSql).rdd.map(row => (row.getString(0), row.getDate(1), row.getDate(2))).map(s => (s._1, s._2, s._3))
      .map(s => (s._1, s._2.toString.substring(0, 7), s._3.toString.substring(0, 7))).distinct()
      .filter(s => s._2.matches("\\d{4}-\\d{2}") && s._3.matches("\\d{4}-\\d{2}"))
      .map{case (goods_sid, start_month, end_month) =>
        import scala.collection.mutable.Set
        val month = Set(start_month)
        if (start_month.equals(end_month) ) {
          month.add(end_month)
        } else if (end_month.equals("3000-12")){
          val start = Calendar.getInstance()
          val end = Calendar.getInstance()
          start.setTime(sdfSku.parse(start_month))
          start.set(start.get(Calendar.YEAR), start.get(Calendar.MONTH), 1)
          end.setTime(sdfSku.parse(sdfSku.format(new Date())))
          end.set(end.get(Calendar.YEAR), end.get(Calendar.MONTH), 2)
          val curr = start
          while (curr.before(end)) {
            month.add(sdfSku.format(curr.getTime()))
            curr.add(Calendar.MONTH, 1)
          }
        } else {
          val start = Calendar.getInstance()
          val end = Calendar.getInstance()
          start.setTime(sdfSku.parse(start_month))
          start.set(start.get(Calendar.YEAR), start.get(Calendar.MONTH), 1)
          end.setTime(sdfSku.parse(end_month))
          end.set(end.get(Calendar.YEAR), end.get(Calendar.MONTH), 2)
          val curr = start
          while (curr.before(end)) {
            month.add(sdfSku.format(curr.getTime()))
            curr.add(Calendar.MONTH, 1)
          }
        }
        val array = month.toArray
        for (i <- 0 until array.size) yield
          (goods_sid, array(i))
      }.flatMap(s => s).distinct().join(goodsTotal)
      .map(s => (s._1, s._2._1, s._2._2._1, s._2._2._3, s._2._2._4, s._2._2._5, s._2._2._6, s._2._2._7, s._2._2._8, s._2._2._9, s._2._2._10, s._2._2._11, s._2._2._12))
      .map{case (goods_sid, month,  com_sid, l1, l1Name, l2, l2Name, l3, l3Name, l4, l4Name, l5, l5Name) =>
        Array((l1, l1Name, 1), (l2, l2Name, 2), (l3, l3Name, 3), (l4, l4Name, 4), (l5, l5Name, 5)).filter(!_._1.equals("-1"))
          .map(s => ((s._1, s._2, s._3, com_sid, month), 1))}
      .flatMap(s => s).reduceByKey(_+_)

    //eighty_percent_con_ratio
    val eightyPercentConRatio = saleRdd.join(goodsTotal).map(s => (s._1, s._2._1, s._2._2._3, s._2._2._4, s._2._2._5, s._2._2._6, s._2._2._7, s._2._2._8, s._2._2._9, s._2._2._10, s._2._2._11, s._2._2._12))
      .map{case (goods_sid, (member_id, order_no, com_sid, month, sale_price, sale_sum), l1, l1Name, l2, l2Name, l3, l3Name, l4, l4Name, l5, l5Name) =>
        Array((l1, l1Name, 1), (l2, l2Name, 2), (l3, l3Name, 3), (l4, l4Name, 4), (l5, l5Name, 5)).filter(!_._1.equals("-1"))
          .map(s => ((s._1, s._2, s._3, com_sid, month, goods_sid), sale_price * sale_sum))
      }.flatMap(s => s).reduceByKey(_+_)
      .map(s => ((s._1._1, s._1._2, s._1._3, s._1._4, s._1._5), Seq(s._2))).reduceByKey(_++_)
      .map{case ((category_id, category_name, lev, com_sid, month), array) =>
        val a = array.sortWith((x, y) => x >y).toArray
        val total = array.reduce(_+_)
        var sum = 0.0
        var j = 0
        var flag = true
        for (i <- 0 until array.length if flag) {
          sum  = sum + a(i)
          j = j + 1
          if (sum >= 0.8 * total) {
            flag = false
          }
        }
        ((category_id, category_name, lev, com_sid, month), j)
      }.join(sku).mapValues(s => (s._1.toDouble / s._2.toDouble))

    //sku_dynamic_ratio
    val skuDynamicRatio = saleRdd.join(goodsTotal).map(s => (s._1, s._2._1, s._2._2._3, s._2._2._4, s._2._2._5, s._2._2._6, s._2._2._7, s._2._2._8, s._2._2._9, s._2._2._10, s._2._2._11, s._2._2._12))
      .map{case (goods_sid, (member_id, order_no, com_sid, month, sale_price, sale_sum), l1, l1Name, l2, l2Name, l3, l3Name, l4, l4Name, l5, l5Name) =>
        Array((l1, l1Name, 1), (l2, l2Name, 2), (l3, l3Name, 3), (l4, l4Name, 4), (l5, l5Name, 5)).filter(!_._1.equals("-1"))
          .map(s => ((s._1, s._2, s._3, com_sid, month), goods_sid))}
      .flatMap(s => s).distinct()
      .map(s => (s._1, 1)).reduceByKey(_+_)
      .join(sku).map(s => (s._1, s._2._1.toDouble / s._2._2.toDouble))


    //sku_for_sale_dynamic_ratio
    val skuForSaleDynamicRatio = saleRdd.join(goodsTotal).map(s => (s._1, s._2._1, s._2._2._3, s._2._2._4, s._2._2._5, s._2._2._6, s._2._2._7, s._2._2._8, s._2._2._9, s._2._2._10, s._2._2._11, s._2._2._12))
      .map{case (goods_sid, (member_id, order_no, com_sid, month, sale_price, sale_sum), l1, l1Name, l2, l2Name, l3, l3Name, l4, l4Name, l5, l5Name) =>
        Array((l1, l1Name, 1), (l2, l2Name, 2), (l3, l3Name, 3), (l4, l4Name, 4), (l5, l5Name, 5)).filter(!_._1.equals("-1"))
          .map(s => ((s._1, s._2, s._3, com_sid, month), goods_sid))}
      .flatMap(s => s).distinct()
      .map(s => (s._1, 1)).reduceByKey(_+_)
      .join(skuForSale).map(s => (s._1, s._2._1.toDouble / s._2._2.toDouble))

    import hiveContext.implicits._
    val tmp = eightyPercentConRatio.join(skuDynamicRatio).map(s => (s._1, s._2))
    val result = skuForSale.leftOuterJoin(skuForSaleDynamicRatio)
      .map{case ((category_id, category_name, lev, com_sid, month), (sku_sale, dynamicForSaleRatio)) =>
          if (dynamicForSaleRatio.isEmpty) {
            ((category_id, category_name, lev, com_sid, month), (sku_sale, 0.0))
          } else {
            ((category_id, category_name, lev, com_sid, month), (sku_sale, dynamicForSaleRatio.get))
          }
      }.leftOuterJoin(tmp)
      .map{case ((category_id, category_name, lev, com_sid, month), ((sku_sale, dynamicForSaleRatio), ratio)) =>
        if (ratio.isEmpty) {
          Sku(category_id.toInt, category_name, lev, com_sid, month, dynamicForSaleRatio, 0.0, 0.0, sku_sale)
        } else {
          Sku(category_id.toInt, category_name, lev, com_sid, month, dynamicForSaleRatio, ratio.get._2, ratio.get._1, sku_sale)
        }
      }.toDF.registerTempTable("tmp")
    hiveContext.sql("insert overwrite table category.category_performance_month_sku select * from tmp")

  }
  //品牌相关
  def categoryBrand(hiveContext: HiveContext) : Unit = {
    val goodsTotalSql = "SELECT a.goods_sid, a.com_sid, a.brand_sid, a.mgm_level1_id, a.mgm_level1_name, a.mgm_level2_id, a.mgm_level2_name, " +
      "a.mgm_level3_id, a.mgm_level3_name, a.mgm_level4_id, a.mgm_level4_name, a.mgm_level5_id, a.sale_level5_name FROM mdata.c_goods_info_snap" +
      s" a WHERE to_date(data_date) = '$now' AND online_ind = '1' "
    val goodsTotal = hiveContext.sql(goodsTotalSql).rdd.map(row =>
      (row.getString(0), (row.getString(1), row.getString(2),
        if (row.isNullAt(3)) "-1" else row.getString(3), row.getString(4),
        if (row.isNullAt(5)) "-1" else row.getString(5), row.getString(6),
        if (row.isNullAt(7)) "-1" else row.getString(7), row.getString(8),
        if (row.isNullAt(9)) "-1" else row.getString(9), row.getString(10),
        if (row.isNullAt(11)) "-1" else row.getString(11), row.getString(12))))

    val sql = "SELECT a.member_id, a.order_no, a.industry_sid, substring(a.sale_time, 0, 7) month, b.sale_price, b.sale_sum, b.goods_code " +
      "FROM mdata.c_order_snap a INNER JOIN mdata.c_order_detail b ON a.order_no = b.order_no AND b.online_order_ind = '1' WHERE " +
      "a.online_order_ind = '1' AND a.order_valid_ind = '1' AND a.order_va_ind = '1'  AND a.order_type_code <> '25'"

    val saleRdd = hiveContext.sql(sql).rdd.map(row => (
      if (row.isNullAt(0) || row.get(0).toString.equalsIgnoreCase("null")) "0" else row.getString(0),row.getString(1),
      if (row.isNullAt(2) || row.get(2).toString.equalsIgnoreCase("null")) "0" else row.getString(2), row.getString(3),
      if (row.isNullAt(4) || row.get(4).toString.equalsIgnoreCase("null")) 0.0 else row.getDouble(4),
      if (row.isNullAt(5) || row.get(5).toString.equalsIgnoreCase("null")) 0.0 else row.getDouble(5), row.getString(6)))
      .map(s => (s._7, (s._1, s._2, s._3, s._4, s._5, s._6)))

    //brand_amount
    val brandNum = goodsTotal.map{case (goods_sid, (com_sid, brand_sid, l1, l1Name, l2, l2Name, l3, l3Name, l4, l4Name, l5, l5Name)) =>
        Array((l1, l1Name, 1), (l2, l2Name, 2), (l3, l3Name, 3), (l4, l4Name, 4), (l5, l5Name, 5)).filter(_._1 != -1L)
          .map(s => ((s._1, s._2, s._3, com_sid), Set(brand_sid)))}.flatMap(s => s).reduceByKey(_++_)
      .map{case ((category_id, category_name, lev, com_sid), brandArray) => ((category_id, category_name, lev, com_sid), brandArray.size)}

    //brand_salesof_amount
    val brandSale = saleRdd.join(goodsTotal).map(s => (s._1, s._2._1, s._2._2._2, s._2._2._3, s._2._2._4, s._2._2._5, s._2._2._6, s._2._2._7, s._2._2._8, s._2._2._9, s._2._2._10, s._2._2._11, s._2._2._12))
      .map{case (goods_sid, (member_id, order_no, com_sid, month, sale_price, sale_sum), brand_sid, l1, l1Name, l2, l2Name, l3, l3Name, l4, l4Name, l5, l5Name) =>
        Array((l1, l1Name, 1), (l2, l2Name, 2), (l3, l3Name, 3), (l4, l4Name, 4), (l5, l5Name, 5)).filter(!_._1.equals("-1"))
          .map(s => ((s._1, s._2, s._3, com_sid, month), brand_sid))
      }.flatMap(s => s).distinct().map(s => (s._1, 1)).reduceByKey(_+_)

    //brand_dynamic_ratio
    val brandDynamicRatio = brandSale.map{case ((category_id, category_name, lev, com_sid, month), brand_salesof_amount) =>
      ((category_id, category_name, lev, com_sid), (month, brand_salesof_amount))}
      .join(brandNum).map{case ((category_id, category_name, lev, com_sid), ((month, brandSale), brandTotal)) =>
      ((category_id, category_name, lev, com_sid, month),(brandSale.toDouble/brandTotal.toDouble))
    }

    import hiveContext.implicits._
    val tmp = brandSale.join(brandDynamicRatio).map(s => ((s._1._1, s._1._2, s._1._3, s._1._4),(s._1._5, s._2._1, s._2._2)))
    val result = brandNum.leftOuterJoin(tmp).map{case ((category_id, category_name, lev, com_sid), (brand_amount, array)) =>
        if (array.isEmpty) {
          Brand(category_id.toInt, category_name, lev, com_sid, "0000-00", 0.0, brand_amount, 0.0)
        } else {
          Brand(category_id.toInt, category_name, lev, com_sid, array.get._1, array.get._2.toDouble, brand_amount, array.get._3)
        }
    }.toDF().registerTempTable("tmp")
    hiveContext.sql("insert overwrite table category.category_performance_month_brand select * from tmp")
  }
  //周期
  def categoryPeriod(hiveContext: HiveContext) : Unit = {
    val goodsTotalSql = "SELECT a.goods_sid, a.com_sid, a.brand_sid, a.mgm_level1_id, a.mgm_level1_name, a.mgm_level2_id, a.mgm_level2_name, " +
      "a.mgm_level3_id, a.mgm_level3_name, a.mgm_level4_id, a.mgm_level4_name, a.mgm_level5_id, a.sale_level5_name FROM mdata.c_goods_info_snap" +
      s" a WHERE to_date(data_date) = '$now' AND online_ind = '1' "
    val goodsTotal = hiveContext.sql(goodsTotalSql).rdd.map(row =>
      (row.getString(0), (row.getString(1), row.getString(2),
        if (row.isNullAt(3)) "-1" else row.getString(3), row.getString(4),
        if (row.isNullAt(5)) "-1" else row.getString(5), row.getString(6),
        if (row.isNullAt(7)) "-1" else row.getString(7), row.getString(8),
        if (row.isNullAt(9)) "-1" else row.getString(9), row.getString(10),
        if (row.isNullAt(11)) "-1" else row.getString(11), row.getString(12))))

    val sql = "SELECT a.member_id, a.order_no, a.industry_sid, substring(a.sale_time, 0, 7) month, b.sale_price, b.sale_sum, b.goods_code " +
      "FROM mdata.c_order_snap a INNER JOIN mdata.c_order_detail b ON a.order_no = b.order_no AND b.online_order_ind = '1' WHERE " +
      "a.online_order_ind = '1' AND a.order_valid_ind = '1' AND a.order_va_ind = '1'  AND a.order_type_code <> '25'"

    val saleRdd = hiveContext.sql(sql).rdd.map(row => (
      if (row.isNullAt(0) || row.get(0).toString.equalsIgnoreCase("null")) "0" else row.getString(0),row.getString(1),
      if (row.isNullAt(2) || row.get(2).toString.equalsIgnoreCase("null")) "0" else row.getString(2), row.getString(3),
      if (row.isNullAt(4) || row.get(4).toString.equalsIgnoreCase("null")) 0.0 else row.getDouble(4),
      if (row.isNullAt(5) || row.get(5).toString.equalsIgnoreCase("null")) 0.0 else row.getDouble(5), row.getString(6)))
      .map(s => (s._7, (s._1, s._2, s._3, s._4, s._5, s._6)))

   val sqlGoods = "SELECT DISTINCT a.goods_sid, a.sale_status, a.start_dt, a.end_dt, b.sale_stock_sum, b.com_sid, b.start_dt, b.end_dt " +
      "FROM pdata.t02_pcm_chan_sale_h a JOIN (SELECT goods_sid, sale_stock_sum, com_sid, start_dt, end_dt FROM pdata.t02_pcm_stock_h " +
      "WHERE shop_sid IS NULL AND active_code IS NULL AND stock_type = 0)b ON a.goods_sid = b.goods_sid "

    val goodsRdd = hiveContext.sql(sqlGoods).rdd.map(row => (row.getString(0), row.getDouble(1), row.getDate(2), row.getDate(3),
      row.getDouble(4), if (row.isNullAt(5) || row.get(5).toString.equalsIgnoreCase("null")) "0" else row.getString(5),
      row.getDate(6), row.getDate(7))).filter(!_._5.equals("0"))

    val sdfgoods = new SimpleDateFormat("yyyy-MM-dd")

    val goodsSaleDaysRdd = goodsRdd.filter(_._2 == 4.0).map{(s => (s._1, s._3.toString.substring(0, 10), s._4.toString.substring(0, 10)))}.distinct()
      .filter(s => s._2.matches("\\d{4}-\\d{2}-\\d{2}") && s._3.matches("\\d{4}-\\d{2}-\\d{2}"))
      .map{case (goods_sid, start_day, end_day) =>
        val days = mutable.Set(start_day)
        if (end_day.equals("3000-12-31")){
          var start = sdfgoods.parse(start_day)
          val end = sdfgoods.parse(sdfgoods.format(new Date()))
          val c = Calendar.getInstance()
          while (start.getTime <= end.getTime) {
            days.add(sdfgoods.format(start))
            c.setTime(start)
            c.add(Calendar.DATE, 1)
            start = c.getTime
          }
         /* val start = Calendar.getInstance();
          val start_tmp = start_day.split("-")
          start.set(start_tmp(0).toInt, start_tmp(1).toInt, start_tmp(2).toInt)
          val startTime = start.getTimeInMillis

          val end = Calendar.getInstance()
          val end_tmp = sdfgoods.format(new Date()).split("-")
          end.set(end_tmp(0).toInt, end_tmp(1).toInt, end_tmp(2).toInt)
          val endTime = end.getTimeInMillis

          val oneDay = 1000 * 60 * 60 * 24L
          var time = startTime
          while (time <= endTime) {
            days.add(sdfgoods.format(new Date(time)))
            time += oneDay
          }*/

        } else {
          var start = sdfgoods.parse(start_day)
          val end = sdfgoods.parse(end_day)
          val c = Calendar.getInstance()
          while (start.getTime <= end.getTime) {
            days.add(sdfgoods.format(start))
            c.setTime(start)
            c.add(Calendar.DATE, 1)
            start = c.getTime
          }
        }
        val array = days.toArray
        for (i <- 0 until array.size) yield
          (goods_sid, array(i))
      }.flatMap(s => s).distinct().map(s => ((s._1, s._2.substring(0, 7)), 1)).reduceByKey(_+_)

    val goodsSale = saleRdd.join(goodsTotal).map(s => (s._1, s._2._1, s._2._2._3, s._2._2._4, s._2._2._5, s._2._2._6, s._2._2._7, s._2._2._8, s._2._2._9, s._2._2._10, s._2._2._11, s._2._2._12))
      .map{case (goods_sid, (member_id, order_no, com_sid, month, sale_price, sale_sum), l1, l1Name, l2, l2Name, l3, l3Name, l4, l4Name, l5, l5Name) =>
        Array((l1, l1Name, 1), (l2, l2Name, 2), (l3, l3Name, 3), (l4, l4Name, 4), (l5, l5Name, 5)).filter(!_._1.equals("-1"))
          .map(s => ((s._1, s._2, s._3, com_sid, month, goods_sid), sale_sum))
      }.flatMap(s => s).reduceByKey(_+_)
      .map{case ((category_id, category_name, lev, com_sid, month, goods_sid), sale) => ((goods_sid, month), (category_id, category_name, lev, com_sid, sale))}
    //SELECT DISTINCT a.goods_sid, a.sale_status, a.start_dt, a.end_dt, b.sale_stock_sum, b.com_sid, b.start_dt, b.end_dt
    val goodsSaleStock = goodsRdd.filter(_._2 == 4.0).map{(s => (s._1, s._5, s._7.toString.substring(0, 10), s._8.toString.substring(0, 10)))}
      .filter(s => s._3.matches("\\d{4}-\\d{2}-\\d{2}") && s._4.matches("\\d{4}-\\d{2}-\\d{2}"))
      .map{case (goods_sid, stock, start_day, end_day) =>
        val days = mutable.Set(start_day)
        if (end_day.equals("3000-12-31")){
          var start = sdfgoods.parse(start_day)
          val end = sdfgoods.parse(sdfgoods.format(new Date()))
          val c = Calendar.getInstance()
          while (start.getTime <= end.getTime) {
            days.add(sdfgoods.format(start))
            c.setTime(start)
            c.add(Calendar.DATE, 1)
            start = c.getTime
          }
          /* val start = Calendar.getInstance();
           val start_tmp = start_day.split("-")
           start.set(start_tmp(0).toInt, start_tmp(1).toInt, start_tmp(2).toInt)
           val startTime = start.getTimeInMillis

           val end = Calendar.getInstance()
           val end_tmp = sdfgoods.format(new Date()).split("-")
           end.set(end_tmp(0).toInt, end_tmp(1).toInt, end_tmp(2).toInt)
           val endTime = end.getTimeInMillis

           val oneDay = 1000 * 60 * 60 * 24L
           var time = startTime
           while (time <= endTime) {
             days.add(sdfgoods.format(new Date(time)))
             time += oneDay
           }*/

        } else {
          var start = sdfgoods.parse(start_day)
          val end = sdfgoods.parse(end_day)
          val c = Calendar.getInstance()
          while (start.getTime <= end.getTime) {
            days.add(sdfgoods.format(start))
            c.setTime(start)
            c.add(Calendar.DATE, 1)
            start = c.getTime
          }
        }
        val array = days.toArray
        for (i <- 0 until array.size) yield
          (goods_sid, array(i), stock)
      }.flatMap(s => s).distinct().map(s => ((s._1, s._2.substring(0, 7)), (s._3, 1))).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map(s => (s._1, s._2._1 / s._2._2.toDouble))

    val goodsturnOverDays = goodsSaleDaysRdd.join(goodsSale).join(goodsSaleStock)
      .map{case ((goods_sid, month), ((sale_days, (category_sid, category_name, lev, com_sid, sales_amount)), stock))=>
        ((category_sid, category_name, lev, com_sid, month), (goods_sid, sale_days, sales_amount, stock))}.filter(_._2._3 != 0.0)
      .map(s => (s._1, (s._2._1, s._2._2.toDouble / s._2._3 * s._2._4)))

     val categoryturnover =  goodsSaleDaysRdd.join(goodsSale).join(goodsSaleStock)
      .map{case ((goods_sid, month), ((sale_days, (category_id, category_name, lev, com_sid, sales)), stock)) =>
       ((category_id, category_name, lev, com_sid, month), (goods_sid, sale_days, sales , stock))}.filter( _._2._3 != 0.0)
       .map(s => (s._1, (s._2._2.toInt / s._2._3 * s._2._4, 1.0))).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
       .map(s => (s._1, s._2._1/s._2._2))

    import hiveContext.implicits._
    val result = goodsturnOverDays.join(categoryturnover)
      .map{case ((category_id, category_name, lev, com_sid, month), ((goods_sid, goods_days), category_days)) =>
        TurnoverDays(category_id.toInt, category_name, lev, com_sid, goods_sid, month, goods_days, category_days)
      }.toDF().registerTempTable("tmp")
    hiveContext.sql("insert overwrite table category.category_performance_month_turnover_days select * from tmp")

    /* val sdf = new SimpleDateFormat("yyyy-MM-dd")
     val turnoverDays = hiveContext.sql(sql).rdd.map(row => (
       (if (row.isNullAt(0) || row.get(0).toString.equalsIgnoreCase("null")) 0L else row.getLong(0), row.getString(1), row.getString(2)),(row.getString(3), row.getDouble(4))))
       .filter(_._1._1 != 0L).join(shelfRdd).map{case ((category_id, goods_sid, com_sid), ((sale_time, sale_sum), (start_time, end_time))) =>
       ((category_id, goods_sid, com_sid), (Seq(sale_sum), Seq((sdf.parse(end_time).getTime - sdf.parse(start_time).getTime)/(1000*3600*24))))}
       .reduceByKey((x, y) => (x._1 ++ y._1, x._2 ++ y._2))
       .mapValues{case (num, days) =>
         val t = num.reduce(_+_)
         val a = days.sortWith((x, y) => x > y).toArray
         (t, a(days.length-1))
       }

     val goodsStock = goodsRdd.map(s => ((s._4, s._1, s._3), s._7)).reduceByKey(_+_)
     val goodsTurnoverDays = turnoverDays.join(goodsStock).map{case ((category_id, goods_sid, com_sid), ((num, days), stock)) =>
       (category_id, goods_sid, com_sid, (days.toDouble * stock.toDouble) / num.toDouble)}*/
  }
  //销售相关
  def categorySale(hiveContext: HiveContext) : Unit = {

    val goodsTotalSql = "SELECT a.goods_sid, a.com_sid, a.brand_sid, a.mgm_level1_id, a.mgm_level1_name, a.mgm_level2_id, a.mgm_level2_name, " +
      "a.mgm_level3_id, a.mgm_level3_name, a.mgm_level4_id, a.mgm_level4_name, a.mgm_level5_id, a.sale_level5_name FROM mdata.c_goods_info_snap" +
      s" a WHERE to_date(data_date) = '$now' AND online_ind = '1' "
    val goodsTotal = hiveContext.sql(goodsTotalSql).rdd.map(row =>
      (row.getString(0), (row.getString(1), row.getString(2),
      if (row.isNullAt(3)) "-1" else row.getString(3), row.getString(4),
      if (row.isNullAt(5)) "-1" else row.getString(5), row.getString(6),
      if (row.isNullAt(7)) "-1" else row.getString(7), row.getString(8),
      if (row.isNullAt(9)) "-1" else row.getString(9), row.getString(10),
      if (row.isNullAt(11)) "-1" else row.getString(11), row.getString(12))))

    val sql = "SELECT a.member_id, a.order_no, a.industry_sid, substring(a.sale_time, 0, 7) month, b.sale_price, b.sale_sum, b.goods_code " +
      "FROM mdata.c_order_snap a INNER JOIN mdata.c_order_detail b ON a.order_no = b.order_no AND b.online_order_ind = '1' WHERE " +
      "a.online_order_ind = '1' AND a.order_valid_ind = '1' AND a.order_va_ind = '1'  AND a.order_type_code <> '25'"

    val saleRdd = hiveContext.sql(sql).rdd.map(row => (
      if (row.isNullAt(0) || row.get(0).toString.equalsIgnoreCase("null")) "0" else row.getString(0),row.getString(1),
      if (row.isNullAt(2) || row.get(2).toString.equalsIgnoreCase("null")) "0" else row.getString(2), row.getString(3),
      if (row.isNullAt(4) || row.get(4).toString.equalsIgnoreCase("null")) 0.0 else row.getDouble(4),
      if (row.isNullAt(5) || row.get(5).toString.equalsIgnoreCase("null")) 0.0 else row.getDouble(5), row.getString(6)))
      .map(s => (s._7, (s._1, s._2, s._3, s._4, s._5, s._6)))

    import hiveContext.implicits._
    val saleMoney = saleRdd.join(goodsTotal).map(s => (s._1, s._2._1, s._2._2._3, s._2._2._4, s._2._2._5, s._2._2._6, s._2._2._7, s._2._2._8, s._2._2._9, s._2._2._10, s._2._2._11, s._2._2._12))
      .map{case (goods_sid, (member_id, order_no, com_sid, month, sale_price, sale_sum), l1, l1Name, l2, l2Name, l3, l3Name, l4, l4Name, l5, l5Name) =>
        Array((l1, l1Name, 1), (l2, l2Name, 2), (l3, l3Name, 3), (l4, l4Name, 4), (l5, l5Name, 5)).filter(!_._1.equals("-1"))
          .map(s => ((s._1, s._2, s._3, com_sid, month), (Set(member_id), Set(order_no), sale_price * sale_sum, sale_sum)))
      }.flatMap(s => s).reduceByKey((x, y) => (x._1 ++ y._1, x._2 ++ y._2, x._3 + y._3, x._4 + y._4))
      .mapValues(s => (s._1.size.toDouble, s._2.size.toDouble, s._3, s._4))
      .mapValues{case (costomers, order_amount, sales, sales_amount) =>
        (sales, order_amount, sales_amount, costomers,
          if (costomers == 0.0) 0.0 else sales_amount / costomers,
          if (costomers == 0.0) 0.0 else sales / costomers,
          if (order_amount == 0.0) 0.0 else sales / order_amount,
          if (sales_amount == 0.0) 0.0 else sales / sales_amount)
      }.map{case ((category_id, category_name, lev, com_sid, month), (sales, order_amount, sales_amount, costomers, avg_buy_number, avg_costomer_price, avg_order_price, avg_goods_price)) =>
        SaleDetail(category_id.toInt, category_name, lev, com_sid, month, sales, order_amount, sales_amount, costomers, avg_buy_number, avg_costomer_price, avg_order_price, avg_goods_price)
      }.toDF().registerTempTable("tmp")
    hiveContext.sql("insert overwrite table category.category_performance_month_sale_detail select * from tmp")
  }
}

case class SaleDetail(category_id: Int, category_name: String, lev: Int, com_sid: String, month: String, sales: Double, order_amount: Double, sales_amount: Double,
                      costomers: Double, avg_buy_number: Double, avg_costomer_price: Double, avg_order_price: Double, avg_goods_price: Double )
case class Popularity(category_id: Int, category_name: String, lev: Int, com_sid: String, month: String, pv: Double, uv: Double,
                 order_conversion_rate: Double, buy_conversion_rate: Double)
case class Sku(category_id: Int, category_name: String, lev: Int, com_sid: String, month: String, sku_for_sale_dynamic_ratio: Double,
               sku_dynamic_ratio: Double, eighty_percent_con_ratio: Double, sku_for_sale: Double)
case class TurnoverDays(category_id: Int, category_name: String, lev: Int, com_sid: String, goods_sid: String, month: String, goods_turnover_days : Double, category_turnover_days: Double)

case class Brand(category_id: Int, category_name: String, lev: Int, com_sid: String, month: String, brand_salesof_amount: Double,
                 brand_amount: Double, brand_dynamic_ratio: Double)

