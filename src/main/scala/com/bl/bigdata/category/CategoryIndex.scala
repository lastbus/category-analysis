package com.bl.bigdata.category

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

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

  //人气相关
  def categoryPopularity(hiveContext: HiveContext) : Unit = {
    val categorySql = "  SELECT c.category_id, c.level1_id, c.level1_name, c.level2_id, c.level2_name, " +
      "   c.level3_id, c.level3_name,  c.level4_id, c.level4_name, c.level5_id, c.level5_name " +
      "   FROM idmdata.dim_management_category c "
    val categoryRawRDD = hiveContext.sql(categorySql).map { r =>
      (if (r.isNullAt(0)) -1L else r.getLong(0),
        (if (r.isNullAt(1)) -1 else r.getLong(1), r.getString(2),
          if (r.isNullAt(3)) -1 else r.getLong(3), r.getString(4),
          if (r.isNullAt(5)) -1 else r.getLong(5), r.getString(6),
          if (r.isNullAt(7)) -1 else r.getLong(7), r.getString(8),
          if (r.isNullAt(9)) -1 else r.getLong(9), r.getString(10)))
    }.distinct().partitionBy(new HashPartitioner(10)).cache()

    val categoryBrandSql = "SELECT c.category_id, c.level1_id, c.level1_name, c.level2_id, c.level2_name, " +
      "   c.level3_id, c.level3_name,  c.level4_id, c.level4_name, c.level5_id, c.level5_name, c.product_id" +
      "   FROM idmdata.dim_management_category c "
    val categoryBrandRawRDD = hiveContext.sql(categoryBrandSql).map { r =>
      ((if (r.isNullAt(11)) 1L else r.getLong(11)),
        (if (r.isNullAt(0)) -1L else r.getLong(0),
          if (r.isNullAt(1)) -1 else r.getLong(1), r.getString(2),
          if (r.isNullAt(3)) -1 else r.getLong(3), r.getString(4),
          if (r.isNullAt(5)) -1 else r.getLong(5), r.getString(6),
          if (r.isNullAt(7)) -1 else r.getLong(7), r.getString(8),
          if (r.isNullAt(9)) -1 else r.getLong(9), r.getString(10)))
    }.filter(s => s._1 != 1L).map(s => (s._1.toString, s._2)).distinct().partitionBy(new HashPartitioner(10)).cache()

    val goodsTotalSql = "SELECT sid, pro_sid, com_sid, brand_sid FROM sourcedata.s06_pcm_mdm_goods  UNION ALL SELECT CAST (sid AS string), CAST (pro_sid AS string), com_sid, CAST (brand_sid AS string) FROM sourcedata.s06_pcm_abandoned_goods"
    val goodsTotal = hiveContext.sql(goodsTotalSql).rdd.map(row => (row.getString(0), row.getString(1), row.getString(2), row.getString(3)))
    val pvUvSql = "SELECT u.category_sid, v.com_sid, u.cookie_id, substring(u.event_date, 0, 7) as month FROM recommendation.user_behavior_raw_data u    " +
      " JOIN (SELECT a.sid, a.com_sid FROM sourcedata.s06_pcm_mdm_goods a UNION ALL SELECT CAST (b.sid AS string), " +
      "b.com_sid FROM  sourcedata.s06_pcm_abandoned_goods b)v ON u.goods_sid = v.sid AND u.behavior_type = '1000'"

    //pv和uv
    val pvuv = hiveContext.sql(pvUvSql).rdd.map(row => (
      if (row.isNullAt(0) || row.get(0).toString.equalsIgnoreCase("null")) "0" else row.getString(0),
      if (row.isNullAt(1) || row.get(1).toString.equalsIgnoreCase("null")) "0" else row.getString(1),
      row.getString(2), row.getString(3))).filter(s => !s._1.equals("0") && !s._2.equals("0"))
      .map{case (category_sid, com_sid, cookie_id, month) => ((category_sid, com_sid, month), (Set(cookie_id), Seq(cookie_id)))}
      .reduceByKey((x, y) => (x._1 ++ y._1, x._2 ++ y._2)).map(s => (s._1._1.toLong, (s._1._2, s._1._3, s._2._1.size, s._2._2.size)))
      .join(categoryRawRDD).map { case (category_id, ((com_sid, month, uv, pv), (l1, l1Name, l2, l2Name, l3, l3Name, l4, l4Name, l5, l5Name))) =>
      Array((l1, l1Name, 1), (l2, l2Name, 2), (l3, l3Name, 3), (l4, l4Name, 4), (l5, l5Name, 5)).filter(_._1 != -1)
        .map(s => ((s._1, s._2, s._3, com_sid, month), (pv, uv)))
    }.flatMap(s => s).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

    val saleSql = "SELECT  o.member_id, o.order_no, o.category_id, o.brand_sid, o.com_sid, substring(o.sale_time, 0, 7) month, o.sale_price, o.sale_sum, o.goods_sid  " +
      " FROM  recommendation.order_info o where o.ORDER_STATUS NOT IN ('1001', '1029', '1100')"
    val saleRdd = hiveContext.sql(saleSql).rdd.map(row =>(
      if (row.isNullAt(0) || row.get(0).toString.equalsIgnoreCase("null")) "0" else row.getString(0),row.getString(1),
      if (row.isNullAt(2) || row.get(2).toString.equalsIgnoreCase("null")) 0L else row.getLong(2),
      if (row.isNullAt(3) || row.get(3).toString.equalsIgnoreCase("null")) "0" else row.getString(3),
      if (row.isNullAt(4) || row.get(4).toString.equalsIgnoreCase("null")) "0" else row.getString(4), row.getString(5),
      if (row.isNullAt(6) || row.get(6).toString.equalsIgnoreCase("null")) 0.0 else row.getDouble(6),
      if (row.isNullAt(7) || row.get(7).toString.equalsIgnoreCase("null")) 0.0 else row.getDouble(7), row.getString(8)
      )).filter(s => !s._1.equals("0") && s._3 != 0L && !s._4.equals("0") && !s._5.equals("0"))

    //order_conversion_rate和buy_conversion_rate
    val rate = saleRdd.map(s => ((s._3, s._5, s._6), (Set(s._1), Set(s._2)))).reduceByKey((x, y) => (x._1 ++ y._1, x._2 ++ y._2))
      .map(s => (s._1._1, (s._1._2, s._1._3, s._2._1.size, s._2._2.size)))
      .join(categoryRawRDD).map{case(category_sid, ((com_sid, month, order_num, member_num), (l1, l1Name, l2, l2Name, l3, l3Name, l4, l4Name, l5, l5Name))) =>
      Array((l1, l1Name, 1), (l2, l2Name, 2), (l3, l3Name, 3), (l4, l4Name, 4), (l5, l5Name, 5))
        .map(s => ((s._1, s._2, s._3, com_sid, month), (order_num, member_num)))}
      .flatMap(s => s).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .join(pvuv).mapValues(s => (s._1._1.toDouble/s._2._2.toDouble, s._1._2.toDouble/s._2._2.toDouble))
  }
  //sku相关
  def categorySku(hiveContext: HiveContext) : Unit = {
    val categorySql = "  SELECT c.category_id, c.level1_id, c.level1_name, c.level2_id, c.level2_name, " +
      "   c.level3_id, c.level3_name,  c.level4_id, c.level4_name, c.level5_id, c.level5_name " +
      "   FROM idmdata.dim_management_category c "
    val categoryRawRDD = hiveContext.sql(categorySql).map { r =>
      (if (r.isNullAt(0)) -1L else r.getLong(0),
        (if (r.isNullAt(1)) -1 else r.getLong(1), r.getString(2),
          if (r.isNullAt(3)) -1 else r.getLong(3), r.getString(4),
          if (r.isNullAt(5)) -1 else r.getLong(5), r.getString(6),
          if (r.isNullAt(7)) -1 else r.getLong(7), r.getString(8),
          if (r.isNullAt(9)) -1 else r.getLong(9), r.getString(10)))
    }.distinct().partitionBy(new HashPartitioner(10)).cache()

    val categoryBrandSql = "SELECT c.category_id, c.level1_id, c.level1_name, c.level2_id, c.level2_name, " +
      "   c.level3_id, c.level3_name,  c.level4_id, c.level4_name, c.level5_id, c.level5_name, c.product_id" +
      "   FROM idmdata.dim_management_category c "
    val categoryBrandRawRDD = hiveContext.sql(categoryBrandSql).map { r =>
      ((if (r.isNullAt(11)) 1L else r.getLong(11)),
        (if (r.isNullAt(0)) -1L else r.getLong(0),
          if (r.isNullAt(1)) -1 else r.getLong(1), r.getString(2),
          if (r.isNullAt(3)) -1 else r.getLong(3), r.getString(4),
          if (r.isNullAt(5)) -1 else r.getLong(5), r.getString(6),
          if (r.isNullAt(7)) -1 else r.getLong(7), r.getString(8),
          if (r.isNullAt(9)) -1 else r.getLong(9), r.getString(10)))
    }.filter(s => s._1 != 1L).map(s => (s._1.toString, s._2)).distinct().partitionBy(new HashPartitioner(10)).cache()

    val goodsTotalSql = "SELECT sid, pro_sid, com_sid, brand_sid FROM sourcedata.s06_pcm_mdm_goods  UNION ALL SELECT CAST (sid AS string), CAST (pro_sid AS string), com_sid, CAST (brand_sid AS string) FROM sourcedata.s06_pcm_abandoned_goods"
    val goodsTotal = hiveContext.sql(goodsTotalSql).rdd.map(row => (row.getString(0), row.getString(1), row.getString(2), row.getString(3)))

    val categoryGoodsRdd = goodsTotal.map(s => (s._2, (s._1, s._3, s._4))).join(categoryBrandRawRDD)
      .map{case (pro_sid, ((goods_sid, com_sid, brand_sid), category)) => (goods_sid, (com_sid, brand_sid, category))}

    val goodsSql = "SELECT sid, pro_sid, com_sid FROM (SELECT a.sid, pro_sid, a.com_sid FROM sourcedata.s06_pcm_mdm_goods a " +
      "UNION ALL SELECT CAST (b.sid AS string), CAST(b.pro_sid AS string),  b.com_sid FROM sourcedata.s06_pcm_abandoned_goods b ) c "
    val skuSql= "SELECT DISTINCT a.goods_sid, a.sale_status, a.start_dt, a.end_dt, b.sale_stock_sum FROM pdata.t02_pcm_chan_sale_h a JOIN pdata.t02_pcm_stock_h b ON a.goods_sid = b.goods_sid "
    val skuRdd = hiveContext.sql(skuSql).rdd.map(row => (row.getString(0), row.getDouble(1), row.getDate(2), row.getDate(3), row.getDouble(4)))
    val sdfSku = new SimpleDateFormat("yyyy-MM")

    //sku_for_sale
    val skuForSale = skuRdd.filter(s => s._2 == 4.0 && s._5 > 0)
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
      }.flatMap(s => s).distinct().join(categoryGoodsRdd)
      .map{case (goods_sid, (month, (com_sid, brand_sid, category))) => ((category._1, com_sid, month), 1)}
      .reduceByKey(_+_)
      .map(s => ((s._1._1), (s._1._2, s._1._3, s._2)))
      .join(categoryRawRDD).map{case (category_id, ((com_sid, month, sku), (l1, l1Name, l2, l2Name, l3, l3Name, l4, l4Name, l5, l5Name))) =>
      Array((l1, l1Name, 1), (l2, l2Name, 2), (l3, l3Name, 3), (l4, l4Name, 4), (l5, l5Name, 5))
        .map(s => ((s._1, s._2, s._3, com_sid, month), sku))}
      .flatMap(s => s).reduceByKey(_+_)

    // sku总数
    val sku = skuRdd.map(s => (s._1, s._3, s._4))
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
      }.flatMap(s => s).distinct().join(categoryGoodsRdd)
      .map{case (goods_sid, (month, (com_sid, brand_sid, category))) => ((category._1, com_sid, month), 1)}
      .reduceByKey(_+_)
      .map(s => ((s._1._1, s._1._2, s._1._3), s._2))

    val saleSql = "SELECT  o.member_id, o.order_no, o.category_id, o.brand_sid, o.com_sid, substring(o.sale_time, 0, 7) month, o.sale_price, o.sale_sum, o.goods_sid  " +
      " FROM  recommendation.order_info o where o.ORDER_STATUS NOT IN ('1001', '1029', '1100')"
    val saleRdd = hiveContext.sql(saleSql).rdd.map(row =>(
      if (row.isNullAt(0) || row.get(0).toString.equalsIgnoreCase("null")) "0" else row.getString(0),row.getString(1),
      if (row.isNullAt(2) || row.get(2).toString.equalsIgnoreCase("null")) 0L else row.getLong(2),
      if (row.isNullAt(3) || row.get(3).toString.equalsIgnoreCase("null")) "0" else row.getString(3),
      if (row.isNullAt(4) || row.get(4).toString.equalsIgnoreCase("null")) "0" else row.getString(4), row.getString(5),
      if (row.isNullAt(6) || row.get(6).toString.equalsIgnoreCase("null")) 0.0 else row.getDouble(6),
      if (row.isNullAt(7) || row.get(7).toString.equalsIgnoreCase("null")) 0.0 else row.getDouble(7), row.getString(8)
      )).filter(s => !s._1.equals("0") && s._3 != 0L && !s._4.equals("0") && !s._5.equals("0"))

    //eighty_percent_con_ratio
    val eightyPercentConRatio = saleRdd.map(s => ((s._3, s._5, s._6, s._9), s._7 * s._8)).reduceByKey(_+_)
      .map(s => ((s._1._1, s._1._2, s._1._3), Seq(s._2)))
      .reduceByKey(_++_)
      .map{case ((category_id, com_sid, month), array) =>
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
        ((category_id, com_sid, month), j)
      }.join(sku).map(s => (s._1._1, (s._1._2, s._1._3, s._2._1, s._2._2)))
      .join(categoryRawRDD).map{case (category_id, ((com_sid, month, sale, skuTotal), (l1, l1Name, l2, l2Name, l3, l3Name, l4, l4Name, l5, l5Name))) =>
      Array((l1, l1Name, 1), (l2, l2Name, 2), (l3, l3Name, 3), (l4, l4Name, 4), (l5, l5Name, 5))
        .map(s => ((s._1, s._2, s._3, com_sid, month), (sale, skuTotal)))}
      .flatMap(s => s).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues(s => (s._1.toDouble/s._2.toDouble))

    //sku_dynamic_ratio
    val skuDynamicRatio = saleRdd.map(s => (s._9, (s._3, s._5, s._6))).distinct()
      .map{case (sid, (category_id, com_sid, month)) => ((category_id, com_sid, month), 1)}.reduceByKey(_+_)
      .join(sku).map{case ((category_id, com_sid, month), (sku_sale, total_sku)) => (category_id, (com_sid, month, sku_sale, total_sku))}
      .join(categoryRawRDD).map{case(category_sid, ((com_sid, month, sku_sale, total_sku), (l1, l1Name, l2, l2Name, l3, l3Name, l4, l4Name, l5, l5Name))) =>
      Array((l1, l1Name, 1), (l2, l2Name, 2), (l3, l3Name, 3), (l4, l4Name, 4), (l5, l5Name, 5))
        .map(s => ((s._1, s._2, s._3, com_sid, month), (sku_sale, total_sku)))}
      .flatMap(s => s).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map(s => (s._1, s._2._1.toDouble/s._2._2.toDouble))

    //sku_for_sale_dynamic_ratio
    val skuForSaleDynamicRatio = saleRdd.map(s => (s._9, (s._3, s._5, s._6))).distinct()
      .map{case (sid, (category_id, com_sid, month)) => ((category_id, com_sid, month), 1)}.reduceByKey(_+_)
      .map(s => (s._1._1, (s._1._2, s._1._3, s._2)))
      .join(categoryRawRDD).map{case(category_sid, ((com_sid, month, sku_sale), (l1, l1Name, l2, l2Name, l3, l3Name, l4, l4Name, l5, l5Name))) =>
      Array((l1, l1Name, 1), (l2, l2Name, 2), (l3, l3Name, 3), (l4, l4Name, 4), (l5, l5Name, 5))
        .map(s => ((s._1, s._2, s._3, com_sid, month), sku_sale))}
      .flatMap(s => s).reduceByKey(_+_)
      .join(skuForSale).map{s => (s._1, s._2._1.toDouble/s._2._2.toDouble)}
  }
  //品牌相关
  def categoryBrand(hiveContext: HiveContext) : Unit = {
    val categorySql = "  SELECT c.category_id, c.level1_id, c.level1_name, c.level2_id, c.level2_name, " +
      "   c.level3_id, c.level3_name,  c.level4_id, c.level4_name, c.level5_id, c.level5_name " +
      "   FROM idmdata.dim_management_category c "
    val categoryRawRDD = hiveContext.sql(categorySql).map { r =>
      (if (r.isNullAt(0)) -1L else r.getLong(0),
        (if (r.isNullAt(1)) -1 else r.getLong(1), r.getString(2),
          if (r.isNullAt(3)) -1 else r.getLong(3), r.getString(4),
          if (r.isNullAt(5)) -1 else r.getLong(5), r.getString(6),
          if (r.isNullAt(7)) -1 else r.getLong(7), r.getString(8),
          if (r.isNullAt(9)) -1 else r.getLong(9), r.getString(10)))
    }.distinct().partitionBy(new HashPartitioner(10)).cache()

    val categoryBrandSql = "SELECT c.category_id, c.level1_id, c.level1_name, c.level2_id, c.level2_name, " +
      "   c.level3_id, c.level3_name,  c.level4_id, c.level4_name, c.level5_id, c.level5_name, c.product_id" +
      "   FROM idmdata.dim_management_category c "
    val categoryBrandRawRDD = hiveContext.sql(categoryBrandSql).map { r =>
      ((if (r.isNullAt(11)) 1L else r.getLong(11)),
        (if (r.isNullAt(0)) -1L else r.getLong(0),
          if (r.isNullAt(1)) -1 else r.getLong(1), r.getString(2),
          if (r.isNullAt(3)) -1 else r.getLong(3), r.getString(4),
          if (r.isNullAt(5)) -1 else r.getLong(5), r.getString(6),
          if (r.isNullAt(7)) -1 else r.getLong(7), r.getString(8),
          if (r.isNullAt(9)) -1 else r.getLong(9), r.getString(10)))
    }.filter(s => s._1 != 1L).map(s => (s._1.toString, s._2)).distinct().partitionBy(new HashPartitioner(10)).cache()

    val goodsTotalSql = "SELECT sid, pro_sid, com_sid, brand_sid FROM sourcedata.s06_pcm_mdm_goods  UNION ALL SELECT CAST (sid AS string), CAST (pro_sid AS string), com_sid, CAST (brand_sid AS string) FROM sourcedata.s06_pcm_abandoned_goods"
    val goodsTotal = hiveContext.sql(goodsTotalSql).rdd.map(row => (row.getString(0), row.getString(1), row.getString(2), row.getString(3)))

    val categoryGoodsRdd = goodsTotal.map(s => (s._2, (s._1, s._3, s._4))).join(categoryBrandRawRDD)
      .map{case (pro_sid, ((goods_sid, com_sid, brand_sid), category)) => (goods_sid, (com_sid, brand_sid, category))}


     val goodsSql = "SELECT sid, pro_sid, com_sid FROM (SELECT a.sid, pro_sid, a.com_sid FROM sourcedata.s06_pcm_mdm_goods a " +
      "UNION ALL SELECT CAST (b.sid AS string), CAST(b.pro_sid AS string),  b.com_sid FROM sourcedata.s06_pcm_abandoned_goods b ) c "
    val skuSql= "SELECT DISTINCT a.goods_sid, a.sale_status, a.start_dt, a.end_dt, b.sale_stock_sum FROM pdata.t02_pcm_chan_sale_h a JOIN pdata.t02_pcm_stock_h b ON a.goods_sid = b.goods_sid "

    val goodsRdd = hiveContext.sql(goodsSql).rdd.map(row => (row.getString(0), (row.getString(1), row.getString(2))))
    val skuRdd = hiveContext.sql(skuSql).rdd.map(row => (row.getString(0), row.getDouble(1), row.getDate(2), row.getDate(3), row.getDouble(4)))

    val saleSql = "SELECT  o.member_id, o.order_no, o.category_id, o.brand_sid, o.com_sid, substring(o.sale_time, 0, 7) month, o.sale_price, o.sale_sum, o.goods_sid  " +
      " FROM  recommendation.order_info o where o.ORDER_STATUS NOT IN ('1001', '1029', '1100')"
    val saleRdd = hiveContext.sql(saleSql).rdd.map(row =>(
      if (row.isNullAt(0) || row.get(0).toString.equalsIgnoreCase("null")) "0" else row.getString(0),row.getString(1),
      if (row.isNullAt(2) || row.get(2).toString.equalsIgnoreCase("null")) 0L else row.getLong(2),
      if (row.isNullAt(3) || row.get(3).toString.equalsIgnoreCase("null")) "0" else row.getString(3),
      if (row.isNullAt(4) || row.get(4).toString.equalsIgnoreCase("null")) "0" else row.getString(4), row.getString(5),
      if (row.isNullAt(6) || row.get(6).toString.equalsIgnoreCase("null")) 0.0 else row.getDouble(6),
      if (row.isNullAt(7) || row.get(7).toString.equalsIgnoreCase("null")) 0.0 else row.getDouble(7), row.getString(8)
      )).filter(s => !s._1.equals("0") && s._3 != 0L && !s._4.equals("0") && !s._5.equals("0"))

    //brand_amount
    val brandNum = categoryGoodsRdd.map(s => (s._2._3._1, s._2))
      .map{case (category_id, (com_sid, brand_sid, (category, l1, l1Name, l2, l2Name, l3, l3Name, l4, l4Name, l5, l5Name))) =>
        Array((l1, l1Name, 1), (l2, l2Name, 2), (l3, l3Name, 3), (l4, l4Name, 4), (l5, l5Name, 5))
          .map(s => ((s._1, s._2, s._3, com_sid), Set(brand_sid)))}.flatMap(s => s).reduceByKey(_++_)
      .map{case ((category_id, category_name, lev, com_sid), brandArray) => ((category_id, category_name, lev, com_sid), brandArray.size)}

    //brand_salesof_amount
    val brandSale = saleRdd.map(s => (s._3, s._5, s._4, s._6)).distinct()
      .map(s => ((s._1.toString, s._2, s._4), 1)).reduceByKey(_+_).map(s => (s._1._1.toLong, (s._1._2, s._1._3,s._2)))
      .join(categoryRawRDD).map{case(category_sid, ((com_sid, month, brandSaleNum), (l1, l1Name, l2, l2Name, l3, l3Name, l4, l4Name, l5, l5Name))) =>
      Array((l1, l1Name, 1), (l2, l2Name, 2), (l3, l3Name, 3), (l4, l4Name, 4), (l5, l5Name, 5))
        .map(s => ((s._1, s._2, s._3, com_sid, month), brandSaleNum))}.flatMap(s => s).reduceByKey(_+_)

    //brand_dynamic_ratio
    val brandDynamicRatio = brandSale.map{case ((category_id, category_name, lev, com_sid, month), brand_salesof_amount) =>
      ((category_id, category_name, lev, com_sid), (month, brand_salesof_amount))}
      .join(brandNum).map{case ((category_id, category_name, lev, com_sid), ((month, brandSale), brandTotal)) =>
      ((category_id, category_name, lev, com_sid, month), brandSale.toDouble/brandTotal.toDouble)
    }
  }
  //周期
  def categoryPeriod(hiveContext: HiveContext) : Unit = {
    val categorySql = "  SELECT c.category_id, c.level1_id, c.level1_name, c.level2_id, c.level2_name, " +
      "   c.level3_id, c.level3_name,  c.level4_id, c.level4_name, c.level5_id, c.level5_name " +
      "   FROM idmdata.dim_management_category c "
    val categoryRawRDD = hiveContext.sql(categorySql).map { r =>
      (if (r.isNullAt(0)) -1L else r.getLong(0),
        (if (r.isNullAt(1)) -1 else r.getLong(1), r.getString(2),
          if (r.isNullAt(3)) -1 else r.getLong(3), r.getString(4),
          if (r.isNullAt(5)) -1 else r.getLong(5), r.getString(6),
          if (r.isNullAt(7)) -1 else r.getLong(7), r.getString(8),
          if (r.isNullAt(9)) -1 else r.getLong(9), r.getString(10)))
    }.distinct().partitionBy(new HashPartitioner(10)).cache()

    val categoryBrandSql = "SELECT c.category_id, c.level1_id, c.level1_name, c.level2_id, c.level2_name, " +
      "   c.level3_id, c.level3_name,  c.level4_id, c.level4_name, c.level5_id, c.level5_name, c.product_id" +
      "   FROM idmdata.dim_management_category c "
    val categoryBrandRawRDD = hiveContext.sql(categoryBrandSql).map { r =>
      ((if (r.isNullAt(11)) 1L else r.getLong(11)),
        (if (r.isNullAt(0)) -1L else r.getLong(0),
          if (r.isNullAt(1)) -1 else r.getLong(1), r.getString(2),
          if (r.isNullAt(3)) -1 else r.getLong(3), r.getString(4),
          if (r.isNullAt(5)) -1 else r.getLong(5), r.getString(6),
          if (r.isNullAt(7)) -1 else r.getLong(7), r.getString(8),
          if (r.isNullAt(9)) -1 else r.getLong(9), r.getString(10)))
    }.filter(s => s._1 != 1L).map(s => (s._1.toString, s._2)).distinct().partitionBy(new HashPartitioner(10)).cache()

    val goodsTotalSql = "SELECT sid, pro_sid, com_sid, brand_sid FROM sourcedata.s06_pcm_mdm_goods  UNION ALL SELECT CAST (sid AS string), CAST (pro_sid AS string), com_sid, CAST (brand_sid AS string) FROM sourcedata.s06_pcm_abandoned_goods"
    val goodsTotal = hiveContext.sql(goodsTotalSql).rdd.map(row => (row.getString(0), row.getString(1), row.getString(2), row.getString(3)))

    val categoryGoodsRdd = goodsTotal.map(s => (s._2, (s._1, s._3, s._4))).join(categoryBrandRawRDD)
      .map{case (pro_sid, ((goods_sid, com_sid, brand_sid), category)) => (goods_sid, (com_sid, brand_sid, category))}


     val goodsSql = "SELECT sid, pro_sid, com_sid FROM (SELECT a.sid, pro_sid, a.com_sid FROM sourcedata.s06_pcm_mdm_goods a " +
      "UNION ALL SELECT CAST (b.sid AS string), CAST(b.pro_sid AS string),  b.com_sid FROM sourcedata.s06_pcm_abandoned_goods b ) c "
    val skuSql= "SELECT DISTINCT a.goods_sid, a.sale_status, a.start_dt, a.end_dt, b.sale_stock_sum FROM pdata.t02_pcm_chan_sale_h a JOIN pdata.t02_pcm_stock_h b ON a.goods_sid = b.goods_sid "

    val goodsRdd = hiveContext.sql(goodsSql).rdd.map(row => (row.getString(0), (row.getString(1), row.getString(2))))
    val skuRdd = hiveContext.sql(skuSql).rdd.map(row => (row.getString(0), row.getDouble(1), row.getDate(2), row.getDate(3), row.getDouble(4)))

    val sdfSku = new SimpleDateFormat("yyyy-MM")

    //sku_for_sale
    val skuForSale = skuRdd.filter(s => s._2 == 4.0 && s._5 > 0)
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
      }.flatMap(s => s).distinct().join(categoryGoodsRdd)
      .map{case (goods_sid, (month, (com_sid, brand_sid, category))) => ((category._1, com_sid, month), 1)}
      .reduceByKey(_+_)
      .map(s => ((s._1._1), (s._1._2, s._1._3, s._2)))
      .join(categoryRawRDD).map{case (category_id, ((com_sid, month, sku), (l1, l1Name, l2, l2Name, l3, l3Name, l4, l4Name, l5, l5Name))) =>
      Array((l1, l1Name, 1), (l2, l2Name, 2), (l3, l3Name, 3), (l4, l4Name, 4), (l5, l5Name, 5))
        .map(s => ((s._1, s._2, s._3, com_sid, month), sku))}
      .flatMap(s => s).reduceByKey(_+_)

    // sku总数
    val sku = skuRdd.map(s => (s._1, s._3, s._4))
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
      }.flatMap(s => s).distinct().join(categoryGoodsRdd)
      .map{case (goods_sid, (month, (com_sid, brand_sid, category))) => ((category._1, com_sid, month), 1)}
      .reduceByKey(_+_)
      .map(s => ((s._1._1, s._1._2, s._1._3), s._2))

    val saleSql = "SELECT  o.member_id, o.order_no, o.category_id, o.brand_sid, o.com_sid, substring(o.sale_time, 0, 7) month, o.sale_price, o.sale_sum, o.goods_sid  " +
      " FROM  recommendation.order_info o where o.ORDER_STATUS NOT IN ('1001', '1029', '1100')"
    val saleRdd = hiveContext.sql(saleSql).rdd.map(row =>(
      if (row.isNullAt(0) || row.get(0).toString.equalsIgnoreCase("null")) "0" else row.getString(0),row.getString(1),
      if (row.isNullAt(2) || row.get(2).toString.equalsIgnoreCase("null")) 0L else row.getLong(2),
      if (row.isNullAt(3) || row.get(3).toString.equalsIgnoreCase("null")) "0" else row.getString(3),
      if (row.isNullAt(4) || row.get(4).toString.equalsIgnoreCase("null")) "0" else row.getString(4), row.getString(5),
      if (row.isNullAt(6) || row.get(6).toString.equalsIgnoreCase("null")) 0.0 else row.getDouble(6),
      if (row.isNullAt(7) || row.get(7).toString.equalsIgnoreCase("null")) 0.0 else row.getDouble(7), row.getString(8)
      )).filter(s => !s._1.equals("0") && s._3 != 0L && !s._4.equals("0") && !s._5.equals("0"))

    //eighty_percent_con_ratio
    val eightyPercentConRatio = saleRdd.map(s => ((s._3, s._5, s._6, s._9), s._7 * s._8)).reduceByKey(_+_)
      .map(s => ((s._1._1, s._1._2, s._1._3), Seq(s._2)))
      .reduceByKey(_++_)
      .map{case ((category_id, com_sid, month), array) =>
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
        ((category_id, com_sid, month), j)
      }.join(sku).map(s => (s._1._1, (s._1._2, s._1._3, s._2._1, s._2._2)))
      .join(categoryRawRDD).map{case (category_id, ((com_sid, month, sale, skuTotal), (l1, l1Name, l2, l2Name, l3, l3Name, l4, l4Name, l5, l5Name))) =>
      Array((l1, l1Name, 1), (l2, l2Name, 2), (l3, l3Name, 3), (l4, l4Name, 4), (l5, l5Name, 5))
        .map(s => ((s._1, s._2, s._3, com_sid, month), (sale, skuTotal)))}
      .flatMap(s => s).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues(s => (s._1.toDouble/s._2.toDouble))


    //sku_dynamic_ratio
    val skuDynamicRatio = saleRdd.map(s => (s._9, (s._3, s._5, s._6))).distinct()
      .map{case (sid, (category_id, com_sid, month)) => ((category_id, com_sid, month), 1)}.reduceByKey(_+_)
      .join(sku).map{case ((category_id, com_sid, month), (sku_sale, total_sku)) => (category_id, (com_sid, month, sku_sale, total_sku))}
      .join(categoryRawRDD).map{case(category_sid, ((com_sid, month, sku_sale, total_sku), (l1, l1Name, l2, l2Name, l3, l3Name, l4, l4Name, l5, l5Name))) =>
      Array((l1, l1Name, 1), (l2, l2Name, 2), (l3, l3Name, 3), (l4, l4Name, 4), (l5, l5Name, 5))
        .map(s => ((s._1, s._2, s._3, com_sid, month), (sku_sale, total_sku)))}
      .flatMap(s => s).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map(s => (s._1, s._2._1.toDouble/s._2._2.toDouble))



    //sku_for_sale_dynamic_ratio
    val skuForSaleDynamicRatio = saleRdd.map(s => (s._9, (s._3, s._5, s._6))).distinct()
      .map{case (sid, (category_id, com_sid, month)) => ((category_id, com_sid, month), 1)}.reduceByKey(_+_)
      .map(s => (s._1._1, (s._1._2, s._1._3, s._2)))
      .join(categoryRawRDD).map{case(category_sid, ((com_sid, month, sku_sale), (l1, l1Name, l2, l2Name, l3, l3Name, l4, l4Name, l5, l5Name))) =>
      Array((l1, l1Name, 1), (l2, l2Name, 2), (l3, l3Name, 3), (l4, l4Name, 4), (l5, l5Name, 5))
        .map(s => ((s._1, s._2, s._3, com_sid, month), sku_sale))}
      .flatMap(s => s).reduceByKey(_+_)
      .join(skuForSale).map{s => (s._1, s._2._1.toDouble/s._2._2.toDouble)}



    //brand_amount
    val brandNum = categoryGoodsRdd.map(s => (s._2._3._1, s._2))
      .map{case (category_id, (com_sid, brand_sid, (category, l1, l1Name, l2, l2Name, l3, l3Name, l4, l4Name, l5, l5Name))) =>
        Array((l1, l1Name, 1), (l2, l2Name, 2), (l3, l3Name, 3), (l4, l4Name, 4), (l5, l5Name, 5))
          .map(s => ((s._1, s._2, s._3, com_sid), Set(brand_sid)))}.flatMap(s => s).reduceByKey(_++_)
      .map{case ((category_id, category_name, lev, com_sid), brandArray) => ((category_id, category_name, lev, com_sid), brandArray.size)}

    //brand_salesof_amount
    val brandSale = saleRdd.map(s => (s._3, s._5, s._4, s._6)).distinct()
      .map(s => ((s._1.toString, s._2, s._4), 1)).reduceByKey(_+_).map(s => (s._1._1.toLong, (s._1._2, s._1._3,s._2)))
      .join(categoryRawRDD).map{case(category_sid, ((com_sid, month, brandSaleNum), (l1, l1Name, l2, l2Name, l3, l3Name, l4, l4Name, l5, l5Name))) =>
      Array((l1, l1Name, 1), (l2, l2Name, 2), (l3, l3Name, 3), (l4, l4Name, 4), (l5, l5Name, 5))
        .map(s => ((s._1, s._2, s._3, com_sid, month), brandSaleNum))}.flatMap(s => s).reduceByKey(_+_)

    //brand_dynamic_ratio
    val brandDynamicRatio = brandSale.map{case ((category_id, category_name, lev, com_sid, month), brand_salesof_amount) =>
      ((category_id, category_name, lev, com_sid), (month, brand_salesof_amount))}
      .join(brandNum).map{case ((category_id, category_name, lev, com_sid), ((month, brandSale), brandTotal)) =>
      ((category_id, category_name, lev, com_sid, month), brandSale.toDouble/brandTotal.toDouble)
    }

    //turnover_days
  /*  val sku = skuRdd.map(s => (s._1, s._3, s._4))
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
      }.flatMap(s => s).distinct().join(categoryGoodsRdd)*/
    val sqlOrder = "SELECT  o.category_id,  o.goods_sid, o.com_sid, substring(o.sale_time, 0, 10), o.sale_sum " +
      " FROM  recommendation.order_info o where o.ORDER_STATUS NOT IN ('1001', '1029', '1100')"
    val sqlGoods = "SELECT DISTINCT a.goods_sid, a.sale_status, a.start_dt, a.end_dt, b.sale_stock_sum, b.com_sid, b.start_dt, b.end_dt FROM pdata.t02_pcm_chan_sale_h a JOIN pdata.t02_pcm_stock_h b ON a.goods_sid = b.goods_sid"

    val orderRdd = hiveContext.sql(sqlOrder).rdd.map(row => (row.getLong(0), row.getString(1), row.getString(2),
      if (row.isNullAt(3) || row.get(3).toString.equalsIgnoreCase("null")) "0" else row.getString(3),
      if (row.isNullAt(4) || row.get(4).toString.equalsIgnoreCase("null")) "0" else row.getString(4)))
      .filter(s => (!s._4.equals("0") && !s._5.equals("0") && s._5 > s._4)).map(s => (s._1, s._2, s._3, s._4, s._5))
    val goodsTotalRdd = hiveContext.sql(sqlGoods).rdd.map(row => (row.getString(0), row.getDouble(1), row.getDate(2), row.getDate(3),
      row.getDouble(4), if (row.isNullAt(5) || row.get(5).toString.equalsIgnoreCase("null")) "0" else row.getString(5),
      row.getDate(6), row.getDate(7))).filter(!_._5.equals("0"))

    val sdfgoods = new SimpleDateFormat("yyyy-MM-dd")

    val goodsSaleDaysRdd = goodsTotalRdd.map{(s => (s._1, s._3.toString.substring(0, 10), s._4.toString.substring(0, 10), s._6))}.distinct()
      .filter(s => s._2.matches("\\d{4}-\\d{2}") && s._3.matches("\\d{4}-\\d{2}-\\d{2}"))
      .map{case (goods_sid, start, end, com_sid) =>


      }


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
    val categorySql = "  SELECT c.category_id, c.level1_id, c.level1_name, c.level2_id, c.level2_name, " +
      "   c.level3_id, c.level3_name,  c.level4_id, c.level4_name, c.level5_id, c.level5_name " +
      "   FROM idmdata.dim_management_category c "
    val categoryRawRDD = hiveContext.sql(categorySql).map { r =>
      (if (r.isNullAt(0)) -1L else r.getLong(0),
        (if (r.isNullAt(1)) -1 else r.getLong(1), r.getString(2),
          if (r.isNullAt(3)) -1 else r.getLong(3), r.getString(4),
          if (r.isNullAt(5)) -1 else r.getLong(5), r.getString(6),
          if (r.isNullAt(7)) -1 else r.getLong(7), r.getString(8),
          if (r.isNullAt(9)) -1 else r.getLong(9), r.getString(10)))
    }.distinct().partitionBy(new HashPartitioner(10)).cache()
    val saleSql = "SELECT  o.member_id, o.order_no, o.category_id, o.brand_sid, o.com_sid, substring(o.sale_time, 0, 7) month, o.sale_price, o.sale_sum, o.goods_sid  " +
      " FROM  recommendation.order_info o where o.ORDER_STATUS NOT IN ('1001', '1029', '1100')"

    val saleRdd = hiveContext.sql(saleSql).rdd.map(row =>(
      if (row.isNullAt(0) || row.get(0).toString.equalsIgnoreCase("null")) "0" else row.getString(0),row.getString(1),
      if (row.isNullAt(2) || row.get(2).toString.equalsIgnoreCase("null")) 0L else row.getLong(2),
      if (row.isNullAt(3) || row.get(3).toString.equalsIgnoreCase("null")) "0" else row.getString(3),
      if (row.isNullAt(4) || row.get(4).toString.equalsIgnoreCase("null")) "0" else row.getString(4), row.getString(5),
      if (row.isNullAt(6) || row.get(6).toString.equalsIgnoreCase("null")) 0.0 else row.getDouble(6),
      if (row.isNullAt(7) || row.get(7).toString.equalsIgnoreCase("null")) 0.0 else row.getDouble(7), row.getString(8)
      )).filter(s => !s._1.equals("0") && s._3 != 0L && !s._4.equals("0") && !s._5.equals("0"))

    val saleMoney = saleRdd.map(s => (s._3, s._5, s._6, s._1, s._2, s._7, s._8, s._9))
      .map{case (category_id, com_sid, month, member_id, order_no, price, num, goods_sid) => ((category_id, com_sid, month), (Set(member_id),Set(order_no), price*num, num.toInt))}
      .reduceByKey((x, y) => (x._1 ++ y._1, x._2 ++ y._2, x._3 + y._3, x._4 + y._4))
      .map(s => (s._1._1, (s._1._2, s._1._3, s._2._1.size, s._2._2.size, s._2._3, s._2._4))).join(categoryRawRDD)
      .map{case (category_id, ((com_sid, month, member_amount, order_amount, sales, sale_amount),(l1, l1Name, l2, l2Name, l3, l3Name, l4, l4Name, l5, l5Name))) =>
        Array((l1, l1Name, 1), (l2, l2Name, 2), (l3, l2Name, 3), (l4, l4Name, 4), (l5, l5Name, 5))
          .map(s => ((s._1, s._2, s._3, com_sid, month),(member_amount, order_amount, sales, sale_amount)))}
      .flatMap(s => s).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4))
      .map(s => (s._1, (s._2._3, s._2._2, s._2._4, s._2._1, s._2._4.toDouble/s._2._1.toDouble, s._2._3.toDouble/s._2._1.toDouble, s._2._3.toDouble/s._2._2.toDouble, s._2._3.toDouble/s._2._4.toDouble)))
      .map{case ((category_id, category_name, lev, com_sid, month), (sales, order_amount, sales_amount, costomers, avg_buy_number, avg_costomer_price, avg_order_price, avg_goods_price)) =>
        ((category_id, category_name, lev, com_sid, month), (sales, order_amount, sales_amount, costomers, avg_buy_number, avg_costomer_price, avg_order_price, avg_goods_price))
      }
  }

}
