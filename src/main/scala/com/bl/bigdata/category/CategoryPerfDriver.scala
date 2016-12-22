package com.bl.bigdata.category

import com.bl.bigdata.util.SparkFactory

/**
 * Created by HJT20 on 2016/9/27.
 */
//This is solved by:-
//1. Add the mysql connector (mysql-connector-java.jar) to the spark->lib directory
//2. Add below at the end of spark/bin/compute-classpath.sh
//
//appendToClasspath "${assembly_folder}"/mysql-connector-java.jar
class CategoryPerfDriver {

}

object CategoryPerfDriver {
  def main(args: Array[String]) {
    val hiveContext = SparkFactory.getHiveContext
    val no = args(0).toInt
    no match {
      case 0=>
        //基本信息表更新
        println("bl_category_performance_basic")
        CategoryStatistic.bl_category_performance_basic(hiveContext)
        //  匹配一号店品类CategoryMatch.main(null)

        //品类品牌基本信息
        println("bl_category_performance_category_brand")
        CategoryStatistic.bl_category_performance_category_brand(hiveContext)

        //销量
        println("bl_category_performance_category_monthly_sales")
        CategoryStatistic.bl_category_performance_category_monthly_sales(hiveContext)

        //品类热销品
        println("bl_category_performance_category_monthly_hotcakes")
        CategoryStatistic.bl_category_performance_category_monthly_hotcakes(hiveContext)

        //人气
        println("bl_category_performance_category_popularity")
        CategoryStatistic.bl_category_performance_category_popularity(hiveContext)

       //存售比
        println("bl_category_performance_monthly_goods_shelf_sales_ratio")
        CategoryStatistic.bl_category_performance_monthly_goods_shelf_sales_ratio(hiveContext)

        //营运
        println("bl_category_performance_category_operation")
        CategoryStatistic.bl_category_performance_category_operation(hiveContext)

       //百联商品价格分布
        println("bl_category_performance_category_price_conf")
       CategoryBLGoodsPrice.main(null)

       //销售得分
        println("bl_category_performance_category_sale_score")
       CategorySalesComp.main(null)

       //配置得分
        println("bl_category_performance_product_line_score")
        CategoryGoodsDist.main(null)

       //人气
        println("bl_category_performance_category_popularity_score")
        CategoryPopularity.main(null)

       //运营
         println("bl_category_performance_category_operation_score")
         CategoryOperation.main(null)

        //品类综合得分
         println("bl_category_performance_score")
         CategoryAnalysis.main(null)

        //yhd bl品牌对比
        println("bl_category_performance_bl_yhd_brand_contrast")
         CategoryBLYhdBrandsContrast.main(null)

        //yhd品类商品价格，不需要每天更新
        println("bl_category_performance_category_yhd_price_dist")
         CategoryYhdPrice.main(null)

          //yhd商品匹配
        println("bl_yhd_items")
         CategoryYhdItemMatch.main(null)

      case 1=>
        //基本信息表更新
        println("bl_category_performance_basic")
        CategoryStatistic.bl_category_performance_basic(hiveContext)
     /* case 2=>
        println(" CategoryMatch")
        // 2. 匹配一号店品类
        CategoryMatch.main(null)*/
      case 2=>
        //营运
        println("bl_category_performance_monthly_goods_shelf_sales_ratio")
        CategoryStatistic.bl_category_performance_category_operation(hiveContext)
      case 3 =>
        //品类品牌基本信息
        println("bl_category_performance_category_brand")
        CategoryStatistic.bl_category_performance_category_brand(hiveContext)
      case 4=>
        //销量
        println("bl_category_performance_category_monthly_sales")
        CategoryStatistic.bl_category_performance_category_monthly_sales(hiveContext)
      case 5=>
        //品类热销品
        println("bl_category_performance_category_monthly_hotcakes")
        CategoryStatistic.bl_category_performance_category_monthly_hotcakes(hiveContext)
      case 6=>
        //人气
        println("bl_category_performance_category_popularity")
        CategoryStatistic.bl_category_performance_category_popularity(hiveContext)
      case 7=>
        //存售比
        println("bl_category_performance_monthly_goods_shelf_sales_ratio")
        CategoryStatistic.bl_category_performance_monthly_goods_shelf_sales_ratio(hiveContext)
      case 8 =>
        //百联商品价格分布
        println("bl_category_performance_category_price_conf")
        CategoryBLGoodsPrice.main(null)
      case 9 =>
        //销售得分
        println("bl_category_performance_category_sale_score")
        CategorySalesComp.main(null)
      case 10 =>
        //配置得分
        println("bl_category_performance_product_line_score")
        CategoryGoodsDist.main(null)
      case 11 =>
        //人气
        println("bl_category_performance_category_popularity_score")
        CategoryPopularity.main(null)
      case 12 =>
        //运营
        println("bl_category_performance_category_operation_score")
        CategoryOperation.main(null)
      case 13 =>
        //品类综合得分
        println("bl_category_performance_score")
        CategoryAnalysis.main(null)
      case 14 =>
        //yhd bl品牌对比
        println("bl_category_performance_bl_yhd_brand_contrast")
        CategoryBLYhdBrandsContrast.main(null)
      case 15 =>
        //yhd品类商品价格，不需要每天更新
        println("bl_category_performance_category_yhd_price_dist")
        CategoryYhdPrice.main(null)
      case 16 =>
        //yhd商品匹配
        println("bl_yhd_items")
        CategoryYhdItemMatch.main(null)
    }


    //
  }
}
