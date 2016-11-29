package com.bl.bigdata.util

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
  * Created by YQ85 on 2016/11/23.
  */

object MysqlClear_48_3 {
  def main(args: Array[String]) : Unit = {
    val tableName = args(0)
    var conn: Connection = null
    var pt: PreparedStatement = null
    try {
      val driver = "com.mysql.jdbc.Driver"
      Class.forName(driver)
      conn = DriverManager.getConnection("jdbc:mysql://10.201.48.3:3306/category_performance", "bl", "bigdata")
      val sql = "delete from " + tableName;
      conn.setAutoCommit(false)
      pt = conn.prepareStatement(sql)
      pt.executeUpdate()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (pt != null) {
        pt.close()
      }
      if (conn != null) {
        conn.close()
      }
    }

  }
}
