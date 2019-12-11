package com.util

import java.sql.{Connection, DriverManager}

object jdbcConnectUtil {
  Class.forName("com.mysql.jdbc.Driver")
  private var con:Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/lab","root","951753")

  def getConnect() ={
    if (con==null){
    con = DriverManager.getConnection("jdbc:mysql://localhost:3306/lab","root","951753")
    }
    con
  }
}
