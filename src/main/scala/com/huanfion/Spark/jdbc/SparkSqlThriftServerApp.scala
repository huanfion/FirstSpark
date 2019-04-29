package com.huanfion.Spark.jdbc

import java.sql.DriverManager

/*
* 通过JDBC的方式访问
*     <dependency>
      <groupId>org.spark-project.hive</groupId>
      <artifactId>hive-jdbc</artifactId>
      <version>1.2.1.spark2</version>
    </dependency>
* */
object SparkSqlThriftServerApp {
  def main(args: Array[String]): Unit = {
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    val conn=DriverManager.getConnection("jdbc:hive2://master:10000","root","")

    val pstmt=conn.prepareStatement("select order_id,user_id,eval_set from badou.orders limit 10")
    val rs=pstmt.executeQuery()
    while (rs.next()){
      println("order_id:"+rs.getInt("order_id")+
      ",user_id:"+rs.getInt("user_id")+
        ",eval_set:"+rs.getString("eval_set")
      )
    }
    rs.close()
    pstmt.close()
    conn.close()
  }
}
