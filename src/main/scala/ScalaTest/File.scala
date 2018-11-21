package ScalaTest

import java.sql.{Connection, DriverManager}

import scala.io.Source

/**
  * 读取文件 读取mysql
  */
object File {
  def main(args: Array[String]): Unit = {
    //    val file = Source.fromFile("/Users/wangjx/data/beijing.txt")
    //    for (line <- file.getLines()){
    //      println(line)
    //    }
    //val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/mysql"
    val username = "root"
    val password = "root"
    var connection:Connection = null
    try{
      classOf[com.mysql.jdbc.Driver]
      connection = DriverManager.getConnection(url,username,password)
      val stat = connection.createStatement()
      val result = stat.executeQuery("select host,user from user")
      while(result.next()){
        val host = result.getString("host")
        val user = result.getString("user")
        println(s"$host.$user")
      }
    }
    catch {
      case e:Exception => e.printStackTrace()
    }
    finally {
      if(connection==null){
        connection.close()
      }
    }
  }
}
