package ScalaTest

/**
  * 模式匹配
  */
object MatchCase {
  def main(args: Array[String]): Unit = {
    greeting(List("wangjx","lisi","wang"))
  }


  def greeting(list:List[String]): Unit ={
    list match {
      case wangjx::Nil => println("1")
      case x::y::Nil => println("2")
      case wangjx::tail => println("3")
      case _ => println("4")
    }
  }
}
