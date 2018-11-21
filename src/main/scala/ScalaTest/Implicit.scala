package ScalaTest

/**
  * 隐式转换
  */
object Implicit {
  def main(args: Array[String]): Unit = {
//    implicit def man2superman(man:man):superman = new superman(man.name)
//    val man = new man("wangjx")
//    man.fly()
    implicit val name : String = "wangjx"
    def testParam(implicit name : String): Unit ={
        println(name + "~~~~~~~~")
    }
    testParam
  }
}

//class man(val name :String){
//  def eat(): Unit ={
//    println(name+"eat")
//  }
//}
//
//class superman(val name :String){
//  def fly(): Unit ={
//    println(name+"fly")
//  }
//}