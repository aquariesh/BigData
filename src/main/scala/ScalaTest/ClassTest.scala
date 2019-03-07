package ScalaTest

/**
  * 伴生对象与伴生类相互访问
  */
object ClassTest {
  def main(args: Array[String]): Unit = {
    val p = new person("wjx",28)
    //p.name
    val txt = scala.io.Source.fromFile("").mkString
    val p2= person
  }
}



class person(val name:String,val age:Int) {
  private[this] val school = "Buu"
  private var gender = "male"
  def this (name:String,age: Int,gender:String){
    this(name,age)
    this.gender=gender
  }
  //person.name
}

object person{
  private val name = "wjx"
  val p = new person("",1)

  def apply(): Unit ={
    new person("wjx",28)
  }
}