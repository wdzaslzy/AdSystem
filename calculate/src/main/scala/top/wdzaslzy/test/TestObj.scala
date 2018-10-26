package top.wdzaslzy.test

/**
  * @author lizy
  **/
object TestObj {

  def main(args: Array[String]): Unit = {
    val s = "0cf6a702ddc2429c9a1fe455ca60ef8c|166|100462|1|6.33|2018-03-13 02:54:50|202.14.246.0/24|中卫|宁夏省|2"
    val words = s.split("\\|")
    println(words.toBuffer)
    println(words(6))
  }

}
