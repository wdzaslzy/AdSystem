package top.wdzaslzy.calculate

import org.apache.spark.sql.SparkSession

/**
  * @author lizy
  **/
object AdAreaAnalyse {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("AdAreaAnalyse")
      .master("local[4]")
      .getOrCreate()

    method1(sparkSession)

    sparkSession.close()
  }

  //SQL形式实现
  def method1(sparkSession: SparkSession): Unit = {
    val dataSet = sparkSession.read.textFile("E:\\adsystem\\log\\adClickLog.txt")
    import sparkSession.implicits._
    val clickLogDataSet = dataSet.map(data => {
      //      lineToAdClickLog(data)
      val infos = data.split("\\|")
      (infos(0), infos(1).toInt, infos(2).toInt, infos(3).toInt, infos(4).toDouble,
        infos(5), infos(6), infos(7), infos(8), infos(9).toInt)
    })
    val clickLogFrame = clickLogDataSet.toDF("sessionId", "advertisersId", "advertisementId", "putInType",
      "adPrice", "requestTime", "requestIp", "city", "province", "isEffective")

    clickLogFrame.createTempView("t_ad_click_log")

    //与自己实现不同
    val resultFrame = sparkSession.sql(
      """
        |select
        |province, city,
        |count(sessionId) as total,
        |sum(case
        | when isEffective = 1 then 1
        | when isEffective = 2 then 0
        | end
        |) as totalEffective,
        |sum(case putInType
        | when 1 then 1
        | end
        |) as showTotal,
        |sum(case putInType
        | when 2 then 1
        | end
        |) as clickTotal,
        |sum(case isEffective
        | when 1 then adPrice
        | end
        |) as totalPrice
        |from t_ad_click_log
        |group by province, city
      """.stripMargin)

    resultFrame.show()
  }

  def method2(sparkSession: SparkSession): Unit = {}

}
