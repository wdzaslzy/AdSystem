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

    //spark sql来实现
//    method1(sparkSession)

    //spark core来实现
    method2(sparkSession)

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

  //CORE形式
  def method2(sparkSession: SparkSession): Unit = {
    val lineRdd = sparkSession.sparkContext.textFile("E:\\adsystem\\log\\adClickLog.txt")
    //1. 取出所有字段
    val fieldRdd = lineRdd.map(line => {
      val infos = line.split("\\|")
      (infos(0), infos(1).toInt, infos(2).toInt, infos(3).toInt, infos(4).toDouble,
        infos(5), infos(6), infos(7), infos(8), infos(9).toInt)
    })
    //2. 将字段转换为有效指标
    val targetRdd = fieldRdd.map(fields => {
      var num = 1
      var effectNum = 0
      var showNum = 0
      var clickNum = 0
      var price = 0.0

      if (fields._10 == 1) {
        effectNum += 1
        price += fields._5
      }

      if (fields._4 == 1) {
        showNum += 1
      } else {
        clickNum += 1
      }

      //城市, (省份，数量，有效数量，展示量，点击量，价格)
      (fields._8, (fields._9, num, effectNum, showNum, clickNum, price))
    })

    val reducedRdd = targetRdd.reduceByKey((a, b) => {
      (a._1, a._2 + b._2, a._3 + b._3, a._4 + b._4, a._5 + b._5, a._6 + b._6)
    })
    val result = reducedRdd.collect()

    println(result.toBuffer)
  }

}
