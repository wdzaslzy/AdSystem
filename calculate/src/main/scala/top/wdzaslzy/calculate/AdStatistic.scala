package top.wdzaslzy.calculate

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import top.wdzaslzy.bean.AdClickLog
import top.wdzaslzy.util.EffectiveSum

/**
  * @author lizy
  **/
object AdStatistic {

  /*
  统计出每个城市有效的数
  城市 参与竞价数 竞价成功数 竞价成功率 展示量 点击量 点击率 广告成本 广告消费
   */
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("AdStatistic")
      .master("local[4]")
      .getOrCreate()

    //方式一
    //    method1(sparkSession)

    //方式二
    method2(sparkSession)

    sparkSession.close()
  }

  //使用Spark-Core形式统计
  def method1(sparkSession: SparkSession): Unit = {
    //读取数据
    val linesRdd = sparkSession.sparkContext.textFile("E:\\adsystem\\log\\adClickLog.txt")

    val adStatistics = new AdClickLog
    val adClickLogRdd = linesRdd.map(lineToAdClickLog)
    val logByCity: RDD[(String, AdClickLog)] = adClickLogRdd.map(log => {
      (log.getCity, log)
    })
    val logByCityReduced: RDD[(String, AdClickLog)] = logByCity.reduceByKey((log1, log2) => {
      log1.addTotal(1)
      if (log2.getIsEffective == 1) {
        log1.addTotalEffective(1)
        log1.setTotalPrice(log1.getTotalPrice + log2.getAdPrice)
      }
      log1.setEffectiveRate(log1.getTotalEffective.toDouble / log1.getTotal)
      if (log2.getPutInType == 1) {
        log1.addShowTypeTotal(1)
      } else {
        log1.addClickTypeTotal(1)
      }
      log1.setClickRate(log1.getClickTypeTotal.toDouble / log2.getShowTypeTotal)

      adStatistics.addTotal(log1.getTotal)
      adStatistics.addTotalEffective(log1.getTotalEffective)


      adStatistics
    })

    val result = logByCityReduced.collect()
    println(result.toBuffer)
  }

  //使用Spark-SQL统计
  def method2(sparkSession: SparkSession): Unit = {
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

    sparkSession.udf.register("effectSum", new EffectiveSum)
    val resultFrame = sparkSession.sql("select city, count(sessionId) as count, effectSum(isEffective) as effectCount " +
      "from t_ad_click_log group by city")
    resultFrame.show()
  }

  def lineToAdClickLog(line: String): AdClickLog = {
    val infos = line.split("\\|")
    AdClickLog().setSessionId(infos(0))
      .setAdvertisersId(infos(1).toInt)
      .setAdvertisementId(infos(2).toInt)
      .setPutInType(infos(3).toInt)
      .setAdPrice(infos(4).toDouble)
      .setRequestTime(infos(5))
      .setRequestIp(infos(6))
      .setCity(infos(7))
      .setProvince(infos(8))
      .setEffective(infos(9).toInt)
  }
}
