package top.wdzaslzy.calculate

import java.util.ResourceBundle

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * @author lizy
  **/
object AdClickDistributeToJson2 {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local[4]")
      .appName("AdClickDistributeToJson")
      .getOrCreate()

    val bundle = ResourceBundle.getBundle("common")
    val cataLog = bundle.getString("ad_click_log_catalog")
    val outPut = bundle.getString("ad_click_distribute_output")

    val logDataSet = sparkSession.read.textFile(cataLog)
    import sparkSession.implicits._
    val rowDataSet = logDataSet.map(line => {
      val rows = line.split("\\|")
      val city = rows(7)
      val province = rows(8)
      (province, city)
    })
    //方式一：转换为DataFrame
    val frame: DataFrame = rowDataSet.toDF("province", "city")
    frame.createTempView("t_ad_click_log")
    val resultFrame = sparkSession.sql("select province, city, count(*) as clickCount from t_ad_click_log group by province, city")

    resultFrame.write.json(outPut)

    sparkSession.close()
  }

}
