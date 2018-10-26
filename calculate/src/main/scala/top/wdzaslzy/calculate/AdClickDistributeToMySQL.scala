package top.wdzaslzy.calculate

import java.util.{Properties, ResourceBundle}

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @author lizy
  **/
object AdClickDistributeToMySQL {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("AdClickDistributeToMySQL")
      .master("local[*]")
      .getOrCreate()

    val bundle = ResourceBundle.getBundle("common")
    val cataLog = bundle.getString("ad_click_log_catalog")
    val jdbcUrl = bundle.getString("mysql_jdbc_url")
    val user = bundle.getString("mysql_user")
    val password = bundle.getString("mysql_password")
    val dataSet = sparkSession.read.textFile(cataLog)

    import sparkSession.implicits._
    val dataFrame = dataSet.map(line => {
      val rows = line.split("\\|")
      val city = rows(7)
      val province = rows(8)
      (province, city)
    }).toDF("province", "city")

    dataFrame.createTempView("t_ad_click_log")
    val resultFrame = sparkSession.sql("select province, city, count(*) as clickCount from t_ad_click_log group by province, city")

    val properties = new Properties()
    properties.setProperty("user", user)
    properties.setProperty("password", password)
    resultFrame.write.mode(SaveMode.Append).jdbc(jdbcUrl, "t_ad_click_distribute", properties)

    sparkSession.close()
  }

}
