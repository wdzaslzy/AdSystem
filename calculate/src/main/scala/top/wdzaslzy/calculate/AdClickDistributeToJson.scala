package top.wdzaslzy.calculate

import java.io.FileWriter
import java.util.ResourceBundle

import org.apache.spark.sql.SparkSession
import top.wdzaslzy.util.ProvincePartitioner

/**
  * @author lizy
  **/
object AdClickDistributeToJson {

  /*
  需求说明：
  根据城市，找出该城市的点击量。每个省份在一起，点击量进行排序
   */
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local[4]")
      .appName("AdClickDistributeToJson")
      .getOrCreate()

    val bundle = ResourceBundle.getBundle("common")
    val cataLog = bundle.getString("ad_click_log_catalog")
    val outPut = bundle.getString("ad_click_distribute_output")
    val linesRdd = sparkSession.sparkContext.textFile(cataLog)

    /*
      ((province, city), 1)
      key: (province, city)
      value: 1
     */
    val provinceCityTupleRdd = linesRdd.map(line => {
      val rows = line.split("\\|")
      val city = rows(7)
      val province = rows(8)
      ((province, city), 1)
    })

    val provinces = provinceCityTupleRdd.map(_._1._1).distinct().collect()

    /*
    ((province, city), n)
    自定义分区器，多少个省多少个分区，每个分区存放相同的省份
     */
    val provinceCityReducedRdd = provinceCityTupleRdd.reduceByKey(new ProvincePartitioner(provinces), _ + _)

    provinceCityReducedRdd.foreachPartition(it => {
      val writer = new FileWriter(outPut, true)
      it.toList.sortBy(_._2).foreach(item => {
        val province = item._1._1
        val city = item._1._2
        val count = item._2
        val outputStr = s"""{"province":"$province", "city":"$city", "clickCount":"$count"}""".stripMargin
        writer.write(outputStr + "\r\n")
      })
      writer.flush()
      writer.close()
    })

    sparkSession.close()
  }

}
