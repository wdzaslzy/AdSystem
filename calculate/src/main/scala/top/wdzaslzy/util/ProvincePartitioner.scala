package top.wdzaslzy.util

import org.apache.spark.Partitioner

import scala.collection.mutable

/**
  * 省份分区器
  *
  * @author lizy
  **/
class ProvincePartitioner(provinces: Array[String]) extends Partitioner {
  private val ruleMap = new mutable.HashMap[String, Int]()
  var index: Int = 0
  provinces.foreach(province => {
    ruleMap.put(province, index)
    index += 1
  })

  override def numPartitions: Int = provinces.length

  override def getPartition(key: Any): Int = ruleMap(key.asInstanceOf[(String, String)]._1)
}
