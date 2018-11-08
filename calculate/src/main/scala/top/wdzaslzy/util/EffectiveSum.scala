package top.wdzaslzy.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, IntegerType, StructField, StructType}

/**
  * @author lizy
  **/
class EffectiveSum extends UserDefinedAggregateFunction{
  override def inputSchema: StructType = StructType(List(
    StructField("value", IntegerType)))

  override def bufferSchema: StructType = StructType(List(
    StructField("products", IntegerType)))

  override def dataType: DataType = IntegerType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (input.getInt(0) == 1) {
      buffer(0) = buffer.getInt(0) + 1
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getInt(0) + buffer2.getInt(0)
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getInt(0)
  }
}
