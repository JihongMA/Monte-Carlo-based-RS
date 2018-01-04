package com.huawei.datasight.mllib.rec

import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.mutable.WrappedArray

/**
  * Compute the distribution of the tag-count from uses' watching history
  * e.g. Suppose the tags of 3 videos are as follows:
  * Input:
  * (0 1 1) \\ the video contains tag_2 and tag_3
  * (0 0 1) \\ the video contains tag_3
  * (1 0 1) \\ the video contains tag_1 and tag_3

  * Output: the distribution of the counts of the above 3 vidoes. prob_i = count_i / total_tags.
  * e.g. prob_1 = 1 / (1 + 1 + 3) = 0.2
  * (0.2, 0.2, 0.6) //

  * @param nTag
  */


class ComputeProb(nTag: Int) extends UserDefinedAggregateFunction {
  // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("tags", ArrayType(ByteType, false)) :: Nil)

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(
    StructField("sum_tag_array", ArrayType(LongType, false))
      :: Nil
  )

  // This is the output type of your aggregatation function.
  override def dataType: DataType = ArrayType(DoubleType, false)

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Array.fill[Long](nTag)(0L)
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[WrappedArray[Long]](0).
      zip(input.getAs[WrappedArray[Byte]](0)).
      map(t => t._1 + t._2.toLong)
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[WrappedArray[Long]](0).
      zip(buffer2.getAs[WrappedArray[Long]](0)).
      map(t => t._1 + t._2)
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    val total = buffer.getAs[WrappedArray[Long]](0).foldLeft(0L)(_ + _)
    buffer.getAs[WrappedArray[Long]](0).
      map( v => v.toDouble/total)
  }

}