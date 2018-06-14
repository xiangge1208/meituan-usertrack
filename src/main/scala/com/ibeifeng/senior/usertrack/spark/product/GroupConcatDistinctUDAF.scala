package com.ibeifeng.senior.usertrack.spark.product

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

/**
  * Created by ibf on 03/18.
  */
object GroupConcatDistinctUDAF extends UserDefinedAggregateFunction {
  val delimiter = ","

  /**
    * 给定输入数据类型
    *
    * @return
    */
  override def inputSchema: StructType = {
    StructType(
      Array(
        StructField("str", StringType, true)
      )
    )
  }

  /**
    * 数据聚合过程中，需要考虑数据的临时保存，这里需要指定保存数据类型
    *
    * @return
    */
  override def bufferSchema: StructType = {
    StructType(
      Array(
        StructField("bufferStr", StringType, true)
      )
    )
  }

  /**
    * 返回数据结果类型
    *
    * @return
    */
  override def dataType: DataType = StringType

  /**
    * 是否支持模糊查询(多次查询返回结果是否一致)； true表示不支持，返回结果一致；false相反
    *
    * @return
    */
  override def deterministic: Boolean = true

  /**
    * 缓存buffer中的数据进行初始化操作
    *
    * @param buffer
    */
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, "") // 给定初始化的值为""
  }

  /**
    * 根据输入的数据，更新缓冲区中的值
    *
    * @param buffer
    * @param input
    */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    // 获取缓存区中的值
    val bufferValue = buffer.getString(0)

    // 获取传递进来的值
    val inputValue = input.getString(0)

    // 合并数据
    val mergedValue = mergeValue(bufferValue, inputValue)

    // 更新缓存区中的值
    buffer.update(0, mergedValue)
  }

  /**
    * 合并两个缓冲区的值，该方法只有在多个分区聚合数据在合并的时候才会使用到
    *
    * @param buffer1
    * @param buffer2
    */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    // 获取缓冲区中的值
    var buffer1Value = buffer1.getString(0)
    val buffer2Value = buffer2.getString(0)

    // 合并数据
    for (tmpStr <- buffer2Value.split(delimiter)) {
      // tmpStr的格式类似"101:上海:25"
      val arr = tmpStr.split(":")
      buffer1Value = mergeValue(buffer1Value, s"${arr(0)}:${arr(1)}", arr(2).toInt)
    }

    // 更新缓冲区
    buffer1.update(0, buffer1Value)
  }

  override def evaluate(buffer: Row): Any = {
    // 最终结果返回
    buffer.getString(0)
  }

  /**
    * 将value合并到buffer中，同时注意数据去重操作
    *
    * @param buffer 缓存数据，格式类似: "101:上海:25,102:杭州:30"；使用","分割不同个体，使用":"在个体中分割不同字段信息
    * @param value  需要合并的数据，格式类似: "101:上海"； 以:进行数据分割
    * @param count  value对应出现的次数, 默认的出现次数
    * @return
    */
  private def mergeValue(buffer: String, value: String, count: Int = 1): String = {
    // 如果buffer中不包含value，直接在buffer的尾部添加一个新的个体即可；如果包含，找到对应的个体，然后将其次数增加一次
    if (buffer.contains(value)) {
      // 更新操作
      buffer
        .split(delimiter)
        .map(v => {
          // 数据v格式为: "id:name:count", eg: "101:上海:25"
          if (v.contains(value)) {
            // 更新v中的值 ==> 只会出现一次
            s"${value}:${v.split(":")(2).toInt + count}"
          } else {
            v
          }
        })
        .mkString(delimiter)
    } else {
      // 插入操作； 如果buffer为空，直接返回value；否则添加一个分隔符后，再返回追加后的结果
      if ("".equals(buffer)) {
        s"${value}:${count}"
      } else {
        s"${buffer}${delimiter}${value}:${count}"
      }
    }
  }
}
