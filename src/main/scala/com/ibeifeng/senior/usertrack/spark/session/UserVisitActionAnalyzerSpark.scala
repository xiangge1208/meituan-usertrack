package com.ibeifeng.senior.usertrack.spark.session

import java.sql.Connection

import com.alibaba.fastjson.{JSONArray, JSONObject}
import com.ibeifeng.senior.usertrack.conf.ConfigurationManager
import com.ibeifeng.senior.usertrack.constant.Constants
import com.ibeifeng.senior.usertrack.dao.factory.DAOFactory
import com.ibeifeng.senior.usertrack.jdbc.JDBCHelper
import com.ibeifeng.senior.usertrack.mock.MockDataUtils
import com.ibeifeng.senior.usertrack.spark.util.{JSONUtil, SQLContextUtil, SparkConfUtil, SparkContextUtil}
import com.ibeifeng.senior.usertrack.util.DateUtils.DateTypeEnum
import com.ibeifeng.senior.usertrack.util.{DateUtils, ParamUtils, StringUtils}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Random, Success, Try}

/**
  * Created by ibf on 10/26.
  */
object UserVisitActionAnalyzerSpark {
  def main(args: Array[String]): Unit = {
    // 一、过滤参数的获取
    // 1.1 获取传入的任务id
    val taskID = ParamUtils.getTaskIdFromArgs(args)
    // 1.2 根据taskid从数据库中获取任务对象
    /**
      * 方式：
      * 1. 基于原始的JDBC/ODBC的代码实现数据的获取
      * 2. 集成第三方的框架, eg: mybatis....
      * 3. 使用SparkSQL来读取JDBC的数据
      * 注意：数据库连接对象的创建数量 ==> 自定义一个数据库连接池，限制一个连接的上限
      */
    val task = if (taskID == null) {
      throw new IllegalArgumentException(s"无效任务参数:${taskID}")
    } else {
      // a. 获取操作对象
      val taskDao = DAOFactory.getTaskDAO
      // b. 基于任务id获取task对象
      taskDao.findByTaskId(taskID)
    }
    // 1.3 获取过滤参数（需要将json格式字符串的数据过滤值转换为JSON对象）
    if (task == null) {
      throw new IllegalArgumentException(s"从数据库中没法获取对应id的任务对象:${taskID}")
    }
    val taskParam: JSONObject = ParamUtils.getTaskParam(task)
    if (taskParam == null || taskParam.isEmpty) {
      throw new IllegalArgumentException(s"不支持过滤参数为空的任务:${taskID}")
    }

    // 二、上下文的创建
    // 2.1 参数的获取
    val appName = Constants.SPARK_APP_NAME_SESSION + taskID
    val isLocal = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)
    // 2.2 SparkConf的创建
    val conf = SparkConfUtil.generateSparkConf(appName, isLocal, that => {
      // 这里可以单独的指定当前spark应用的相关参数
      // nothings
    })
    // 2.3 SparkContext对象的构建
    val sc = SparkContextUtil.getSparkContext(conf)
    // 2.4 SQLContext对象的创建
    /**
      * 实际运行环境中，数据一定是在Hive表中，所以一定要构建HiveContext对象；但是在开发测过过程中，由于连接hive一般麻烦，而且执行速度比较慢；所以经常在开发过程中，使用模拟数据进行测试运行
      */
    val sqlContext = SQLContextUtil.getInstance(
      sc,
      integratedHive = !isLocal,
      generateMockData = (sc, sqlContext) => {
        // 本地模式的情况下，进行数据的模拟操作 ==> 模拟出一个表
        if (isLocal) {
          MockDataUtils.mockData(sc, sqlContext)
        }
      }
    )

    // 三、数据读取及任务参数过滤，并形成RDD
    val actionRDD = this.getActionRDDByFilter(sqlContext, taskParam)

    // 四、数据按照session id进行聚合
    // 原因：后续的数据处理基本上都是一个会话一个会话的进行处理，所以提前将数据进行聚合操作
    // 这里使用groupByKey的主要原因是：只能用该API；一个会话中的数据量不会太大，也就不会导致内存溢出等问题
    val sessionID2RecordsRDD = actionRDD
      .map(record => (record.sessionId, record))
      .groupByKey()
    sessionID2RecordsRDD.cache()

    // 四、需求一代码
    /**
      * 用户的session聚合统计
      * 主要统计两个指标：会话数量&会话长度
      * 会话数量：sessionID的数量
      * 会话长度：一个会话中，最后一条访问记录的时间-第一条记录的访问数据
      * 具体的指标：
      * 1. 总的会话个数：过滤后RDD中sessionID的数量
      * 2. 总的会话长度(单位秒): 过滤后RDD中所有session的长度的和
      * 3. 无效会话数量：会话长度小于1秒的会话id数量
      * 4. 各个不同会话长度区间段中的会话数量
      * 0-4s/5-10s/11-30s/31-60s/1-3min/3-6min/6min+ ==> A\B\C\D\E\F\G
      * 5. 计算各个小时段的会话数量
      * 6. 计算各个小时段的会话长度
      * 7. 计算各个小时段的无效会话数量
      *
      * 注意：如果一个会话中，访问数据跨小时存在，eg：8:59访问第一个页面,9:02访问第二个页面；把这个会话计算在两个小时中(分别计算)
      **/
    val totalSessionCount: Long = sessionID2RecordsRDD.count()
    val sessionID2LengthRDD = sessionID2RecordsRDD.map {
      case (sessionID, records) => {
        // 1. 获取当前会话中的所有数据的访问时间戳(毫米级)
        val actionTimestamps = records.map(record => {
          // actionTime格式为yyyy-MM-dd HH:mm:ss的时间字符串
          val actionTime = record.actionTime
          val timestamp = DateUtils.parseString2Long(actionTime)
          timestamp
        })

        // 2. 获取第一条数据的时间戳(最小值)和最后一条数据的时间戳(最大值)
        val minActionTimestamp = actionTimestamps.min
        val maxActionTimestamp = actionTimestamps.max
        val length = maxActionTimestamp - minActionTimestamp

        // 3. 返回结果
        // TODO: 这里的sessionID是否需要进行输出操作？？？ ===>  不需要的
        (sessionID, length)
      }
    }
    sessionID2LengthRDD.cache()
    val totalSessionLength: Double = sessionID2LengthRDD.map(_._2).sum() / 1000
    val invalidSessionCount: Long = sessionID2LengthRDD.filter(_._2 < 1000).count()
    val preSessionLengthLevelSessionCount = sessionID2LengthRDD
      .map {
        case (_, length) => {
          // 根据会话长度得到当前会话的级别
          val sessionLevel = {
            if (length < 5000) "A"
            else if (length < 11000) "B"
            else if (length < 31000) "C"
            else if (length < 60000) "D"
            else if (length < 180000) "E"
            else if (length < 360000) "F"
            else "G"
          }

          // 返回结果
          (sessionLevel, 1)
        }
      }
      .reduceByKey(_ + _)
      .collect()
    sessionID2LengthRDD.unpersist()

    // 计算各个小时段各个会话的会话长度
    val dayAndHour2SessionLengthRDD: RDD[((String, Int), Long)] = sessionID2RecordsRDD.flatMap {
      case (sessionID, records) => {
        //1. 获取当前会话中的记录操作对应的时间
        val dayAndHourAndSessionID2TimestampIter = records.map(record => {
          // actionTime格式为yyyy-MM-dd HH:mm:ss的时间字符串
          val actionTime = record.actionTime
          val timestamp = DateUtils.parseString2Long(actionTime)
          // 获取得到day和hour两个时间值
          val day = DateUtils.parseLong2String(timestamp, DateUtils.DATE_FORMAT)
          val hour = DateUtils.getSpecificDateValueOfDateTypeEnum(timestamp, DateTypeEnum.HOUR)
          ((day, hour, record.sessionId), timestamp)
        })

        // 2. 计算各个时间段各个会话中的会话长度
        val dayAndHour2LengthIter = dayAndHourAndSessionID2TimestampIter
          .groupBy(_._1)
          .map {
            case ((day, hour, sessionID), iter) => {
              val times = iter.map(_._2)
              // a. 获取最大值和最小值
              val maxTimes = times.max
              val minTimes = times.min
              // b. 计算长度
              val length = maxTimes - minTimes
              // c. 返回结果
              ((day, hour), length)
            }
          }

        // 3. 返回结果
        dayAndHour2LengthIter
      }
    }
    dayAndHour2SessionLengthRDD.cache()
    val preDayAndHourOfSessionCount = dayAndHour2SessionLengthRDD
      .map(tuple => (tuple._1, 1))
      .reduceByKey(_ + _)
      .collect()
    val preDayAndHourOfSessionLength: Array[((String, Int), Long)] = dayAndHour2SessionLengthRDD
      .reduceByKey(_ + _)
      .map(tuple => (tuple._1, tuple._2 / 1000))
      .collect()
    val preDayAndHourOfInvalidSessionCount = dayAndHour2SessionLengthRDD
      .filter(t => t._2 < 1000)
      .map(tuple => (tuple._1, 1))
      .reduceByKey(_ + _)
      .collect()
    dayAndHour2SessionLengthRDD.unpersist()

    this.saveSessionAggrResult(sc, taskID, totalSessionCount, totalSessionLength, invalidSessionCount, preSessionLengthLevelSessionCount, preDayAndHourOfSessionCount, preDayAndHourOfSessionLength, preDayAndHourOfInvalidSessionCount)


    // 六、需求二实现
    /**
      * 按照给定的比例从每个小时区间段中抽取session的数据(详细数据)，并且将数据保存到一个HDFS上的文本文件中，同时将每个小时抽取出来的session中的访问次数最多的前10个session的详细信息输出到RDBMs中
      * 1. 获得/抽取最终session的sessionID；抽取的sessionID按照小时进行分布的
      * 2. 将抽取得到的sessionID和session具体信息进行join操作，得到最终的抽取session的详细会话信息
      * 3. 结果保存HDFS
      * 4. 从抽取结果中获取每个小时段访问次数最多的10个session将数据保存到JDBC中
      * 备注：这里的小时段考虑天和小时
      * eg:
      * 一天的session数量10万
      * 给定的比率是1%(1000个session)
      * -1. 先计算每个小时段的session数量
      * 0-1: 1000个
      * .....
      * 9-10: 20000个
      * ......
      * -2. 计算出每个小时段需要被抽取的最少session数量
      * 0-1: 1个
      * ....
      * 9-10: 200个
      * ......
      * -3. 按照给定的数量量随机获取session的数据
      * 功能：为了方便观察者对每个会话的访问轨迹有一定的了解，可以从中得到一些用户的操作习惯，对产生的优化有一定的帮助；而且观察整体的数据，由于数据量太大，很难得到一个结果，但是随机抽样的形式可以保证最终结果的公平性
      **/
    // 从任务参数中获取比率
    val ratio: Double = {
      // 获取参数
      val param: Option[String] = ParamUtils.getParam(taskParam, Constants.PARAM_SESSION_RATIO)
      // Option => Some & None ==> 表示有值无值的
      // Try => Success & Failure ==> 表示执行成功和执行异常
      // value值：-1表示param没有设值，-2表示设的值不是double数据类型，大于0表示正常值
      var value = Try(if (param.isDefined) param.get.toDouble else -1.0).getOrElse(-2.0)

      // 当设置的比例值超过范围的时候(0,0.1]，设置为默认值0.01(1%)
      if (value <= 0 || value > 0.1) {
        value = 0.01
      }

      // 返回结果
      value
    }
    // 从任务参数中获取数据抽样规则
    val sampleType = ParamUtils.getParam(taskParam, Constants.PARAM_SAMPLE_TYPE)
      .filter(_.nonEmpty)
      .getOrElse("bernoulli")

    // 创建最基本的数据抽样原始RDD对象，类型: RDD[((day,hour), sessionID]
    val baseSampleDataRDD: RDD[((String, Int), String)] = sessionID2RecordsRDD.flatMap {
      case (sessionID, records) => {
        // 获取当前记录中操作的时间值，返回结果是:(day, hour)
        val day2HourIter = records.map(record => {
          val actionTime = record.actionTime
          val timestamp = DateUtils.parseString2Long(actionTime)
          // 获取day(yyyy-MM-dd)和hour(小时数，24小时制)
          val day = DateUtils.parseLong2String(timestamp, DateUtils.DATE_FORMAT)
          val hour = DateUtils.getSpecificDateValueOfDateTypeEnum(timestamp, DateTypeEnum.HOUR)

          // 返回结果
          (day, hour)
        })

        // 数据转换并进行输出
        day2HourIter.map(tuple => (tuple, sessionID))
      }
    }
    baseSampleDataRDD.cache()

    // 基于baseSampleDataRDD进行数据抽样
    // 由于可以使用RDD提供的专门用于随机抽样的方法进行数据抽取
    // withReplacement：指定的是数据抽样的策略，当该参数为true的时候，采用泊松分布(poisson)来进行数据抽样；为false的时候采用伯努利分布(bernoulli)进行数据抽样;
    // bernoulli分布就是一个二项分布，要不是，要不不是； ration几率是，1-ration几率不是: http://baike.baidu.com/link?url=0VdeMT1Av6j1QFQ6CQgB0NgWCt9_M_uSVeojrSYFMTOvy93WbLxgIecoVMiDL2U6UCWGSB2BvgAUAwN6sktMOQHxA-yRQvfRck8dL3spzCubshZJBuDkSDZEhw0GGnAL ; http://baike.baidu.com/link?url=ExRsGwufuQS4VDuBMVoXq39iwUooNxaOVDVKEbV7MGrjRE8SMSP79LVks3QBQDQtKf9_ibH3J8wfD2Qc7WZmDUeHlS7PTSqn6rePD-_-JhCy5VN2mByjupU488OcskJF
    // poisson相对于bernoulli分布不是简单的是或者不是的机制: http://baike.baidu.com/item/%E6%B3%8A%E6%9D%BE%E5%88%86%E5%B8%83
    // fractions: 每组key中数据抽样的比例值
    val sampleDataByAPIRDD = baseSampleDataRDD.sampleByKey(
      withReplacement = "poisson".equals(sampleType),
      fractions = preDayAndHourOfSessionCount.map(tuple => (tuple._1, ratio)).toMap
    )
    sampleDataByAPIRDD.cache()

    // 通过sampleByKey函数，在抽样的过程中可能导致某些时间段没有抽取到数据或者抽取的数据不够，所以需要补充数据
    // 1. 计算一下每个时间段最少需要的样本数据量，假设每个时间段最少3个样本数据，最多50个样本(如果样本数达到50个，但是比率没有达到，认为样本数量足够)
    val preDayAndHour2SampleData = preDayAndHourOfSessionCount.map {
      case ((day, hour), count) => {
        val rationCount = count * ratio
        ((day, hour), (Math.min(Math.max(3, rationCount.toInt), 50), Array.empty[String]))
      }
    }.toMap
    // TODO: 为什么使用广播变量? 广播变量的作用? 广播变量的原理?
    val broadcastOfDayAndHour2Count = sc.broadcast(preDayAndHour2SampleData)
    // 2. 计算出已经抽样符合条件的时间段和不符合条件的时间段
    val preDayAndHour2sampledData = sampleDataByAPIRDD
      .aggregateByKey((0, ArrayBuffer[String]()))(
        (u, v) => {
          // newNumber表示当前key以及抽取的数据量；
          // v表示的是sampleDataByAPIRDD中当前key区间中，被抽取出来的一个sessionid
          val newNumber = u._1 + 1
          if (newNumber >= 50) {
            // 这个时候不需要进行sid的传输
            (newNumber, ArrayBuffer.empty[String])
          } else {
            // 需要进行添加操作
            u._2 += v
            (newNumber, u._2)
          }
        },
        (u1, u2) => {
          val newNumber = u1._1 + u2._1
          if (newNumber >= 50) {
            // 这个时候不需要进行sid的传输
            (newNumber, ArrayBuffer.empty[String])
          } else {
            // 需要进行添加操作
            u1._2 ++= u2._2
            (newNumber, u1._2)
          }
        }
      )
      .map {
        case ((day, hour), (count, buffer)) => {
          val needMinCount = broadcastOfDayAndHour2Count.value.getOrElse((day, hour), (3, null))._1
          if (count >= needMinCount) {
            // 不需要进行数据额外抽取的操作
            // 此时buffer为空
            ((day, hour), (0, Array.empty[String]))
          } else {
            // 需要进行数据抽样操作，需要补充的的数据数是: needMinCount - count
            ((day, hour), (needMinCount - count, buffer.distinct.toArray))
          }
        }
      }
      .collect()
      .toMap
    // 3. 合并上述两个集合得到最终需要进行补充抽样的时间段
    // needValues = sampleDataByAPIRDD中没有被抽取到数据的时间区间 + 被抽取但是数量不够的时间区间
    val needValues = preDayAndHour2SampleData
      .filterNot {
        case (key, (count, _)) => {
          preDayAndHour2sampledData.contains(key)
        }
      }
      .toBuffer
    needValues ++= preDayAndHour2sampledData.filter(_._2._1 > 0)
    // 对于不会再使用的广播变量，记得随时进行删除操作
    broadcastOfDayAndHour2Count.unpersist(true)

    // 如果需要进行补充数据抽样，进行数据抽样操作
    val dayAndHour2SessionIDRDD: RDD[((String, Int), String)] = {
      if (needValues.size > 0) {
        // 进行数据重新补充抽样
        // 1. 将needValues集合转换为Map并广播出去
        val broadcastOfNeedValues = sc.broadcast(needValues.toMap)

        // 2. 数据过滤转换操作
        val filteredBaseSampleDataRDD = baseSampleDataRDD
          .filter {
            case ((day, hour), sessionID) => {
              // 只要(day,hour)时间段在需要补充抽样的数据集合中
              broadcastOfNeedValues.value.contains((day, hour))
            }
          }
          .repartition(baseSampleDataRDD.partitions.length)

        // 3. 数据抽样操作==> 按照时间段进行数据分组后，再进行数据抽样
        /**
          * Note: 当使用groupByKey的时候，可能存在内存溢出，所以说需要掌握解决该问题的方法<br/>
          * 一般的解决方案：两阶段聚合<br/>
          * 1. 给数据添加一个随机前缀
          * 2. 先进行局部聚合(由于进行了随机前缀的添加，不可能出现内存溢出)
          * 3. 将随机前缀删除，在局部聚合的基础上进行一个全局数据聚合
          **/
        val extraSampleDataRDD = filteredBaseSampleDataRDD
          .mapPartitions(iter => {
            val random = Random
            // 原来的一个key，可能随机变成了100个key
            iter.map {
              case ((day, hour), sessionID) => {
                ((random.nextInt(100), day, hour), sessionID)
              }
            }
          })
          .groupByKey()
          .flatMap {
            case ((_, day, hour), sessionIDIter) => {
              // 从sessionIDIter迭代器中随机抽取需要的数据
              val key = (day, hour)
              // 从广播变量中获取当前key需要额外抽取的sessionID的数量以及已经存在的sessionID
              val (count, exclusiveIDs) = broadcastOfNeedValues.value.get(key).get
              // 从sessionIDIter迭代器中抽取count数量的sessionID
              // 需要将之前该时间段被抽样的sessionID从迭代器中过滤出去，这样可以保证数据抽样不会重复
              val sampleIter = fetchSampleItem(sessionIDIter, count, exclusiveIDs)
              // 转换数据并输出
              sampleIter.map(sessionID => (key, sessionID))
            }
          }
          .groupByKey()
          .flatMap {
            case ((day, hour), sessionIDIter) => {
              // 从sessionIDIter迭代器中随机抽取需要的数据
              val key = (day, hour)
              // 从广播变量中获取当前key需要额外抽取的sessionID的数量以及已经存在的sessionID
              val (count, exclusiveIDs) = broadcastOfNeedValues.value.get(key).get
              // 从sessionIDIter迭代器中抽取count数量的sessionID
              // 需要将之前该时间段被抽样的sessionID从迭代器中过滤出去，这样可以保证数据抽样不会重复
              val sampleIter = fetchSampleItem(sessionIDIter, count, exclusiveIDs)
              // 转换数据并输出
              sampleIter.map(sessionID => (key, sessionID))
            }
          }

        /* // 源代码
        val extraSampleDataRDD = filteredBaseSampleDataRDD
          .groupByKey()
          .flatMap {
            case ((day, hour), sessionIDIter) => {
              // 从sessionIDIter迭代器中随机抽取需要的数据
              val key = (day, hour)
              // 从广播变量中获取当前key需要额外抽取的sessionID的数量以及已经存在的sessionID
              val (count, exclusiveIDs) = broadcastOfNeedValues.value.get(key).get
              // 从sessionIDIter迭代器中抽取count数量的sessionID
              // 需要将之前该时间段被抽样的sessionID从迭代器中过滤出去，这样可以保证数据抽样不会重复
              val sampleIter = fetchSampleItem(sessionIDIter, count, exclusiveIDs)
              // 转换数据并输出
              sampleIter.map(sessionID => (key, sessionID))
            }
          }
        * */
        broadcastOfNeedValues.unpersist(true)

        // 将两次进行抽取的RDD进行数据合并操作
        sampleDataByAPIRDD
          .union(extraSampleDataRDD)
          .distinct()
      } else {
        sampleDataByAPIRDD.distinct()
      }
    }

    // 将抽样得到的sessionID和session具体信息进行join操作，得到最终抽样的sessionID的详细会话信息
    val finalSampleSessionRDD = sessionID2RecordsRDD
      .join(dayAndHour2SessionIDRDD.map(_.swap))
      .map {
        case (sessionID, (records, (day, hour))) => {
          //这里的records中每一对跨小时的数据进行处理操作
          // TODO: 看业务是否需要进行数据过滤
          val filteredRecords = records.filter(record => {
            val actionTime = record.actionTime
            val timestamp = DateUtils.parseString2Long(actionTime)
            val tmpDay = DateUtils.parseLong2String(timestamp, DateUtils.DATE_FORMAT)
            val tmpHour = DateUtils.getSpecificDateValueOfDateTypeEnum(timestamp, DateTypeEnum.HOUR)

            // 只需要day和hour与输入的数据项匹配的record数据
            day.equals(tmpDay) && hour == tmpHour
          })

          // 返回结果
          ((sessionID, day, hour), filteredRecords)
        }
      }
    finalSampleSessionRDD.cache()

    // 1. 将数据写出到HDFS上, 数据格式为JSON
    val sampleSessionDataSavePath = s"/beifeng/170505/spark-project/session_sample/task_${taskID}"
    FileSystem.get(sc.hadoopConfiguration).delete(new Path(sampleSessionDataSavePath), true)
    finalSampleSessionRDD
      .flatMap {
        case ((sessionID, day, hour), records) => {
          records.map(record => {
            JSONUtil.mergeSampleSessionResultToJSONString(day, hour, record)
          })
        }
      }
      .saveAsTextFile(sampleSessionDataSavePath)

    // 2. 将数据写到RDBMs中，在RDBMs中只保存每个时间段出现session中访问次数前5的session信息
    // 2.1 获取得到访问次数前5的sessionID信息
    val top5SessionIDAndDayOfHour = finalSampleSessionRDD
      .map(tuple => (tuple._1, tuple._2.size))
      .reduceByKey(_ + _)
      .map {
        case ((sessionID, day, hour), count) => {
          ((day, hour), (sessionID, count))
        }
      }
      .groupByKey()
      .flatMap {
        case ((day, hour), iter) => {
          iter
            .toList
            .sortBy(_._2)
            .takeRight(5)
            .map {
              case (sessionID, _) => {
                (sessionID, day, hour)
              }
            }
        }
      }
      .collect()
    val broadcastOfTop5SampleSessionData = sc.broadcast(top5SessionIDAndDayOfHour)
    // 2.2 从最终的数据抽样中获取Top5的数据以及将数据写入到数据库中
    finalSampleSessionRDD
      .filter {
        case (key, records) => {
          broadcastOfTop5SampleSessionData.value.contains(key)
        }
      }
      .foreachPartition(iter => {
        // TODO: 输出到RDBMs中的时候，需要考虑数据提交的方式是什么？到底是批量提交(进行代码修改)还是输出一条就提交一次(自动/默认)
        // 1. 获取JDBC连接
        val jdbcHelper = JDBCHelper.getInstance()
        var conn: Connection = null

        try {
          conn = jdbcHelper.getConnection
          // 2. 创建Statement对象
          val sql = "INSERT INTO tb_task_result_sample_session(`task_id`,`session_id`,`day`,`hour`,`record`) VALUES(?,?,?,?,?)"
          val pstmt = conn.prepareStatement(sql)
          // 3. 对数据进行迭代输出操作
          iter.foreach {
            case ((sessionID, day, hour), records) => {
              val jsonArray: JSONArray = records
                .map(record => record.transform2JSONObject())
                .foldLeft(new JSONArray())((arr, obj) => {
                  arr.add(obj)
                  arr
                })

              // 设置数据输出
              // TODO: 对于taskID是否广播变量关系不大，因为taskID只是一个long类型的数据
              pstmt.setLong(1, taskID)
              pstmt.setString(2, sessionID)
              pstmt.setString(3, day)
              pstmt.setInt(4, hour)
              pstmt.setString(5, jsonArray.toJSONString)

              // 由于一个会话中的数据可能会比较大，所以直接提交，不进行batch批量提交
              pstmt.executeUpdate()
            }
          }
        } finally {
          // 4. 进行连接关闭操作
          jdbcHelper.returnConnection(conn)
        }
      })

    baseSampleDataRDD.unpersist(false)
    sampleDataByAPIRDD.unpersist(false)
    finalSampleSessionRDD.unpersist(false)

    // 七、需求三: 获取点击、下单、支付次数前10的各个品类的各种操作的次数
    /**
      * 点击、下单、支付是三种不同的操作，需求获取每个操作中触发次数最多的前10个品类(最多30个品类)
      * 对数据先按照操作类型进行分组，然后对每组数据进行计数统计，最后对每组数据进行Top10的结果获取
      * 这个需求实质上就是一个分组排序TopK的需求
      * 步骤:
      * a. 从原始RDD中获取计算所需要的数据
      * ==> 点击的falg为0；下单为1；支付为2
      * b. 求各个品类被触发的次数==>wordcount
      * c. 分组TopN程序-->按照flag进行数据分区，然后对每个分区的数据进行数据获取
      * TODO: 作业 --> 考虑分组TopN实现过程中，OOM异常的解决代码 & 考虑一下使用SparkSQL如何使用
      * d. 由于分组TopN后RDD的数据量直接降低到30条数据一下, 所以将分区数更改为1
      * e. 按照品类id合并三类操作被触发的次数（按理来讲，应该按照categoryID进行数据分区，然后对每组数据进行聚合 => RDD上的操作; 但是由于只有一个分区，调用groupByKeyAPI会存在shuffle过程，这里不太建议直接在rdd上使用groupByKey api; 直接使用mapPartitions， 然后对分区中的数据迭代器进行操作《存在一个分组合并结果的动作》）
      */
    val top10CategoryIDAndCountRDD: RDD[(String, (Int, Int, Int))] = sessionID2RecordsRDD
      /* 1. 计算每个会话中每个品类的点击次数(0)、下单次数(1)以及支付次数(2)
       *  品类id  次数  操作类型
       *  c1      10    0
       *  c1      5     1
       *  c2      12    2
       *  c1      20    0
       */
      .flatMap {
        case (sessionID, records) => {
          // 从records迭代器中获取各个操作触发的品类id：
          //-----(String, Int) ==> (品类id, 操作类型的标记信息)-----------
          val iter: Iterable[(String, Int)] = records.flatMap(record => {
            val clickCategoryID = record.clickCategoryId
            val orderCategoryIDs = record.orderCategoryIds
            val payCategoryIDs = record.payCategoryIds

            if (StringUtils.isNotEmpty(clickCategoryID)) {
              Iterator.single((clickCategoryID, 0))
            } else if (StringUtils.isNotEmpty(orderCategoryIDs)) {
              orderCategoryIDs
                .split(Constants.SPLIT_CATEGORY_OR_PRODUCT_ID_SEPARATOR_ESCAOE)
                .filter(_.trim.nonEmpty)
                .map(id => (id, 1))
            } else if (StringUtils.isNotEmpty(payCategoryIDs)) {
              payCategoryIDs
                .split(Constants.SPLIT_CATEGORY_OR_PRODUCT_ID_SEPARATOR_ESCAOE)
                .filter(_.trim.nonEmpty)
                .map(id => (id, 2))
            } else {
              Iterator.empty
            }
          })
          // 返回值 => ((品类ID, 操作类型)，出现1次)
          iter.map(key => (key, 1))
        }
      }
      //((品类ID, 操作类型)，出现1次)做累加操作
      /*2. 计算每个品类的点击次数(0)、下单次数(1)以及支付次数(2)
      * 品类id  次数  操作类型
      * c1      30    0    （c1  20  0）+（c1  10  0）
      * c1      5     1
      * c2      12    2
      */
      .reduceByKey(_ + _)
      //------(操作类型，(品类ID，出现次数))--------
      .map(tuple => (tuple._1._2, (tuple._1._1, tuple._2)))
      .groupByKey() // 按照操作类别进行分组
      .flatMap {
      case (flag, iter) => {
        // 对iter中的数据按照次数count进行排序；然后获取数量最多的前10个数据
        //-----(品类ID，出现次数)-----
        val top10Category: List[(String, Int)] = iter
          .toList
          .sortBy(_._2)
          .takeRight(10)

        // 结果输出返回
        top10Category.map {
          case (categoryID, count) => {
            (categoryID, (flag, count))
//            品类id  操作类型  次数
//            c1      0         30
//            c1      1         5
//            c2      2         12
          }
        }
      }
    }
      .repartition(1)
      .mapPartitions(iter => {
        iter
          .toList
          .groupBy(_._1)
          .map {
            case (categoryID, list) => {
              // 对list中的数据进行合并=>list中最多三条数据， 三条数据的flag都不一样
              val categoryCount = list.foldLeft((0, 0, 0))((a, b) => {
                b._2._1 match {
                  case 0 => (b._2._2, a._2, a._3)
                  case 1 => (a._1, b._2._2, a._3)
                  case 2 => (a._1, a._2, b._2._2)
                }
              })

              // 返回结果
              (categoryID, categoryCount)
            }
          }
          .toIterator
      })
    top10CategoryIDAndCountRDD.cache()

    // 数据输出到JDBC
    top10CategoryIDAndCountRDD.foreachPartition(iter => {
      val jdbcHelper = JDBCHelper.getInstance()
      Try {
        val conn = jdbcHelper.getConnection
        val oldAutoCommit = conn.getAutoCommit
        conn.setAutoCommit(false) // 当开启批量提交的时候，必须关闭自动提交机制 ==> 设置为false
        // 2. 创建Statement对象
        val sql = "INSERT INTO tb_task_top10_category(`task_id`,`category_id`,`click_count`,`order_count`,`pay_count`) VALUES(?,?,?,?,?)"
        val pstmt = conn.prepareStatement(sql)
        var recordCount = 0 // 用于记录批量提交了多少数据量
        // 3. 对数据进行迭代输出操作
        iter.foreach {
          case (categoryID, (clickCount, orderCount, payCount)) => {
            // 设置数据输出
            pstmt.setLong(1, taskID)
            pstmt.setString(2, categoryID)
            pstmt.setInt(3, clickCount)
            pstmt.setInt(4, orderCount)
            pstmt.setInt(5, payCount)

            // 启动批量提交
            pstmt.addBatch()
            recordCount += 1

            // 每500条触发一次数据提交操作
            if (recordCount % 500 == 0) {
              pstmt.executeBatch()
              conn.commit()
            }
          }
        }

        // 4. 进行连接提交操作
        pstmt.executeBatch()
        conn.commit()

        // 5. 返回结果
        (oldAutoCommit, conn)
      } match {
        case Success((oldAutoCommit, conn)) => {
          // 执行成功
          Try(conn.setAutoCommit(oldAutoCommit))
          Try(jdbcHelper.returnConnection(conn))
        }
        case Failure(execption) => {
          // 执行失败
          jdbcHelper.returnConnection(null)
          throw execption
        }
      }
    })

    // 八、需求四：获取Top10品类中各个品类被触发的Session中，Session访问数量最多的前10
    /**
      * 触发：点击、下单、支付三种操作中的任意一种
      * 如果在一个会话中，某种操作被触发的多次，那么就计算多次，不涉及数据去重
      * 最终结果：sessionID和各种操作的触发次数报道到JDBC中；同时将具体的session数据保存HDFS
      * 步骤：
      * a. 计算获取Top10的session信息(包含：ID和触发次数)
      * b. 结果保存
      * c. 具体session信息获取，并保存HDFS
      **/
    // 1. 将Top10的categoryID广播出去
    val top10CategoryIDList = top10CategoryIDAndCountRDD.collect().map(t => t._1)
    val broadcastOfTop10CategoryID = sc.broadcast(top10CategoryIDList)
    // 2. 获取Top10的Session信息
    val preCategoryTop10SessionID2CountRDD: RDD[((String, Int), List[(String, Int)])] = sessionID2RecordsRDD
      .mapPartitions(iter => {
        val bv = broadcastOfTop10CategoryID.value
        iter.flatMap {
          case (sessionID, records) => {
            // 求得有效的品类id和类型
            val categoryIDAndFlagIter: Iterable[(String, Int)] = records
              .flatMap(record => {
                val clickCategoryID = record.clickCategoryId
                val orderCategoryIDs = record.orderCategoryIds
                val payCategoryIDs = record.payCategoryIds

                if (StringUtils.isNotEmpty(clickCategoryID)) {
                  Iterator.single((clickCategoryID, 0))
                } else if (StringUtils.isNotEmpty(orderCategoryIDs)) {
                  orderCategoryIDs
                    .split(Constants.SPLIT_CATEGORY_OR_PRODUCT_ID_SEPARATOR_ESCAOE)
                    .filter(_.trim.nonEmpty)
                    .map(id => (id, 1))
                } else if (StringUtils.isNotEmpty(payCategoryIDs)) {
                  payCategoryIDs
                    .split(Constants.SPLIT_CATEGORY_OR_PRODUCT_ID_SEPARATOR_ESCAOE)
                    .filter(_.trim.nonEmpty)
                    .map(id => (id, 2))
                } else {
                  Iterator.empty
                }
              })
              .filter {
                case (categoryID, _) => bv.contains(categoryID)
              }
            // 求被触发的次数(在当前会话中)
            val categoryIDAndFalg2CountIter: Iterator[((String, Int), Int)] = categoryIDAndFlagIter
              .groupBy(v => v)
              .map(tuple => (tuple._1, tuple._2.size))
              .toIterator
            // 返回结果
            categoryIDAndFalg2CountIter.map {
              case ((categoryID, flag), count) => {
                ((categoryID, flag), (sessionID, count))
              }
            }
          }
        }
      })
      .groupByKey()
      .map {
        case ((categoryID, flag), iter) => {
          val top10SessionID2CountIter = iter
            .toList
            .sortBy(_._2)
            .takeRight(10)

          ((categoryID, flag), top10SessionID2CountIter)
        }
      }
    preCategoryTop10SessionID2CountRDD.cache()

    // 3. 将数据输出到JDBC中（结果数据按照categoryID进行聚合，数据输出到MySQL）
    preCategoryTop10SessionID2CountRDD
      .map(tuple => (tuple._1._1, (tuple._1._2, tuple._2)))
      .groupByKey()
      .map {
        case (categoryID, iter) => {
          val categorySessions: (String, String, String) = iter.foldLeft(("", "", ""))((a, b) => {
            val sessionIDStr = b._2
              .foldLeft(new JSONArray())((aa, bb) => {
                val obj = new JSONObject()
                obj.put("sessionid", bb._1)
                obj.put("count", bb._2)
                aa.add(obj)
                aa
              })
              .toJSONString

            b._1 match {
              case 0 => (sessionIDStr, a._2, a._3)
              case 1 => (a._1, sessionIDStr, a._3)
              case 2 => (a._1, a._2, sessionIDStr)
            }
          })

          // (categoryID, (点击操作会话，下单操作会话，支付操作会话))
          (categoryID, categorySessions)
        }
      }
      .foreachPartition(iter => {
        // TODO: 作业将数据输出到表tb_task_top10_category_session
        iter.take(2).foreach(println)
      })

    // 4. 获取具体会话信息(根据会话id来获取)
    val preCategoryTop10SessionDataSavePath = s"/beifeng/spark-project/top10_category_session/task_${taskID}"
    FileSystem.get(sc.hadoopConfiguration).delete(new Path(preCategoryTop10SessionDataSavePath), true)
    val preCategoryTop10SessionId = preCategoryTop10SessionID2CountRDD
      .flatMap(t => t._2.map(_._1))
      .collect()
      .distinct
    val broadCastOfTop10SessionId = sc.broadcast(preCategoryTop10SessionId)
    sessionID2RecordsRDD
      .filter(t => broadCastOfTop10SessionId.value.contains(t._1))
      .flatMap {
        case (sessionID, records) => {
          records.map(record => record.transform2JSONObject().toJSONString)
        }
      }
      .saveAsTextFile(preCategoryTop10SessionDataSavePath)

    top10CategoryIDAndCountRDD.unpersist()
    preCategoryTop10SessionID2CountRDD.unpersist()
    broadCastOfTop10SessionId.unpersist(true)
    broadcastOfTop10CategoryID.unpersist(true)

    // 八、操作完成后，进行必要的清理操作
    sessionID2RecordsRDD.unpersist()
  }

  /**
    * 保存需求一的结果数据
    *
    * @param sc
    * @param taskID
    * @param totalSessionCount
    * @param totalSessionLength
    * @param invalidSessionCount
    * @param preSessionLengthLevelSessionCount
    * @param preDayAndHourOfSessionCount
    * @param preDayAndHourOfSessionLength
    * @param preDayAndHourOfInvalidSessionCount
    */
  def saveSessionAggrResult(sc: SparkContext, taskID: Long,
                            totalSessionCount: Long,
                            totalSessionLength: Double,
                            invalidSessionCount: Long,
                            preSessionLengthLevelSessionCount: Array[(String, Int)],
                            preDayAndHourOfSessionCount: Array[((String, Int), Int)],
                            preDayAndHourOfSessionLength: Array[((String, Int), Long)],
                            preDayAndHourOfInvalidSessionCount: Array[((String, Int), Int)]
                           ): Unit = {
    // 1. 将数据转换为字符串
    val json: String = JSONUtil.mergeSessionAggrResultToJSONString(totalSessionCount, totalSessionLength, invalidSessionCount, preSessionLengthLevelSessionCount, preDayAndHourOfSessionCount, preDayAndHourOfSessionLength, preDayAndHourOfInvalidSessionCount)

    // taskID, json_str
    // TODO: 自己进行具体的存储
    println(json)
  }

  /**
    * 基于给定的过滤参数从Hive中读取相关表中的相关数据
    * 常见的过滤参数：date、sex、age、processional、address、最近搜索的关键词、订单的相关信息(最高订单金额、最高订单商品单价)......
    *
    * @param sqlContext
    * @param taskParam
    * @return
    */
  def getActionRDDByFilter(sqlContext: SQLContext, taskParam: JSONObject): RDD[UserVisitSessionRecord] = {
    // 过滤参数的获取
    val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)
    val sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
    // 职业是一个多选的过滤参数，参数之间使用逗号分隔， eg: 程序员,教师,工人 ==> ui.professional IN ('程序员','教师','工人')
    val professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
    val professionalWhereStr = professionals
      .map(v => {
        val str = v.split(",").map(t => s"'${t}'").mkString("(", ",", ")")
        s" AND ui.professional IN  ${str}"
      })
      .getOrElse("")
    // 判断数据是否需要join操作
    val needJoinUserInfoTable = if (sex.isDefined || professionals.isDefined) Some(true) else None

    // hql的构建===> hql可以考虑一下改成先数据过滤，然后再join
    val hql =
      s"""
         | SELECT
         |   uva.*
         | FROM
         |   user_visit_action uva
         |     ${needJoinUserInfoTable.map(v => " JOIN user_info ui ON uva.user_id = ui.user_id ").getOrElse("")}
         | WHERE 1 = 1
         |   ${startDate.map(v => s" AND uva.date >= '${v}' ").getOrElse("")}
         |   ${endDate.map(v => s" AND uva.date <= '${v}' ").getOrElse("")}
         |   ${sex.map(v => s" AND ui.sex = '${v}' ").getOrElse("")}
         |   ${professionalWhereStr}
      """.stripMargin
    println(s"========\n${hql}\n========")

    // hql的执行得到DataFrame
    val df = sqlContext.sql(hql)

    // DataFrame转换为RDD
    val columnNames = Array("date", "user_id", "session_id", "page_id", "action_time", "search_keyword", "click_category_id", "click_product_id", "order_category_ids", "order_product_ids", "pay_category_ids", "pay_product_ids", "city_id")
    df.map(row => {
      val date: String = row.getAs[String](columnNames(0))
      val userId: Long = row.getAs[Long](columnNames(1))
      val sessionId: String = row.getAs[String](columnNames(2))
      val pageId: Long = row.getAs[Long](columnNames(3))
      val actionTime: String = row.getAs[String](columnNames(4))
      val searchKeyword: String = row.getAs[String](columnNames(5))
      val clickCategoryId: String = row.getAs[String](columnNames(6))
      val clickProductId: String = row.getAs[String](columnNames(7))
      val orderCategoryIds: String = row.getAs[String](columnNames(8))
      val orderProductIds: String = row.getAs[String](columnNames(9))
      val payCategoryIds: String = row.getAs[String](columnNames(10))
      val payProductIds: String = row.getAs[String](columnNames(11))
      val cityId: Int = row.getAs[Int](columnNames(12))

      new UserVisitSessionRecord(date, userId, sessionId, pageId, actionTime, searchKeyword, clickCategoryId, clickProductId, orderCategoryIds, orderProductIds, payCategoryIds, payProductIds, cityId)
    })
  }

  /**
    * 根据给定的需要抽样的数据量，从给定的迭代器中抽取数据
    *
    * @param iter           原始的数据迭代器
    * @param count          需要抽样的数据
    * @param exclusiveItems 给定需要排除的数据集合
    * @param random         数据随机器
    * @return
    */
  def fetchSampleItem[T](iter: Iterable[T], count: Int, exclusiveItems: Array[T] = Array.empty, random: Random = Random): Iterable[T] = {
    if (count == 0) {
      // 当需要获取的数据为0个的时候，直接返回一个空的迭代器对象
      Iterable.empty[T]
    } else {
      // 1. 数据过滤
      val filteredIter = iter.filterNot(t => exclusiveItems.contains(t))
      // 2. 获取迭代器中的数据的数量
      val iterSize = filteredIter.size
      // 2. 进行数据判断
      if (iterSize <= count || iterSize == 0) {
        // 直接返回原本的迭代器
        filteredIter
      } else {
        // 从迭代器中进行随机抽象
        // 2.1 随机产生最终数据所在的index下标
        import scala.collection.mutable
        val indexSet = mutable.Set[Int]()
        while (indexSet.size < count) {
          // 随机一个下标保存到集合中
          val index = random.nextInt(iterSize)
          indexSet += index
        }

        // 2.2 从迭代器中获取对应下标的数据
        filteredIter
          .zipWithIndex
          .filter {
            case (item, index) => {
              indexSet.contains(index)
            }
          }
          .map(_._1)
      }
    }
  }
}
