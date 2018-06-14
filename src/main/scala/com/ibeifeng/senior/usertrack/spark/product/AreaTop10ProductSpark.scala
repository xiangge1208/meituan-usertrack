package com.ibeifeng.senior.usertrack.spark.product

import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import com.ibeifeng.senior.usertrack.conf.ConfigurationManager
import com.ibeifeng.senior.usertrack.constant.Constants
import com.ibeifeng.senior.usertrack.dao.factory.DAOFactory
import com.ibeifeng.senior.usertrack.mock.MockDataUtils
import com.ibeifeng.senior.usertrack.spark.util.{SQLContextUtil, SparkConfUtil, SparkContextUtil}
import com.ibeifeng.senior.usertrack.util.ParamUtils
import org.apache.spark.sql.SQLContext


object AreaTop10ProductSpark {
  // lazy：延迟初始化，直到第一次被使用的时候才会进行初始化操作
  lazy val url = ConfigurationManager.getProperty(Constants.JDBC_URL)
  lazy val table = "city_info"
  lazy val properties = {
    val prop = new Properties()
    prop.put("user", ConfigurationManager.getProperty(Constants.JDBC_USER))
    prop.put("password", ConfigurationManager.getProperty(Constants.JDBC_PASSWORD))
    prop
  }

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
    val appName = Constants.SPARK_APP_NAME_PRODUCT + taskID
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
      integratedHive = true, // 因为代码中使用了hive的窗口分析函数，所以必须使用HiveContext作为程序入口
      generateMockData = (sc, sqlContext) => {
        // 本地模式的情况下，进行数据的模拟操作 ==> 模拟出一个表
        if (isLocal) {
          // 模拟数据构建: user_visit_action和user_info的数据
          MockDataUtils.mockData(sc, sqlContext)
          // 模拟数据构建：product_info的数据
          MockDataUtils.loadProductInfoMockData(sc, sqlContext)
        }
      }
    )

    // 自定义UDF
    def isEmpty(str: String) = str == null || str.trim.isEmpty || "null".equalsIgnoreCase(str.trim)

    sqlContext.udf.register("isNotEmpty", (str: String) => !isEmpty(str))
    sqlContext.udf.register("concat_id_name", (id: Int, name: String) => {
      s"${id}:${name}"
    })
    sqlContext.udf.register("fetch_product_type_from_json", (json: String) => {
      val value = JSON
        .parseObject(json)
        .getString("product_type")
      if (value == null || value.isEmpty) {
        "未知商品"
      } else {
        value match {
          case "1" => "第三方商品"
          case "0" => "自营商品"
          case _ => "未知商品"
        }
      }
    })
    // 自定义UDAF
    sqlContext.udf.register("concat_distinct_str", GroupConcatDistinctUDAF)

    // 三、数据过滤（不需要返回RDD以及过滤参数有可能不同）
    this.filterDataByTaskParamAndRegisterTmpTable(sqlContext, taskParam)

    // 四、读取关系型数据库中的city_info城市信息表的数据，并注册成为临时表
    this.fetchCityInfoDataAndRegisterTmpTable(sqlContext)

    // 五、将基础的用户访问数据和城市信息表数据进行join操作，并注册成为临时表
    this.registerTableMergeUserVisitActionAndCityInfoData(sqlContext)

    // 六、统计各个区域、各个商品的点击次数(area、product_id、click_count)
    this.registerAreaProductClickCountTable(sqlContext)

    // 七、分组排序TopN ==> 对各个区域的点击数据进行分组，然后对每组数据获取点击次数最多的前10个商品
    this.registerAreaTop10ProductClickCountTable(sqlContext)

    // 八、合并商品信息，得到最终的结果表
    this.registerMergeTop10AndProductInfoTable(sqlContext)

    // 九、结果输出
    this.saveResult(sqlContext)
  }

  /**
    * 数据结果输出
    *
    * @param sqlContext
    */
  def saveResult(sqlContext: SQLContext): Unit = {
    // 一般数据输输出到关系型数据库中
    /**
      * 数据输出到关系型数据库的方式：
      * 1. 将数据转换为RDD，然后RDD进行数据输出
      * --a. rdd调用foreachPartition定义数据输出代码进行输出操作
      * --b. rdd调用saveXXX方式使用自定义的OutputFormat进行数据输出
      * 2. 使用DataFrame的write.jdbc进行数据输出 ==> 不支持Insert Or Update操作
      */
    // TODO: 自己考虑怎么进行数据输出
    sqlContext
      .read
      .table("tmp_result")
      .show()
  }

  /**
    * 合并商品信息
    *
    * @param sqlContext
    */
  def registerMergeTop10AndProductInfoTable(sqlContext: SQLContext): Unit = {
    // 1. hql的编写
    val hql =
      """
        | SELECT
        |   CASE
        |     WHEN tatpcc.area = '华东' OR tatpcc.area = '华南' THEN 'A'
        |     WHEN tatpcc.area = '华北' OR tatpcc.area = '华中' THEN 'B'
        |     WHEN tatpcc.area = '东北' THEN 'C'
        |     ELSE 'D'
        |   END AS area_level,
        |   tatpcc.click_product_id AS product_id,
        |   tatpcc.area,
        |   tatpcc.click_count,
        |   tatpcc.city_infos,
        |   pi.product_name,
        |   fetch_product_type_from_json(pi.extend_info) AS product_type
        | FROM
        |   tmp_area_top10_product_click_count tatpcc
        |     JOIN product_info pi ON tatpcc.click_product_id = pi.product_id
      """.stripMargin

    // 2. hql执行
    val df = sqlContext.sql(hql)

    // 3. 临时表注册
    df.registerTempTable("tmp_result")
  }

  /**
    * 获取各个区域热门商品Top10
    *
    * @param sqlContext
    */
  def registerAreaTop10ProductClickCountTable(sqlContext: SQLContext): Unit = {
    // 1. 分组hql编写(分组+添加一个排序序号) ==> row_number
    val hql =
      """
        | SELECT
        |   area, click_product_id, click_count, city_infos,
        |   ROW_NUMBER() OVER(PARTITION BY area ORDER BY click_count DESC) AS rnk
        | FROM tmp_area_product_click_count
      """.stripMargin
    // 2. hql执行并注册临时表
    sqlContext.sql(hql).registerTempTable("tmp_area_product_click_count_rnk")
    // 3. topN的hql编写
    val hql2 =
      """
        | SELECT
        |   area, click_product_id, click_count, city_infos
        | FROM tmp_area_product_click_count_rnk
        | WHERE rnk <= 10
      """.stripMargin
    // 4. hql执行并注册临时表
    sqlContext.sql(hql2).registerTempTable("tmp_area_top10_product_click_count")

  }

  /**
    * 统计各个区域、各个商品的点击次数
    *
    * @param sqlContext
    */
  def registerAreaProductClickCountTable(sqlContext: SQLContext): Unit = {
    /** *
      * 原始数据:
      * p1 c1 上海 华东
      * p1 c2 杭州 华东
      * p2 c1 上海 华东
      * p1 c1 上海 华东
      * p1 c2 杭州 华东
      * p1 c5 北京 华北
      * .....
      * 结果：
      * 华东 p1 4 c1:上海:2,c2:杭州:2
      * 华东 p2 1 c1:上海:1
      * 华北 p1 1 c5:北京:1
      * .....
      *
      * 1. 需要将city_id和city_name进行合并 => UDF
      * 2. 需要将同组的所有数据合并执行的城市信息进行聚合，并计算各个节点的次数 => UDAF
      */
    // 1. hql语句
    val hql =
    """
      | SELECT
      |   area, click_product_id,
      |   COUNT(1) AS click_count,
      |   concat_distinct_str(concat_id_name(city_id, city_name)) AS city_infos
      | FROM
      |   tmp_basic_product_city_info
      | GROUP BY area, click_product_id
    """.stripMargin

    // 2. hql语句执行
    val df = sqlContext.sql(hql)

    // 3. 临时表注册
    df.registerTempTable("tmp_area_product_click_count")
  }

  /**
    * 合并用户商品点击信息和城市信息的数据并将其注册成为临时表
    *
    * @param sqlContext
    */
  def registerTableMergeUserVisitActionAndCityInfoData(sqlContext: SQLContext): Unit = {
    // 1. hql
    val hql =
      """
        | SELECT
        |   tbuva.city_id,
        |   tbuva.click_product_id,
        |   uci.city_name,
        |   uci.area,
        |   uci.province_name
        | FROM
        |  tmp_basic_user_visit_action tbuva JOIN tmp_city_info uci ON tbuva.city_id = uci.city_id
      """.stripMargin
    // 2. hql语句执行
    val df = sqlContext.sql(hql)
    // 3. 注册临时表
    df.registerTempTable("tmp_basic_product_city_info")
  }

  /**
    * 获取关系型数据库中的数据，并将其注册成为临时表
    *
    * @param sqlContext
    */
  def fetchCityInfoDataAndRegisterTmpTable(sqlContext: SQLContext): Unit = {
    // JDBC数据库的数据读取方式：通过SparkSQL的read模式进行读取, jdbc的数据读取接口总共有三个，一般使用自定义分区的方式来进行数据读取操作 ==> 这里使用最简单的方式
    sqlContext
      .read
      .jdbc(url, table, properties)
      .registerTempTable("tmp_city_info")
  }

  /**
    * 过滤参数，并注册临时表
    *
    * @param sqlContext
    * @param taskParam
    */
  def filterDataByTaskParamAndRegisterTmpTable(sqlContext: SQLContext, taskParam: JSONObject): Unit = {
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
         |   uva.click_product_id, uva.city_id
         | FROM
         |   user_visit_action uva
         |   ${needJoinUserInfoTable.map(v => " JOIN user_info ui ON uva.user_id = ui.user_id ").getOrElse("")}
         | WHERE isNotEmpty(uva.click_product_id)
         |   ${startDate.map(v => s" AND uva.date >= '${v}' ").getOrElse("")}
         |   ${endDate.map(v => s" AND uva.date <= '${v}' ").getOrElse("")}
         |   ${sex.map(v => s" AND ui.sex = '${v}' ").getOrElse("")}
         |   ${professionalWhereStr}
      """.stripMargin
    println(s"========\n${hql}\n========")

    // hql的执行得到DataFrame
    val df = sqlContext.sql(hql)

    // DataFrame注册成为临时表
    df.registerTempTable("tmp_basic_user_visit_action")
  }
}
