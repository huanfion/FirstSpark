import java.util.{Date, UUID}

import com.huanfion.commons
import com.huanfion.commons.conf.ConfigurationManager
import com.huanfion.commons.constant.Constants
import com.huanfion.commons.model.{SessionAggrStat, UserInfo, UserVisitAction}
import com.huanfion.commons.utils._
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable

object SessionStatisticAgg {



  def main(args: Array[String]): Unit = {
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    //拿到的配置字符串转化为json
    val taskParam = JSONObject.fromObject(jsonStr)
    //每一个spark任务唯一标识符，存入数据库的时候作为主键，区别不同的spark产生的数据
    val taskUUID = UUID.randomUUID().toString

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("session")
    //"hive.metastore.uris", "thrift://master:9083"
    val spark = SparkSession.builder().config(sparkConf).config("hive.metastore.uri", "thrift://master:9083").enableHiveSupport().getOrCreate()

    val actionRDD = getActionRDD(spark, taskParam)
    //    actionRDD.foreach(println(_))
    //将数据转化为session粒度 <sessionid,(sessionid,searchKeywords,clickCategoryIds,age,professional,city,sex)>
    val sessionId2ActionRDD = actionRDD.map {
      item => (item.session_id, item)
    }
    val sessionId2GroupRDD = sessionId2ActionRDD.groupByKey()
    sessionId2GroupRDD.cache()
    //    sessionId2GroupRDD.foreach(println(_))
    //获取聚合数据里面的聚合信息
    val session2FullAggrInfoRDD = getFullInfoData(spark, sessionId2GroupRDD)
    //    userId2FullInfoRDD.foreach(println(_))
    //声明自定义累加器
    val sessionAggrStatAccumulator=new SessionStatAccumulator()
    //注册累加器
    spark.sparkContext.register(sessionAggrStatAccumulator,"sessionAggrStatAccumulator")
    //过滤用户数据
    val sessionId2FilterRDD=getFilteredData(taskParam, session2FullAggrInfoRDD,sessionAggrStatAccumulator)
    sessionId2FilterRDD.foreach(println(_))
    for ((k,v) <- sessionAggrStatAccumulator.value) {
      println("k="+k+",value="+v)
    }

    //// 业务功能一：统计各个范围的session占比，并写入MySQL
    calculateAndPersistAggrStat(spark,taskUUID,sessionAggrStatAccumulator.value)
  }

  def calculateAndPersistAggrStat(spark: SparkSession, taskUUID: String, value: mutable.HashMap[String, Int]): Unit = {
    val session_count=value.getOrElse(Constants.SESSION_COUNT,1).toDouble
    val visit_Length_1s_3s=value.getOrElse(Constants.TIME_PERIOD_1s_3s,0)
    val visit_Length_4s_6s=value.getOrElse(Constants.TIME_PERIOD_4s_6s,0)
    val visit_length_7s_9s = value.getOrElse(Constants.TIME_PERIOD_7s_9s, 0)
    val visit_length_10s_30s = value.getOrElse(Constants.TIME_PERIOD_10s_30s, 0)
    val visit_length_30s_60s = value.getOrElse(Constants.TIME_PERIOD_30s_60s, 0)
    val visit_length_1m_3m = value.getOrElse(Constants.TIME_PERIOD_1m_3m, 0)
    val visit_length_3m_10m = value.getOrElse(Constants.TIME_PERIOD_3m_10m, 0)
    val visit_length_10m_30m = value.getOrElse(Constants.TIME_PERIOD_10m_30m, 0)
    val visit_length_30m = value.getOrElse(Constants.TIME_PERIOD_30m, 0)

    val step_length_1_3 = value.getOrElse(Constants.STEP_PERIOD_1_3, 0)
    val step_length_4_6 = value.getOrElse(Constants.STEP_PERIOD_4_6, 0)
    val step_length_7_9 = value.getOrElse(Constants.STEP_PERIOD_7_9, 0)
    val step_length_10_30 = value.getOrElse(Constants.STEP_PERIOD_10_30, 0)
    val step_length_30_60 = value.getOrElse(Constants.STEP_PERIOD_30_60, 0)
    val step_length_60 = value.getOrElse(Constants.STEP_PERIOD_60, 0)

    //统计各个访问时长和访问步长在多个范围内session的占比
    val visit_Length_1s_3s_ratio=NumberUtils.formatDouble(visit_Length_1s_3s/session_count,2)
    val visit_Length_4s_6s_ratio=NumberUtils.formatDouble(visit_Length_4s_6s/session_count,2)
    val visit_length_7s_9s_ratio= NumberUtils.formatDouble(visit_length_7s_9s/session_count,2)
    val visit_length_10s_30s_ratio = NumberUtils.formatDouble(visit_length_10s_30s/session_count,2)
    val visit_length_30s_60s_ratio = NumberUtils.formatDouble(visit_Length_1s_3s/session_count,2)
    val visit_length_1m_3m_ratio = NumberUtils.formatDouble(visit_length_1m_3m/session_count,2)
    val visit_length_3m_10m_ratio = NumberUtils.formatDouble(visit_length_3m_10m/session_count,2)
    val visit_length_10m_30m_ratio = NumberUtils.formatDouble(visit_length_10m_30m/session_count,2)
    val visit_length_30m_ratio = NumberUtils.formatDouble(visit_length_30m/session_count,2)

    val step_length_1_3_ratio = NumberUtils.formatDouble(step_length_1_3 / session_count, 2)
    val step_length_4_6_ratio = NumberUtils.formatDouble(step_length_4_6 / session_count, 2)
    val step_length_7_9_ratio = NumberUtils.formatDouble(step_length_7_9 / session_count, 2)
    val step_length_10_30_ratio = NumberUtils.formatDouble(step_length_10_30 / session_count, 2)
    val step_length_30_60_ratio = NumberUtils.formatDouble(step_length_30_60 / session_count, 2)
    val step_length_60_ratio = NumberUtils.formatDouble(step_length_60 / session_count, 2)

    val sessionAggrStat=new SessionAggrStat(taskUUID,session_count.toInt,visit_Length_1s_3s_ratio,visit_Length_4s_6s_ratio,visit_length_7s_9s_ratio,visit_length_10m_30m_ratio
    ,visit_length_30s_60s_ratio,visit_length_1m_3m_ratio,visit_length_3m_10m_ratio,visit_length_10m_30m_ratio,visit_length_30m_ratio,
      step_length_1_3_ratio,step_length_4_6_ratio,step_length_7_9_ratio,step_length_10_30_ratio,step_length_30_60_ratio,step_length_60_ratio)
    //统计结果写入mysql
    /**
      * mysql 创建表session_aggr_stat
      * -- ----------------------------
      * --  Table structure for `session_aggr_stat`
      * -- ----------------------------
      * DROP TABLE IF EXISTS `session_aggr_stat`;
      * CREATE TABLE `session_aggr_stat` (
      * `taskid` varchar(255) DEFAULT NULL,
      * `session_count` int(11) DEFAULT NULL,
      * `visit_length_1s_3s_ratio` double DEFAULT NULL,
      * `visit_length_4s_6s_ratio` double DEFAULT NULL,
      * `visit_length_7s_9s_ratio` double DEFAULT NULL,
      * `visit_length_10s_30s_ratio` double DEFAULT NULL,
      * `visit_length_30s_60s_ratio` double DEFAULT NULL,
      * `visit_length_1m_3m_ratio` double DEFAULT NULL,
      * `visit_length_3m_10m_ratio` double DEFAULT NULL,
      * `visit_length_10m_30m_ratio` double DEFAULT NULL,
      * `visit_length_30m_ratio` double DEFAULT NULL,
      * `step_length_1_3_ratio` double DEFAULT NULL,
      * `step_length_4_6_ratio` double DEFAULT NULL,
      * `step_length_7_9_ratio` double DEFAULT NULL,
      * `step_length_10_30_ratio` double DEFAULT NULL,
      * `step_length_30_60_ratio` double DEFAULT NULL,
      * `step_length_60_ratio` double DEFAULT NULL,
      * KEY `idx_task_id` (`taskid`)
      * ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
      */
    import spark.implicits._
    val sessionAggrStatRDD=spark.sparkContext.makeRDD(Array(sessionAggrStat))
    sessionAggrStatRDD.toDF().write.format("jdbc")
      .option("url",ConfigurationManager.config.getString(Constants.JDBC_URL))//"jdbc:mysql://master:3306/test"
      .option("dbtable","session_aggr_stat")
      .option("user",ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password",ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append).save()

  }
  def getFilteredData(taskParam: JSONObject, session2FullAggrInfoRDD: RDD[(String, String)],sessionAggrStatAccumulator:SessionStatAccumulator) = {
    val startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE)
    val endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE)
    val professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
    val cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)
    val sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
    val keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)
    val categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS)

    var filterInfo =
      (if (startAge != null) Constants.PARAM_START_AGE + "=" + startAge + "|" else "") +
        (if (endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|" else "") +
        (if (professionals != null) Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" else "") +
        (if (cities != null) Constants.PARAM_CITIES + "=" + cities + "|" else "") +
        (if (sex != null) Constants.PARAM_SEX + "=" + sex + "|" else "") +
        (if (keywords != null) Constants.PARAM_KEYWORDS + "=" + keywords + "|" else "") +
        (if (categoryIds != null) Constants.PARAM_CATEGORY_IDS + "=" + categoryIds else "")
    if (filterInfo.endsWith("\\|")) {
      filterInfo = filterInfo.substring(0, filterInfo.length - 1)
    }
    //根据filterInfo信息筛选rdd
    val sessionId2FilterRDD=session2FullAggrInfoRDD.filter { case (sessionid, fullInfo) =>
      var success = true
      if (!ValidUtils.between(fullInfo, Constants.FIELD_AGE, filterInfo, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
        success = false
      }
      else if (!ValidUtils.in(fullInfo, Constants.FIELD_PROFESSIONAL, filterInfo, Constants.PARAM_PROFESSIONALS)) {
        success = false
      }
      else if (!ValidUtils.in(fullInfo, Constants.FIELD_CITY, filterInfo, Constants.PARAM_CITIES)) {
        success = false
      }
      else if (!ValidUtils.equal(fullInfo, Constants.FIELD_SEX, filterInfo, Constants.PARAM_SEX)) {
        success = false
      }
      else if (!ValidUtils.in(fullInfo, Constants.FIELD_CATEGORY_ID, filterInfo, Constants.PARAM_CATEGORY_IDS)) {
        success = false
      }
      else if (!ValidUtils.in(fullInfo, Constants.FIELD_SEARCH_KEYWORDS, filterInfo, Constants.PARAM_KEYWORDS)) {
        success = false
      }
      if(success){
        sessionAggrStatAccumulator.add(Constants.SESSION_COUNT)
        val visitLength=StringUtils.getFieldFromConcatString(fullInfo,"\\|",Constants.FIELD_VISIT_LENGTH).toLong
        val setpLength=StringUtils.getFieldFromConcatString(fullInfo,"\\|",Constants.FIELD_STEP_LENGTH).toLong

        // 计算访问时长范围
        def calculateVisitLength(visitLength: Long) {
          if (visitLength >= 1 && visitLength <= 3) {
            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
          } else if (visitLength >= 4 && visitLength <= 6) {
            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
          } else if (visitLength >= 7 && visitLength <= 9) {
            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
          } else if (visitLength >= 10 && visitLength <= 30) {
            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
          } else if (visitLength > 30 && visitLength <= 60) {
            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
          } else if (visitLength > 60 && visitLength <= 180) {
            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
          } else if (visitLength > 180 && visitLength <= 600) {
            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
          } else if (visitLength > 600 && visitLength <= 1800) {
            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
          } else if (visitLength > 1800) {
            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
          }
        }

        // 计算访问步长范围
        def calculateStepLength(stepLength: Long) {
          if (stepLength >= 1 && stepLength <= 3) {
            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
          } else if (stepLength >= 4 && stepLength <= 6) {
            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
          } else if (stepLength >= 7 && stepLength <= 9) {
            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
          } else if (stepLength >= 10 && stepLength <= 30) {
            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
          } else if (stepLength > 30 && stepLength <= 60) {
            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
          } else if (stepLength > 60) {
            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
          }
        }
        calculateVisitLength(visitLength)
        calculateStepLength(setpLength)
      }
      success
    }
    sessionId2FilterRDD
  }

  def getFullInfoData(spark: SparkSession, sessionId2GroupRDD: RDD[(String, Iterable[UserVisitAction])]) = {
    val userId2AggrInfoRDD = sessionId2GroupRDD.map { case (sid, iterableAction) =>
      val searchKeywordBuffer = new StringBuilder("")
      val clickCategoryIdBuffer = new StringBuilder("")

      var startTime: Date = null;
      var endTime: Date = null;
      var userid = -1L
      // session 的访问步长
      var stepLength = 0
      iterableAction.foreach { userVisitAction =>
        if (userid == -1L) {
          userid = userVisitAction.userid
        }
        val searchKeyword = userVisitAction.search_keyword
        val clickCategoryId = userVisitAction.click_category_id

        if (StringUtils.isNotEmpty(searchKeyword)) {
          if (!searchKeywordBuffer.contains(searchKeyword)) {
            searchKeywordBuffer.append(searchKeyword + ",")
          }
        }
        if (clickCategoryId != null || clickCategoryId != -1L) {
          if (!clickCategoryIdBuffer.toString().contains(clickCategoryId.toString)) {
            clickCategoryIdBuffer.append(clickCategoryId + ",")
          }
        }
        //计算actiontime ，action 开始时间和结束时间
        val actionTime = DateUtils.parseTime(userVisitAction.action_time)

        if (startTime == null) {
          startTime = actionTime
        }
        if (endTime == null) {
          endTime = actionTime
        }
        if (actionTime.before(startTime)) {
          startTime = actionTime
        }
        if (actionTime.after(endTime)) {
          endTime = actionTime
        }
        stepLength += 1
      }
      val searchKeywords = StringUtils.trimComma(searchKeywordBuffer.toString())
      val clickCategoryIds = StringUtils.trimComma(clickCategoryIdBuffer.toString())

      //计算session访问时长(秒)
      val visitLength = (endTime.getTime - startTime.getTime) / 1000

      val partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sid + "|" +
        Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|" +
        Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|" +
        Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
        Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
        Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime)
      (userid, partAggrInfo)
    }
    //联合userinfo表数据
    import spark.implicits._
    val userid2InfoRDD = spark.sql("select * from user_info").as[UserInfo].rdd.map { item => (item.user_id, item) }
    //根据userid 把UserInfo 和userId2AggrInfoRDD join操作
    val userid2FullInfoRDD = userId2AggrInfoRDD.join(userid2InfoRDD)
    //对jion起来的数据进行拼接，并且返回<sessionid,fullAggrInfo>
    val session2FullAggrInfoRDD = userid2FullInfoRDD.map { case (userid, (partAggrInfo, userInfo)) =>
      val sessionid = StringUtils.getFieldFromConcatString(partAggrInfo, "\\|", Constants.FIELD_SESSION_ID)
      val fullAggrInfo = partAggrInfo + "|" +
        Constants.FIELD_AGE + "=" + userInfo.age + "|" +
        Constants.FIELD_PROFESSIONAL + "=" + userInfo.processional + "|" +
        Constants.FIELD_SEX + "=" + userInfo.sex + "|" +
        Constants.FIELD_CITY + "=" + userInfo.city
      (sessionid, fullAggrInfo)
    }
    session2FullAggrInfoRDD
  }

  def getActionRDD(spark: SparkSession, taskParam: JSONObject) = {
    val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

    val sql = "select * from user_visit_action where date>'" + startDate + "' and  date <='" + endDate + "'"
    import spark.implicits._
    spark.sql(sql).as[UserVisitAction].rdd
  }
}
