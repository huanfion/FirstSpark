package com.huanfion.commons.utils

import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTime
import java.util.Date
/**
  * 日期时间工具类
  * 使用Joda实现，使用Java提供的Date会存在线程安全问题
  */
object DateUtils {
  val TIME_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
  val DATE_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd")
  val DATEKEY_FORMAT = DateTimeFormat.forPattern("yyyyMMdd")
  val DATETIME_FORMAT = DateTimeFormat.forPattern("yyyyMMddHHmm")

  /**
    * 判断一个时间是否在另一个时间之前
    *
    * @param time1
    * @param time2
    * @return
    */
  def before(time1: String, time2: String) = {
    if (TIME_FORMAT.parseDateTime(time1).isBefore(TIME_FORMAT.parseDateTime(time2))) {
      true
    }
    else {
      false
    }
  }

  /**
    * 判断一个时间是否在另一个时间之后
    * @param time1
    * @param time2
    * @return
    */
  def after(time1: String, time2: String) = {
    if (TIME_FORMAT.parseDateTime(time1).isAfter(TIME_FORMAT.parseDateTime(time2))) {
      true
    }
    else {
      false
    }
  }

  /**
    * 计算两个时间的差值 单位：秒
    * @param time1
    * @param time2
    * @return
    */
  def minus(time1:String,time2:String)={
    TIME_FORMAT.parseDateTime(time1).getMillis-TIME_FORMAT.parseDateTime(time2).getMillis
  }

  /**
    * 获取年月日和小时
    * @param datetime 时间（yyyy-MM-dd HH:mm:ss）
    * @return
    */
  def getDateHour(datetime:String)={
    val date=datetime.split(" ")(0)
    val hourMinuteSecond=datetime.split(" ")(1)
    val hour=hourMinuteSecond.split(":")(0)
    date+"_"+hour
  }

  /**
    * 获取当天日期 yyyy-MM-dd
    * @return
    */
  def getTodayDate()={
    DateTime.now.toString(DATE_FORMAT)
  }

  /**
    * 格式化时间（yyyy-MM-dd HH:mm:ss）
    * @param date Date对象
    * @return 格式化后的时间
    */
  def formatTime(date:Date):String = {
    new DateTime(date).toString(TIME_FORMAT)
  }
  /**
    * 解析时间字符串
    * @param time 时间字符串
    * @return Date
    */
  def parseTime(time:String):Date = {
    TIME_FORMAT.parseDateTime(time).toDate
  }

}
