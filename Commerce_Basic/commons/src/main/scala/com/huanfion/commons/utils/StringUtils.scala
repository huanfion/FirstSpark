package com.huanfion.commons.utils


import net.sf.json.JSONObject

import scala.collection.mutable
/**
  * 字符串工具类
  */
object StringUtils {

  /**
    * 判断字符串是否为空
    *
    * @param str 字符串
    * @return 是否为空
    */
  def isEmpty(str: String) = {
    str == null || "".equals(str)
  }
  /**
    * 判断字符串是否为空
    *
    * @param str 字符串
    * @return 是否为空
    */
  def isNotEmpty(str: String) = {
    str != null && !"".equals(str)
  }
  /**
    * 自动补全两位数字
    * @param str
    * @return
    */
  def fulfuill(str: String) = {
    if (str.length == 2) {
      str
    } else {
      "0" + str
    }
  }
  /**
    * 截断字符串两侧的逗号
    * @param str 字符串
    * @return 字符串
    */
  def trimComma(str:String):String = {
    var result = ""
    if(str.startsWith(",")) {
      result = str.substring(1)
    }
    if(str.endsWith(",")) {
      result = str.substring(0, str.length() - 1)
    }
    result
  }
  /**
    * 从拼接的字符串中提取字段
    * @param str 字符串
    * @param delimiter 分隔符
    * @param field 字段
    * @return 字段值
    */
  def getFieldFromConcatString(str:String, delimiter:String, field:String):String = {
    try {
      val fields = str.split(delimiter);
      for(concatField <- fields) {
        // searchKeywords=|clickCategoryIds=1,2,3
        if(concatField.split("=").length == 2) {
          val fieldName = concatField.split("=")(0)
          val fieldValue = concatField.split("=")(1)
          if(fieldName.equals(field)) {
            return fieldValue
          }
        }
      }
    } catch{
      case e:Exception => e.printStackTrace()
    }
    null
  }

  /**
    * 从拼接的字符串中给字段设置值
    * @param str 字符串
    * @param delimiter 分隔符
    * @param field 字段名
    * @param newFieldValue 新的field值
    * @return 字段值
    */
  def setFieldInConcatString(str:String, delimiter:String, field:String, newFieldValue:String):String = {

    val fieldsMap = new mutable.HashMap[String,String]()

    for(fileds <- str.split(delimiter)){
      var arra = fileds.split("=")
      if(arra(0).compareTo(field) == 0)
        fieldsMap += (field -> newFieldValue)
      else
        fieldsMap += (arra(0) -> arra(1))
    }
    fieldsMap.map(item=> item._1 + "=" + item._2).mkString(delimiter)
  }

}
/**
  * 参数工具类
  *
  */
object ParamUtils {

  /**
    * 从JSON对象中提取参数
    * @param jsonObject JSON对象
    * @return 参数
    */
  def getParam(jsonObject:JSONObject, field:String):String = {
    jsonObject.getString(field)
    /*val jsonArray = jsonObject.getJSONArray(field)
    if(jsonArray != null && jsonArray.size() > 0) {
      return jsonArray.getString(0)
    }
    null*/
  }

}

/**
  * 数字格工具类
  *
  */
object NumberUtils {

  /**
    * 格式化小数
    * @param scale 四舍五入的位数
    * @return 格式化小数
    */
  def formatDouble(num:Double, scale:Int):Double = {
    val bd = BigDecimal(num)
    bd.setScale(scale, BigDecimal.RoundingMode.HALF_UP).doubleValue()
  }

}

/**
  * 校验工具类
  *
  */
object ValidUtils {

  /**
    * 校验数据中的指定字段，是否在指定范围内
    * @param data 数据
    * @param dataField 数据字段
    * @param parameter 参数
    * @param startParamField 起始参数字段
    * @param endParamField 结束参数字段
    * @return 校验结果
    */
  def between(data:String, dataField:String, parameter:String, startParamField:String, endParamField:String):Boolean = {

    val startParamFieldStr = StringUtils.getFieldFromConcatString(parameter, "\\|", startParamField)
    val endParamFieldStr = StringUtils.getFieldFromConcatString(parameter, "\\|", endParamField)
    if(startParamFieldStr == null || endParamFieldStr == null) {
      return true
    }

    val startParamFieldValue = startParamFieldStr.toInt
    val endParamFieldValue = endParamFieldStr.toInt

    val dataFieldStr = StringUtils.getFieldFromConcatString(data, "\\|", dataField)
    if(dataFieldStr != null) {
      val dataFieldValue = dataFieldStr.toInt
      if(dataFieldValue >= startParamFieldValue && dataFieldValue <= endParamFieldValue) {
        return true
      } else {
        return false
      }
    }
    false
  }

  /**
    * 校验数据中的指定字段，是否有值与参数字段的值相同
    * @param data 数据
    * @param dataField 数据字段
    * @param parameter 参数
    * @param paramField 参数字段
    * @return 校验结果
    */
  def in(data:String, dataField:String, parameter:String, paramField:String):Boolean = {
    val paramFieldValue = StringUtils.getFieldFromConcatString(parameter, "\\|", paramField)
    if(paramFieldValue == null) {
      return true
    }
    val paramFieldValueSplited = paramFieldValue.split(",")

    val dataFieldValue = StringUtils.getFieldFromConcatString(data, "\\|", dataField)
    if(dataFieldValue != null && dataFieldValue != "-1") {
      val dataFieldValueSplited = dataFieldValue.split(",")

      for(singleDataFieldValue <- dataFieldValueSplited) {
        for(singleParamFieldValue <- paramFieldValueSplited) {
          if(singleDataFieldValue.compareTo(singleParamFieldValue) ==0) {
            return true
          }
        }
      }
    }
    false
  }

  /**
    * 校验数据中的指定字段，是否在指定范围内
    * @param data 数据
    * @param dataField 数据字段
    * @param parameter 参数
    * @param paramField 参数字段
    * @return 校验结果
    */
  def equal(data:String, dataField:String, parameter:String, paramField:String):Boolean = {
    val paramFieldValue = StringUtils.getFieldFromConcatString(parameter, "\\|", paramField)
    if(paramFieldValue == null) {
      return true
    }

    val dataFieldValue = StringUtils.getFieldFromConcatString(data, "\\|", dataField)
    if(dataFieldValue != null) {
      if(dataFieldValue.compareTo(paramFieldValue) == 0) {
        return true
      }
    }
    false
  }

}