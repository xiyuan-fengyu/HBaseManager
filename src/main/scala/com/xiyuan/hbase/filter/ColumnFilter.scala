package com.xiyuan.hbase.filter

import com.xiyuan.hbase.annotation.Column
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.util.Bytes

import scala.util.matching.Regex

/**
  * Created by xiyuan_fengyu on 2016/6/30.
  */
/**
  * express的格式为([^=!<>]+)(=|!=|<|<=|>|>=)([^=!<>]*)
  * 操作符只支持：=  !=  <   <=  >   >=
  * 可以通过ColumnFilterToSingleColumnValueFilter扩展的singleFilter方法转换为SingleColumnValueFilter
  * 注意： column0是对象的属性字段名，在转换的过程中会转换为hbase数据库中的字段名
  * 例如：column0<=0,操作符两边可以有空格，例如column0 <= 0
  * ColumnFilter("column0<=0").singleFilter
  *
  * @param express 过滤条件
  */
case class ColumnFilter(express: String)

object ColumnFilterExt {

  private val expressRegex = "([^=!<>]+)(=|!=|<|<=|>|>=)([^=!<>]*)"

  implicit class ColumnFilterToSingleColumnValueFilter(filter: ColumnFilter) {

    def singleFilter[T](clazz: Class[T]): SingleColumnValueFilter = {
      val express = filter.express.replaceAll(" ", "")
      if (express.matches(expressRegex)) {
        val regex = new Regex(expressRegex)
        val regex(column, option, value) = express

        val columnName: String = try {
          val field = clazz.getField(column)
          val columnAnno = field.getAnnotation(classOf[Column])
          if (columnAnno == null) {
            column
          }
          else {
            if (columnAnno.name().equals("")) {
              column
            }
            else {
              columnAnno.name()
            }
          }
        }
        catch {
          case e: Exception =>
            column
        }

        val op: CompareOp = option match  {
          case "=" =>
            CompareOp.EQUAL
          case "!=" =>
            CompareOp.NOT_EQUAL
          case "<" =>
            CompareOp.LESS
          case "<=" =>
            CompareOp.LESS_OR_EQUAL
          case ">" =>
            CompareOp.GREATER
          case ">=" =>
            CompareOp.GREATER_OR_EQUAL
          case _ =>
            CompareOp.NO_OP
        }
        new SingleColumnValueFilter(Bytes.toBytes(columnName), null, op, Bytes.toBytes(value))
      }
      else null
    }

  }

}
