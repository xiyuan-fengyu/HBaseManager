package com.xiyuan.demo

import java.util.Date

import com.xiyuan.demo.model.{DirtyMsg, HotNews, HBaseTest}
import com.xiyuan.hbase.HBaseManager
import com.xiyuan.hbase.filter.ColumnFilter
import com.xiyuan.demo.extension.RandomExt._
import com.xiyuan.template.log.XYLog

import scala.collection.mutable.ArrayBuffer

/**
  * Created by xiyuan_fengyu on 2016/6/29.
  */
object App {

  def main(args: Array[String]) {
    //用于初始化连接（第一次连接需要较长时间）
//    HBaseManager.testConnection(classOf[HBaseTest])

    //删除表
//    HBaseManager.deleteTable(classOf[HBaseTest])

//    //插入一条数据
//    val now = new Date().getTime
//    val temp = new HBaseTest()
//    //rowkey格式：type（2位） + timestamp（13位） + uniqueId（5位）
//    temp.id = "%2d".format(21).replaceAll(" ", "0") + now + "%5d".format(99999.random).replaceAll(" ", "0")
//    temp.column0 = "" + 0
//    temp.column1 = "" + 0
//    temp.column2 = "" + 0
//    HBaseManager.save(temp)
//    //根据rowId来查询记录
//    println(HBaseManager.find(classOf[HBaseTest], temp.id))

    //插入一组数据
//    val threeDay: Long = 1000L * 3600L * 72
//    val now = new Date().getTime
//    val list = ArrayBuffer[HBaseTest]()
//    for(i <- 0 until 20;
//        j <- 0 until 100
//    ) {
//      val temp = new HBaseTest()
//      //rowkey格式：type（2位） + timestamp（13位） + uniqueId（5位）
//      temp.id = "%2d".format(i).replaceAll(" ", "0") + (now - threeDay.random) + "%5d".format(99999.random).replaceAll(" ", "0")
//      temp.column0 = "" + j
//      temp.column1 = "" + j
//      temp.column2 = "" + j
//      list += temp
//    }
//    HBaseManager.saveList(list.toArray)

    //查询所有
//    HBaseManager.scan(classOf[HBaseTest], null, null).foreach(println)

    //设置了起始rowkey和过滤条件的查询
//    val threeDay: Long = 1000L * 3600L * 72
//    val now = new Date().getTime
//    HBaseManager.scan(classOf[HBaseTest],
//      "%2d".format(10).replaceAll(" ", "0") + (now - threeDay / 3),
//      "%2d".format(10).replaceAll(" ", "0") + now,
//      ColumnFilter("column0 >= 50"),
//      ColumnFilter("column0 <= 60")
//    ).foreach(println)


    //获取记录
//    XYLog.d(HBaseManager.find(classOf[DirtyMsg], "000021816862"))

    //删除一条记录
//    HBaseManager.delete(classOf[DirtyMsg], "000021816862")
//    XYLog.d(HBaseManager.find(classOf[DirtyMsg], "000021816862"))
//
//    //删除多条记录
//    XYLog.d(HBaseManager.scan(classOf[DirtyMsg],"000021816862", "000021816865"))
//    HBaseManager.deleteList(classOf[DirtyMsg], Array("000021816862", "000021816865"))
//    XYLog.d(HBaseManager.scan(classOf[DirtyMsg],"000021816862", "000021816865"))
  }

}
