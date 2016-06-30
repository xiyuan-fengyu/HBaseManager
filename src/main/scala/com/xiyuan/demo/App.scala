package com.xiyuan.demo

import java.util
import java.util.Date

import com.xiyuan.demo.model.{HotNews, HBaseTest}
import com.xiyuan.hbase.HBaseManager
import com.xiyuan.hbase.filter.ColumnFilter

import scala.util.matching.Regex

/**
  * Created by YT on 2016/6/29.
  */
object App {

  def main(args: Array[String]) {
    //删除表
//    HBaseManager.deleteTable(classOf[HBaseTest])

    //插入数据
//    val list = new util.ArrayList[HBaseTest]()
//    for(i <- 0 until 10) {
//      val temp = new HBaseTest()
//      temp.id = "%10d".format(i).replaceAll(" ", "0")
//      temp.column0 = "column0_" + i
//      temp.column1 = "column1_" + i
//      temp.column2 = "column2_" + i
//      list.add(temp)
//    }
//    HBaseManager.save(list)

    //根据rowId来查询记录
//    HBaseManager.scan(classOf[HBaseTest], null, null).toArray().foreach(println)


//    import com.xiyuan.demo.extension.RandomExt._
//    val threeDay: Long = 1000L * 3600L * 72
//    val now = new Date().getTime
//    val list = new util.ArrayList[HBaseTest]()
//    for(i <- 0 until 20;
//        j <- 0 until 100
//    ) {
//      val temp = new HBaseTest()
//      //rowkey格式：type（2位） + timestamp（13位） + uniqueId（5位）
//      temp.id = "%2d".format(i).replaceAll(" ", "0") + (now - threeDay.random) + "%5d".format(99999.random).replaceAll(" ", "0")
//      temp.column0 = "" + j
//      temp.column1 = "" + j
//      temp.column2 = "" + j
//      list.add(temp)
//    }
//    HBaseManager.save(list)

//    val threeDay: Long = 1000L * 3600L * 72
//    val now = new Date().getTime
//    HBaseManager.scan(classOf[HBaseTest],
//      "%2d".format(10).replaceAll(" ", "0") + (now - threeDay / 3),
//      "%2d".format(10).replaceAll(" ", "0") + (now),
//      new ColumnFilter("column0 >= 0.5"),
//      new ColumnFilter("column0 <= 99")
//    ).toArray().foreach(println)

  }

}
