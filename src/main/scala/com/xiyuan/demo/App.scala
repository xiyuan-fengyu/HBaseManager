package com.xiyuan.demo

import java.util

import com.xiyuan.demo.model.{HotNews, HBaseTest}
import com.xiyuan.hbase.HBaseManager

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
    HBaseManager.scan(classOf[HBaseTest], null, null).toArray().foreach(println)
  }

}
