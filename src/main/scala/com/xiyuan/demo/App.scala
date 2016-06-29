package com.xiyuan.demo

import com.xiyuan.demo.model.HBaseTest
import com.xiyuan.hbase.HBaseManager

/**
  * Created by YT on 2016/6/29.
  */
object App {

  def main(args: Array[String]) {
    HBaseManager.scan(classOf[HBaseTest], "row0", null).toArray().foreach(println)
  }

}
