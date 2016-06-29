package com.xiyuan.demo

import com.xiyuan.demo.model.{HotNews, HBaseTest}
import com.xiyuan.hbase.HBaseManager

/**
  * Created by YT on 2016/6/29.
  */
object App {

  def main(args: Array[String]) {
    HBaseManager.scan(classOf[HotNews], null, null).toArray().foreach(println)
  }

}
