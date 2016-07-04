package com.xiyuan.demo.extension

/**
  * Created by xiyuan_fengyu on 2016/6/30.
  */
object RandomExt {

  implicit class IntRandom(i: Int) {

    var random: Int = {
      (math.random * i).toInt
    }

  }

  implicit class LongRandom(i: Long) {

    var random: Long = {
      (math.random * i).toLong
    }

  }

}
