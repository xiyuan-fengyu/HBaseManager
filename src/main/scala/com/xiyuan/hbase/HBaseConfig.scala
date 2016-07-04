package com.xiyuan.hbase

import com.xiyuan.hbase.util.ConfigUtil

/**
	* Created by xiyuan_fengyu on 2016/7/1.
	*/
object HBaseConfig {

	private val properties = ConfigUtil.loadProperties("HBaseConfig.properties")

	val hbase_zookeeper_quorum = properties.getProperty("hbase.zookeeper.quorum")

	val hbase_master = properties.getProperty("hbase.master")

	val hbase_zookeeper_property_clientPort = properties.getProperty("hbase.zookeeper.property.clientPort").toInt

}