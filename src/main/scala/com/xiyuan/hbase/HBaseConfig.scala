package com.xiyuan.hbase

import com.xiyuan.hbase.util.ConfigUtil

object HBaseConfig {

	private val properties = ConfigUtil.loadProperties("HBaseConfig.properties")

	val hbase_zookeeper_quorum = properties.getProperty("hbase.zookeeper.quorum")

	val hbase_master = properties.getProperty("hbase.master")

	val log4j_config = properties.getProperty("log4j.config")

	val hbase_zookeeper_property_clientPort = properties.getProperty("hbase.zookeeper.property.clientPort").toInt

}