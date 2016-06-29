package com.xiyuan.hbase;

import java.util.Properties;

public class Config {

	private static final Properties properties = ConfigUtil.loadProperties("config.properties");

	public static final String hbase_zookeeper_quorum = properties.getProperty("hbase.zookeeper.quorum");

	public static final String hbase_master = properties.getProperty("hbase.master");

	public static final int hbase_zookeeper_property_clientPort = Integer.parseInt(properties.getProperty("hbase.zookeeper.property.clientPort"));

}