package com.xiyuan.demo.model;

import com.xiyuan.hbase.annotation.Column;
import com.xiyuan.hbase.annotation.RowKey;
import com.xiyuan.hbase.annotation.Table;

/**
 * Created by xiyuan_fengyu on 2016/7/1.
 */
@Table(name="hbaseTest")
public class HBaseTest {

	@RowKey
	public String id;
	
	@Column
	public String column0;
	
	@Column
	public String column1;
	
	@Column
	public String column2;
	
	@Override
	public String toString() {
		return "id = " + id + "\n" + 
				"column0 = " + column0 + "\n" + 
				"column1 = " + column1 + "\n" + 
				"column2 = " + column2 + "\n";
	}
}
