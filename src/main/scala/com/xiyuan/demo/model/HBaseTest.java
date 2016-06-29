package com.xiyuan.demo.model;

import com.xiyuan.hbase.annotation.Column;
import com.xiyuan.hbase.annotation.RowId;
import com.xiyuan.hbase.annotation.Table;

@Table(name="hbaseTest")
public class HBaseTest {

	@RowId
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
