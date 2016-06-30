package com.xiyuan.hbase;

import com.xiyuan.hbase.annotation.*;
import com.xiyuan.hbase.annotation.Column;
import com.xiyuan.hbase.filter.ColumnFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.xiyuan.hbase.filter.*;

public class HBaseManager {

	private static final Configuration conf = HBaseConfiguration.create();
	
	static {
		conf.set("hbase.zookeeper.property.clientPort", "" + HBaseConfig.hbase_zookeeper_property_clientPort);
		conf.set("hbase.zookeeper.quorum", HBaseConfig.hbase_zookeeper_quorum);
		conf.set("hbase.master", HBaseConfig.hbase_master);
	}
	
	private static final Map<Table, Connection> connections = new HashMap<Table, Connection>();

	private static <T> Table getTable(Class<T> clazz) {
		String tableName = getTableName(clazz);
		if (tableName == null || tableName.equals("")) {
			return null;
		}

		Connection connection = null;
		try {
			connection = ConnectionFactory.createConnection(conf);
			Admin admin = connection.getAdmin();
			TableName tableNameObj = TableName.valueOf(tableName);
			if (!admin.tableExists(tableNameObj)) {
				HTableDescriptor tblDesc = new HTableDescriptor(tableNameObj);
				Field[] fields = clazz.getFields();
				for (Field field: fields) {
					if (isColumn(field)) {
						tblDesc.addFamily(new HColumnDescriptor(getColumnName(field)));
					}
				}
				admin.createTable(tblDesc);
			}
			Table table = connection.getTable(TableName.valueOf(tableName));
			connections.put(table, connection);
			return table;
		} catch (IOException e) {
			e.printStackTrace();
			if (connection != null) {
				try {
					connection.close();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}
		}
		return null;
	}

	public static <T> void deleteTable(Class<T> clazz) {
		String tableName = getTableName(clazz);
		if (tableName == null || tableName.equals("")) {
			return;
		}

		Connection connection = null;
		try {
			connection = ConnectionFactory.createConnection(conf);
			Admin admin = connection.getAdmin();
			TableName tableNameObj = TableName.valueOf(tableName);
			if (admin.tableExists(tableNameObj)) {
				admin.disableTable(tableNameObj);
				admin.deleteTable(tableNameObj);
			}
		} catch (IOException e) {
			e.printStackTrace();
			if (connection != null) {
				try {
					connection.close();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}
		}
	}

	private static void releaseTable(Table table) {
		if (table != null) {
			Connection connection = connections.get(table);
			
			try {
				table.close();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			
			if (connection != null) {
				connections.remove(table);
				try {
					connection.close();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}
		}
	}
	
	public static <T> ArrayList<T> scan(Class<T> clazz, String startRow, String stopRow, ColumnFilter... filters) {
		ArrayList<T> resultList = new ArrayList<T>(); 
		
		final Table table = getTable(clazz);
    	if (table != null) {
    		try {
    			Scan scan = new Scan();
    			if (startRow != null && !startRow.equals("")) {
    				scan.setStartRow(Bytes.toBytes(startRow));
    			}
    			if (stopRow != null && !stopRow.equals("")) {
    				scan.setStopRow(Bytes.toBytes(stopRow));
    			}

				if (filters != null) {
					if (filters.length == 1) {
						SingleColumnValueFilter temp = ColumnFilterExt.ColumnFilterToSingleColumnValueFilter(filters[0]).singleFilter(clazz);
						if (temp != null) {
							scan.setFilter(temp);
						}
					}
					else {
						List<Filter> columnFilters = new ArrayList<Filter>();
						for (ColumnFilter f: filters) {
							SingleColumnValueFilter temp = ColumnFilterExt.ColumnFilterToSingleColumnValueFilter(f).singleFilter(clazz);
							if (temp != null) {
								columnFilters.add(temp);
							}
						}
						if (columnFilters.size() > 1) {
							FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, columnFilters);
							scan.setFilter(filterList);
						}
					}
				}

        		ResultScanner resultScanner = table.getScanner(scan);
        		Result next = resultScanner.next();
        		while (next != null) {
        			T tempT = resultToObject(next, clazz);
        			if (tempT != null) {
        				resultList.add(tempT);        				
        			}
        			
        			next = resultScanner.next();
        		}
    		} catch (IOException e) {
    			e.printStackTrace();
    		}
    		
    		releaseTable(table);
    	}
    	
    	return resultList;
	}
	
	public static <T> T find(Class<T> clazz, String rowId) {
		T t = null;
		
		final Table table = getTable(clazz);
    	if (table != null) {
    		Get get = new Get(Bytes.toBytes(rowId));
    		try {
				Result result = table.get(get);
				if (result != null) {
					t = resultToObject(result, clazz);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
    		
    		releaseTable(table);
    	}
		
		return t;
	}
	
	private static <T> T resultToObject(Result result, Class<T> clazz) {
		T t = null;
		
		if (result != null) {
			List<Cell> cells  = result.listCells();
			if (cells != null) {
				try {
					t = clazz.newInstance();
					setValue(clazz, t, getRowKeyName(clazz), result.getRow());

					for (Cell cell: cells) {
						String key = Bytes.toString(CellUtil.cloneFamily(cell));
						byte[] value = CellUtil.cloneValue(cell);
						setValue(clazz, t, key, value);
					}
				} catch (InstantiationException e) {
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				}
			}
		}
		return t;
	}
	
	public static <T> boolean save(T t) {
		if (t == null) {
			return false;
		}
		
		@SuppressWarnings("unchecked")
		Class<T> clazz = (Class<T>) t.getClass();
		
		final Table table = getTable(clazz);
    	if (table != null) {
    		Put put = createPut(clazz, t);
			if (put != null) {
				try {
					table.put(put);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
    		
    		releaseTable(table);
    	}
    	else {
    		return false;
    	}
    	
    	return true;
	}
	
	public static <T> boolean save(List<T> list) {
		if (list == null || list.size() == 0) {
			return false;
		}
		
		@SuppressWarnings("unchecked")
		Class<T> clazz = (Class<T>) list.get(0).getClass();
		
		final Table table = getTable(clazz);
    	if (table != null) {
    		List<Put> puts = new ArrayList<Put>();
    		for (T t : list) {
				Put put = createPut(clazz, t);
				if (put != null) {
					puts.add(put);
				}
			}
    		try {
				table.put(puts);
			} catch (IOException e) {
				e.printStackTrace();
			}
    		
    		releaseTable(table);
    	}
    	else {
    		return false;
    	}
    	
    	return true;
	}

	private static <T> String getTableName(Class<T> clazz) {
		com.xiyuan.hbase.annotation.Table tableAnno = clazz.getAnnotation(com.xiyuan.hbase.annotation.Table.class);
		if (tableAnno == null) {
			return null;
		}
		else {
			String tableName = tableAnno.name();
			if (tableName.equals("")) {
				tableName = clazz.getSimpleName();
			}
			return tableName;
		}
	}

	private static <T> String getRowKeyName(Class<T> clazz) {
		Field[] fields = clazz.getFields();
		for(Field field: fields) {
			RowKey rowKeyAnno = field.getAnnotation(RowKey.class);
			if (rowKeyAnno != null) {
				String rowIdAnnoName = rowKeyAnno.name();
				if (rowIdAnnoName.equals("")) {
					rowIdAnnoName = field.getName();
				}
				return rowIdAnnoName;
			}
		}
		return null;
	}

	private static String getRowKeyName(Field field) {
		RowKey rowKeyAnno = field.getAnnotation(RowKey.class);
		if (rowKeyAnno == null) {
			return null;
		}
		else {
			String rowIdAnnoName = rowKeyAnno.name();
			if (rowIdAnnoName.equals("")) {
				rowIdAnnoName = field.getName();
			}
			return rowIdAnnoName;
		}
	}
	
	private static String getColumnName(Field field) {
		Column columnAnno = field.getAnnotation(Column.class);
		if (columnAnno != null) {
			String columnName = columnAnno.name();
			if (columnName.equals("")) {
				columnName = field.getName();
			}
			return columnName;
		}
		else {
			return null;
		}
	}
	
	private static boolean isRowKey(Field field) {
		RowKey rowKeyAnno = field.getAnnotation(RowKey.class);
		return rowKeyAnno != null;
	}
	
	private static boolean isColumn(Field field) {
		Column columnAnno = field.getAnnotation(Column.class);
		return columnAnno != null;
	}
	
	private static <T> Put createPut(Class<T> clazz, T instance) {
		Field[] fields = clazz.getDeclaredFields();
		
		Put put = null;
		for (Field field : fields) {
			if (isRowKey(field)) {
				try {
					byte[] row = fieldValueToBytes(clazz, instance, field);
					if (row != null) {
						put = new Put(row);
					}
				} catch (IllegalArgumentException e) {
					e.printStackTrace();
				}
				break;
			}
		}
		
		if (put != null) {
			for (Field field : fields) {
				if (isColumn(field)) {
					byte[] columnName = Bytes.toBytes(getColumnName(field));
					byte[] columnValue = fieldValueToBytes(clazz, instance, field);
					put.addColumn(columnName, null, columnValue);
				}
			}
		}
		
		return put;
	}
	
	private static <T> byte[] fieldValueToBytes(Class<T> clazz, T instance, Field field) {
		byte[] result = null;
		
		Class<?> fieldType = field.getType();
		Object value = null;
		try {
			value = field.get(instance);
			if (fieldType == String.class) {
				result = Bytes.toBytes((String)value);
			}
			else if (fieldType == int.class || fieldType == Integer.class) {
				result = Bytes.toBytes((Integer)value);
			}
			else if (fieldType == long.class || fieldType == Long.class) {
				result = Bytes.toBytes((Long)value);
			}
			else if (fieldType == double.class || fieldType == Double.class) {
				result = Bytes.toBytes((Double)value);
			}
			else if (fieldType == float.class || fieldType == Float.class) {
				result = Bytes.toBytes((Float)value);
			}
			else if (fieldType == short.class || fieldType == Short.class) {
				result = Bytes.toBytes((Short)value);
			} 
			else if (fieldType == BigDecimal.class) {
				result = Bytes.toBytes((BigDecimal)value);
			}
			else if (fieldType == byte[].class || fieldType == Bytes[].class) {
				result = (byte[])value;
			}
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		
		return result;
	}
	
	private static <T> void setValue(Class<T> clazz, T instance, String key, byte[] value) {
		try {
			Field field = clazz.getDeclaredField(key);
			Class<?> fieldType = field.getType();
			if (fieldType == String.class) {
				field.set(instance, Bytes.toString(value));
			}
			else if (fieldType == int.class || fieldType == Integer.class) {
				field.set(instance, Bytes.toInt(value));
			}
			else if (fieldType == long.class || fieldType == Long.class) {
				field.set(instance, Bytes.toLong(value));
			}
			else if (fieldType == double.class || fieldType == Double.class) {
				field.set(instance, Bytes.toDouble(value));
			}
			else if (fieldType == float.class || fieldType == Float.class) {
				field.set(instance, Bytes.toFloat(value));
			}
			else if (fieldType == short.class || fieldType == Short.class) {
				field.set(instance, Bytes.toShort(value));
			} 
			else if (fieldType == BigDecimal.class) {
				field.set(instance, Bytes.toBigDecimal(value));
			}
			else if (fieldType == byte[].class || fieldType == Bytes[].class) {
				field.set(instance, value);
			}
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (NoSuchFieldException e) {
			e.printStackTrace();
		} catch (SecurityException e) {
			e.printStackTrace();
		}
	}
}
