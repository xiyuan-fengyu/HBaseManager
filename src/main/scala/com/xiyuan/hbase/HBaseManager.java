package com.xiyuan.hbase;

import com.xiyuan.hbase.annotation.*;
import com.xiyuan.hbase.annotation.Column;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HBaseManager {

	private static final Configuration conf = HBaseConfiguration.create();
	
	static {
		conf.set("hbase.zookeeper.property.clientPort", "" + Config.hbase_zookeeper_property_clientPort);
		conf.set("hbase.zookeeper.quorum", Config.hbase_zookeeper_quorum);
		conf.set("hbase.master", Config.hbase_master);
	}
	
	private static final Map<Table, Connection> connections = new HashMap<Table, Connection>();
	
	private static Table getTable(String tableName) {
		Connection connection = null;
		try {
			connection = ConnectionFactory.createConnection(conf);
			Table table = connection.getTable(TableName.valueOf(tableName));
			if (table != null) {
				connections.put(table, connection);
			}
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
	
	public static <T> ArrayList<T> scan(Class<T> clazz, String startRow, String stopRow) {
		ArrayList<T> resultList = new ArrayList<T>(); 
		
		String tableName = getTableName(clazz);
		if (tableName == null || tableName.equals("")) {
			return resultList;
		}
		
		final Table table = getTable(tableName);
    	if (table != null) {
    		try {
    			Scan scan = new Scan();
    			if (startRow != null && !startRow.equals("")) {
    				scan.setStartRow(Bytes.toBytes(startRow));
    			}
    			if (stopRow != null && !stopRow.equals("")) {
    				scan.setStopRow(Bytes.toBytes(stopRow));
    			}
        		ResultScanner resultScanner = table.getScanner(scan);
        		Result next = resultScanner.next();
        		while (next != null) {
        			T tempT = cellsToObject(clazz, next.listCells());
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
		
		String tableName = getTableName(clazz);
		if (tableName == null || tableName.equals("")) {
			return t;
		}
		
		final Table table = getTable(tableName);
    	if (table != null) {
    		Get get = new Get(Bytes.toBytes(rowId));
    		try {
				Result result = table.get(get);
				if (result != null) {
					t = cellsToObject(clazz, result.listCells());
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
    		
    		releaseTable(table);
    	}
		
		return t;
	}
	
	private static <T> T cellsToObject(Class<T> clazz, List<Cell> cells) {
		T t = null;
		
		if (cells != null) {
			try {
				t = clazz.newInstance();
				for (int i = 0, size = cells.size(); i < size; i++) {
					Cell cell = cells.get(i);
					if (i == 0) {
						String family = Bytes.toString(CellUtil.cloneFamily(cell));
						byte[] row = CellUtil.cloneRow(cell);
						setValue(clazz, t, family, row);
					}
					
					String key = Bytes.toString(CellUtil.cloneQualifier(cell));
					byte[] value = CellUtil.cloneValue(cell);
					setValue(clazz, t, key, value);
				}
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
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
		
		String tableName = getTableName(clazz);
		if (tableName == null || tableName.equals("")) {
			return false;
		}
		
		final Table table = getTable(tableName);
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
		
		String tableName = getTableName(clazz);
		if (tableName == null || tableName.equals("")) {
			return false;
		}
		
		final Table table = getTable(tableName);
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
	
	private static String getFamilyName(Field field) {
		Family familyAnno = field.getAnnotation(Family.class);
		if (familyAnno == null) {
			return null;
		}
		else {
			String familyName = familyAnno.name();
			if (familyName.equals("")) {
				familyName = field.getName();
			}
			return familyName;
		}
	}
	
	private static String getColumnName(Field field) {
		Column columnAnno = field.getAnnotation(Column.class);
		if (columnAnno == null) {
			return null;
		}
		else {
			String columnName = columnAnno.name();
			if (columnName.equals("")) {
				columnName = field.getName();
			}
			return columnName;
		}
	}
	
	private static boolean isFamily(Field field) {
		Family familyAnno = field.getAnnotation(Family.class);
		return familyAnno != null;
	}
	
	private static boolean isColumn(Field field) {
		Column columnAnno = field.getAnnotation(Column.class);
		return columnAnno != null;
	}
	
	private static <T> Put createPut(Class<T> clazz, T instance) {
		Field[] fields = clazz.getDeclaredFields();
		
		Put put = null;
		byte[] family = null;
		for (Field field : fields) {
			if (isFamily(field)) {
				try {
					byte[] row = fieldValueToBytes(clazz, instance, field);
					if (row != null) {
						put = new Put(row);
						family = Bytes.toBytes(getFamilyName(field));
					}
				} catch (IllegalArgumentException e) {
					e.printStackTrace();
				}
				break;
			}
		}
		
		if (put != null && family != null) {
			for (Field field : fields) {
				if (isColumn(field)) {
					byte[] columnName = Bytes.toBytes(getColumnName(field));
					byte[] columnValue = fieldValueToBytes(clazz, instance, field);
					put.addColumn(family, columnName, columnValue);
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
