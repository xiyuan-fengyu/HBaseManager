package com.xiyuan.hbase

import com.xiyuan.hbase.annotation._
import com.xiyuan.hbase.annotation.Column
import com.xiyuan.hbase.filter.ColumnFilter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.client.Table
import org.apache.hadoop.hbase.filter.Filter
import org.apache.hadoop.hbase.filter.FilterList
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.util.Bytes
import java.io.IOException
import java.lang.reflect.Field
import java.math.BigDecimal
import com.xiyuan.hbase.filter._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object HBaseManager {

  private val conf: Configuration = HBaseConfiguration.create
  conf.set("hbase.zookeeper.property.clientPort", "" + HBaseConfig.hbase_zookeeper_property_clientPort)
  conf.set("hbase.zookeeper.quorum", HBaseConfig.hbase_zookeeper_quorum)
  conf.set("hbase.master", HBaseConfig.hbase_master)

  private val connections: mutable.HashMap[Table, Connection] = new mutable.HashMap[Table, Connection]

  private def getTable[T](clazz: Class[T]): Table = {
    val tableName: String = getTableName(clazz)
    if (tableName == null || (tableName == "")) {
      return null
    }
    var connection: Connection = null
    try {
      connection = ConnectionFactory.createConnection(conf)
      val admin: Admin = connection.getAdmin
      val tableNameObj: TableName = TableName.valueOf(tableName)
      if (!admin.tableExists(tableNameObj)) {
        val tblDesc: HTableDescriptor = new HTableDescriptor(tableNameObj)
        val fields: Array[Field] = clazz.getFields
        for (field <- fields) {
          if (isColumn(field)) {
            tblDesc.addFamily(new HColumnDescriptor(getColumnName(field)))
          }
        }
        admin.createTable(tblDesc)
      }
      val table: Table = connection.getTable(TableName.valueOf(tableName))
      connections.put(table, connection)
      table
    }
    catch {
      case e: IOException =>
        e.printStackTrace()
        if (connection != null) {
          try {
            connection.close()
          }
          catch {
            case e1: Exception =>
              e1.printStackTrace()
          }
        }
        null
    }
  }

  def deleteTable[T](clazz: Class[T]) {
    val tableName: String = getTableName(clazz)
    if (tableName == null || (tableName == "")) {
      return
    }
    var connection: Connection = null
    try {
      connection = ConnectionFactory.createConnection(conf)
      val admin: Admin = connection.getAdmin
      val tableNameObj: TableName = TableName.valueOf(tableName)
      if (admin.tableExists(tableNameObj)) {
        admin.disableTable(tableNameObj)
        admin.deleteTable(tableNameObj)
      }
    }
    catch {
      case e: IOException =>
        e.printStackTrace()
        if (connection != null) {
          try {
            connection.close()
          }
          catch {
            case e1: IOException =>
              e1.printStackTrace()
          }
        }
    }
  }

  private def releaseTable(table: Table) {
    if (table != null) {
      val connection = connections.get(table)
      try {
        table.close()
      }
      catch {
        case e1: IOException =>
          e1.printStackTrace()
      }
      if (connection.nonEmpty) {
        connections.remove(table)
        try {
          connection.get.close()
        }
        catch {
          case e1: IOException =>
            e1.printStackTrace()
        }
      }
    }
  }

  def scan[T: ClassTag](clazz: Class[T], startRow: String, stopRow: String, filters: ColumnFilter*): Array[T] = {
    val resultList: ArrayBuffer[T] = new ArrayBuffer[T]
    val table: Table = getTable(clazz)
    if (table != null) {
      try {
        val scan: Scan = new Scan
        if (startRow != null && !(startRow == "")) {
          scan.setStartRow(Bytes.toBytes(startRow))
        }
        if (stopRow != null && !(stopRow == "")) {
          scan.setStopRow(Bytes.toBytes(stopRow))
        }
        if (filters != null) {
          if (filters.length == 1) {
            val temp: SingleColumnValueFilter = ColumnFilterExt.ColumnFilterToSingleColumnValueFilter(filters(0)).singleFilter(clazz)
            if (temp != null) {
              scan.setFilter(temp)
            }
          }
          else {
            val columnFilters = new java.util.ArrayList[Filter]
            for (f <- filters) {
              val temp: SingleColumnValueFilter = ColumnFilterExt.ColumnFilterToSingleColumnValueFilter(f).singleFilter(clazz)
              if (temp != null) {
                columnFilters.add(temp)
              }
            }
            if (columnFilters.size() > 0) {
              val filterList: FilterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, columnFilters)
              scan.setFilter(filterList)
            }
          }
        }
        val resultScanner: ResultScanner = table.getScanner(scan)
        var next: Result = resultScanner.next
        while (next != null) {
          val tempT: T = resultToObject(next, clazz)
          if (tempT != null) {
            resultList += tempT
          }
          next = resultScanner.next
        }
      }
      catch {
        case e: IOException =>
          e.printStackTrace()
      }
      releaseTable(table)
    }
    resultList.toArray
  }

  def find[T](clazz: Class[T], rowId: String): T = {
    var t: T = null.asInstanceOf[T]
    val table: Table = getTable(clazz)
    if (table != null) {
      val get: Get = new Get(Bytes.toBytes(rowId))
      try {
        val result: Result = table.get(get)
        if (result != null) {
          t = resultToObject(result, clazz)
        }
      }
      catch {
        case e: IOException =>
          e.printStackTrace()
      }
      releaseTable(table)
    }
    t
  }

  private def resultToObject[T](result: Result, clazz: Class[T]): T = {
    var t: T = null.asInstanceOf[T]
    if (result != null) {
      val cells: java.util.List[Cell] = result.listCells
      if (cells != null) {
        try {
          t = clazz.newInstance
          setValue(clazz, t, getRowKeyName(clazz), result.getRow)
          import scala.collection.JavaConversions._
          for (cell <- cells) {
            val key: String = Bytes.toString(CellUtil.cloneFamily(cell))
            val value: Array[Byte] = CellUtil.cloneValue(cell)
            setValue(clazz, t, key, value)
          }
        }
        catch {
          case e: Exception =>
            e.printStackTrace()
        }
      }
    }
    t
  }

  def save[T: ClassTag](t: T): Boolean = {
    if (t == null) {
      return false
    }
    @SuppressWarnings(Array("unchecked")) val clazz: Class[T] = t.getClass.asInstanceOf[Class[T]]
    var flag = false
    val table: Table = getTable(clazz)
    if (table != null) {
      val put: Put = createPut(clazz, t)
      if (put != null) {
        try {
          table.put(put)
          flag = true
        }
        catch {
          case e: Exception =>
            e.printStackTrace()
        }
      }
      releaseTable(table)
    }
    flag
  }

  def saveList[T: ClassTag](list: Array[T]): Boolean = {
    var flag = false
    if (list != null && list.nonEmpty) {
      @SuppressWarnings(Array("unchecked")) val clazz: Class[T] = list(0).getClass.asInstanceOf[Class[T]]
      val table: Table = getTable(clazz)
      if (table != null) {
        val puts: java.util.List[Put] = new java.util.ArrayList[Put]
        for (t <- list) {
          val put: Put = createPut(clazz, t)
          if (put != null) {
            puts.add(put)
          }
        }
        try {
          table.put(puts)
          flag = true
        }
        catch {
          case e: Exception =>
            e.printStackTrace()
        }
        releaseTable(table)
      }
    }
    flag
  }

  private def getTableName[T](clazz: Class[T]): String = {
    val tableAnno: com.xiyuan.hbase.annotation.Table = clazz.getAnnotation(classOf[com.xiyuan.hbase.annotation.Table])
    if (tableAnno == null) {
      null
    }
    else {
      var tableName: String = tableAnno.name
      if (tableName == "") {
        tableName = clazz.getSimpleName
      }
      tableName
    }
  }

  private def getRowKeyName[T](clazz: Class[T]): String = {
    val fields: Array[Field] = clazz.getFields
    for (field <- fields) {
      val rowKeyAnno: RowKey = field.getAnnotation(classOf[RowKey])
      if (rowKeyAnno != null) {
        var rowIdAnnoName: String = rowKeyAnno.name
        if (rowIdAnnoName == "") {
          rowIdAnnoName = field.getName
        }
        return rowIdAnnoName
      }
    }
    null
  }

  private def getRowKeyName(field: Field): String = {
    val rowKeyAnno: RowKey = field.getAnnotation(classOf[RowKey])
    if (rowKeyAnno == null) {
      null
    }
    else {
      var rowIdAnnoName: String = rowKeyAnno.name
      if (rowIdAnnoName == "") {
        rowIdAnnoName = field.getName
      }
      rowIdAnnoName
    }
  }

  private def getColumnName(field: Field): String = {
    val columnAnno: Column = field.getAnnotation(classOf[Column])
    if (columnAnno != null) {
      var columnName: String = columnAnno.name
      if (columnName == "") {
        columnName = field.getName
      }
      columnName
    }
    else {
      null
    }
  }

  private def isRowKey(field: Field): Boolean = {
    field.getAnnotation(classOf[RowKey]) != null
  }

  private def isColumn(field: Field): Boolean = {
    field.getAnnotation(classOf[Column]) != null
  }

  private def createPut[T](clazz: Class[T], instance: T): Put = {
    val fields: Array[Field] = clazz.getDeclaredFields
    var put: Put = null
    for (field <- fields; if put == null) {
      if (isRowKey(field)) {
        try {
          val row: Array[Byte] = fieldValueToBytes(clazz, instance, field)
          if (row != null) {
            put = new Put(row)
          }
        }
        catch {
          case e: Exception =>
            e.printStackTrace()
        }
      }
    }
    if (put != null) {
      for (field <- fields) {
        if (isColumn(field)) {
          val columnName: Array[Byte] = Bytes.toBytes(getColumnName(field))
          val columnValue: Array[Byte] = fieldValueToBytes(clazz, instance, field)
          put.addColumn(columnName, null, columnValue)
        }
      }
    }
    put
  }

  private def fieldValueToBytes[T](clazz: Class[T], instance: T, field: Field): Array[Byte] = {
    var result: Array[Byte] = null
    val fieldType: Class[_] = field.getType
    var value: AnyRef = null
    try {
      value = field.get(instance)
      if (fieldType ==classOf[String]) {
        result = Bytes.toBytes(value.asInstanceOf[String])
      }
      else if (fieldType == classOf[Int] || fieldType == classOf[Integer]) {
        result = Bytes.toBytes(value.asInstanceOf[Integer])
      }
      else if (fieldType == classOf[Long] || fieldType == classOf[Long]) {
        result = Bytes.toBytes(value.asInstanceOf[Long])
      }
      else if (fieldType == classOf[Double] || fieldType == classOf[Double]) {
        result = Bytes.toBytes(value.asInstanceOf[Double])
      }
      else if (fieldType == classOf[Float] || fieldType == classOf[Float]) {
        result = Bytes.toBytes(value.asInstanceOf[Float])
      }
      else if (fieldType == classOf[Short] || fieldType == classOf[Short]) {
        result = Bytes.toBytes(value.asInstanceOf[Short])
      }
      else if (fieldType ==classOf[BigDecimal]) {
        result = Bytes.toBytes(value.asInstanceOf[BigDecimal])
      }
      else if (fieldType == classOf[Array[Byte]] || fieldType == classOf[Array[Bytes]]) {
        result = value.asInstanceOf[Array[Byte]]
      }
    }
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
    result
  }

  private def setValue[T](clazz: Class[T], instance: T, key: String, value: Array[Byte]) {
    try {
      val field: Field = clazz.getDeclaredField(key)
      val fieldType: Class[_] = field.getType
      if (fieldType ==classOf[String]) {
        field.set(instance, Bytes.toString(value))
      }
      else if (fieldType ==classOf[Int] || fieldType ==classOf[Integer]) {
        field.set(instance, Bytes.toInt(value))
      }
      else if (fieldType ==classOf[Long] || fieldType ==classOf[Long]) {
        field.set(instance, Bytes.toLong(value))
      }
      else if (fieldType ==classOf[Double] || fieldType ==classOf[Double]) {
        field.set(instance, Bytes.toDouble(value))
      }
      else if (fieldType ==classOf[Float] || fieldType ==classOf[Float]) {
        field.set(instance, Bytes.toFloat(value))
      }
      else if (fieldType ==classOf[Short] || fieldType ==classOf[Short]) {
        field.set(instance, Bytes.toShort(value))
      }
      else if (fieldType ==classOf[BigDecimal]) {
        field.set(instance, Bytes.toBigDecimal(value))
      }
      else if (fieldType ==classOf[Array[Byte]] || fieldType ==classOf[Array[Bytes]]) {
        field.set(instance, value)
      }
    }
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }


}

