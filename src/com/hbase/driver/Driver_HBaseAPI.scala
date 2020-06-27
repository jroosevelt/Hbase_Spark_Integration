package com.hcsc.hbase.prac


import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.{ TableName, HTableDescriptor, HColumnDescriptor, CellUtil }
import org.apache.hadoop.hbase.client.{ Admin, Get, Put, Connection, Table, Scan, Result }
import org.apache.hadoop.hbase.util.Bytes

object Driver_HBaseAPI {
	//lazy val log = Logger.getLogger(this.getClass.getName)
	
  //Through Hbase scala api we can retrieve hbase data as string. No need to create spark/hbase context
  
  def main(args: Array[String]) = {
    //Yarn level setting logs
					//Logger.getLogger("org").setLevel(Level.WARN)

					//Instantiating Configurations
					val hbconf = HBaseConfiguration.create()			
					val hbaseConnection = ConnectionFactory.createConnection(hbconf)					
					val tableConnection = hbaseConnection.getTable(TableName.valueOf("cust_dtl_api"))
					
					//getTable(tableConnection)
					putTable(tableConnection)
					scanTable(tableConnection)
					
					hbaseConnection.close
					tableConnection.close
  }
	
	def printRow(result: Result) = {
			val rawCell = result.rawCells()
					print(Bytes.toString(result.getRow) + " : ")

					for (cell <- rawCell) {
						val colFamily = Bytes.toString(CellUtil.cloneFamily(cell))
								val colName = Bytes.toString(CellUtil.cloneQualifier(cell))
								val colValue = Bytes.toString(CellUtil.cloneValue(cell))
								print("(%s:%s %s) ".format(colFamily, colName, colValue))
					}
			println()
	}

	//Definition to retrieve data based on single key
	def getTable(tableConnection: Table) = {

			val get = new Get(Bytes.toBytes("1001"))
			val result = tableConnection.get(get)
					/*val byteRes = result.getValue(Bytes.toBytes("personal"), Bytes.toBytes("name"))
    val finalRes = Bytes.toString(byteRes)
    println(finalRes)*/
					printRow(result)
					//log.warn("""Data has been retrieved for 1001 row key""")
	}
	
	//Definition to scan complete table
	def scanTable(tableConnection: Table) = {

			val scan = new Scan()
					val resultSet = tableConnection.getScanner(scan)
					val iterRes = resultSet.iterator()
					var nextRes = iterRes.next()

					while (iterRes.hasNext()) {
						printRow(nextRes)
						nextRes = iterRes.next()
					}
			printRow(nextRes)
			
	}

	def putTable(tableConnection: Table) = {

			    val put = new Put(Bytes.toBytes("1003"))
					put.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("name"), Bytes.toBytes("roose"))
					put.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("age"), Bytes.toBytes("32"))

					val put1 = new Put(Bytes.toBytes("1004"))
					put1.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("name"), Bytes.toBytes("sara"))
					put1.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("age"), Bytes.toBytes("31"))

					val put2 = new Put(Bytes.toBytes("1005"))
					put2.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("name"), Bytes.toBytes("david"))
					put2.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("age"), Bytes.toBytes("24"))

					tableConnection.put(put)
					tableConnection.put(put1)
					tableConnection.put(put2)

					
	}
	 

}