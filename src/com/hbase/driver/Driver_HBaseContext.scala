package com.hcsc.hbase.prac

import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Driver_HBaseContext {
  
  def main(args: Array[String]) ={
   
  //Through Hbase context we can retrieve hbase data as rdd
    
  val sparkconf = new SparkConf().setAppName("hbase access")
  val sc = new SparkContext(sparkconf)
  
  val hbaseconf = HBaseConfiguration.create()  
  val hbaseContext = new HBaseContext(sc, hbaseconf)
  val tablename = "cust_dtl"
  
  bulkGetMethod(hbaseContext,sc,tablename) //calling bulkget funtion
  
  }
  
  
//Definition to get the data using bulkGet API which can filter the data at HBase itself and retrieve only the filtered data thus memory saved
  def bulkGetMethod(hbaseContext: HBaseContext, sc: SparkContext, tableName: String) = {
   val keyList = List("1001", "1003")
    val keyRdd = sc.parallelize(keyList)
    val finalKeyRdd = keyRdd.map(x => Bytes.toBytes(x))

    val getRdd = hbaseContext.bulkGet[Array[Byte], String](TableName.valueOf(tableName), 10, finalKeyRdd,
      record => {
        new Get(record)
      },
      (result: Result) => {
        val b = new StringBuilder
        if (!result.isEmpty()) {
          val it = result.rawCells().iterator
          while (it.hasNext) {
            val kv = it.next()
            //val q = Bytes.toString(kv.getQualifierArray())
            b.append(Bytes.toString(CellUtil.cloneFamily(kv)) + ":")
            b.append(Bytes.toString(CellUtil.cloneQualifier(kv)) + " -> ")
            b.append(Bytes.toString(CellUtil.cloneValue(kv)) + " ")
          }
          b.toString.dropRight(1)
        }
        else {
          ""
        }
      })
    getRdd.collect().foreach(v => println(v))
    
  }
  
}