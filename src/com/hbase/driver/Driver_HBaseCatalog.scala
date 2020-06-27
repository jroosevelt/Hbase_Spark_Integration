package com.hcsc.hbase.prac


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql
import org.apache.spark.SparkConf
import org.apache.spark.sql.datasources.hbase.HBaseTableCatalog


object Driver_HBaseCatalog {
  
  //Through Hbase catalog we can retrieve hbase data as dataframe
  
  def main(args:Array[String]) {      
   //val sparkConf = new SparkConf().setAppName("hbase access").setMaster("yarn")
   val spark = SparkSession.builder()
                           .appName("any_session")
                           //.master("yarn")
                           .enableHiveSupport()
                           //.config(sparkConf)
                           .getOrCreate()
       //import spark.implicits._
       val act_tbl_df = spark.sqlContext.read.options(Map(HBaseTableCatalog.tableCatalog->cust_dtl_api_catalog)).format("org.apache.spark.sql.execution.datasources.hbase").load()
			 act_tbl_df.show(false)               
  }
  
  
  //Define the hbase catalog/schema
  def cust_dtl_api_catalog = s"""{
			|"table":{"namespace":"default", "name":"cust_dtl_api"},"tableCoder":"PrimitiveType",
			|"rowkey":"key",
			|"columns":{
			|"studId":{"cf":"rowkey", "col":"key", "type":"string"},
			|"name":{"cf":"personal", "col":"name", "type":"string"},
			|"age":{"cf":"personal", "col":"age", "type":"string"},
			|"sex":{"cf":"personal", "col":"sex", "type":"string"},
			|"dept":{"cf":"personal", "col":"dept", "type":"string"}
			| }
			|}""".stripMargin;
  
}