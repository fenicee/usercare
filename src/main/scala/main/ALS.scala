package main

import main.ReadOrderCsv.{getAllFiles, readCSV}
import model.Orderscript
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ALS {
  def main(args: Array[String]): Unit = {
//    val sparkSession = SparkSession.builder().appName("ALS").getOrCreate()
//    Logger.getRootLogger.setLevel(Level.WARN)
//
    val spark = SparkSession.builder.appName("testApp").getOrCreate()
    val fileDir= "hdfs://master:9000/user/root/orderexcel"
    val orderCsvfiles = getAllFiles(fileDir)
    var data: RDD[Orderscript] = spark.sparkContext.emptyRDD[Orderscript]
    orderCsvfiles.foreach(eachfile=>{
      data = data.union(readCSV(spark,eachfile))
    })
    data.collect().foreach(f=>{
      println(f.日期)
    })
    spark.stop()
  }
}
