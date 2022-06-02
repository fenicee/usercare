package main


import model.EconData
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes

import java.util


object ReadAndPut {



  def readCSV(sparkSession:SparkSession,inputfile:String): RDD[EconData] ={
    val csvread:RDD[Array[String]] = sparkSession.sparkContext.hadoopFile(inputfile,classOf[TextInputFormat]
      ,classOf[LongWritable],classOf[Text]).map(pair=>new String(pair._2.getBytes,0,pair._2.getLength,"GBK"))
      .map(x=>x.split(","))
    val csvreadRDD:RDD[EconData] = csvread.map(x=>EconData(x(0),x(1),x(14),x(15),x(16),x(18),x(22),x(35),x(36),x(37),x(44)))
    return csvreadRDD
  }


  def reverseOrderID(orderId: String): String = {
    val reversed = ""
    var newStr = ""
    for ( i<-0 until orderId.length)
      newStr = orderId.charAt(i) +newStr
    return newStr
  }

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("sparkRWhbase").getOrCreate()
    val inputfile = "hdfs://master:9000/user/root/csvfiles/ecloudata.csv"
    // 本地测试
    //val inputfile = "C:\\Users\\linhaowen\\Downloads\\ecloudata.csv"
    val data = readCSV(sparkSession,inputfile)

    val list = new util.ArrayList[Put]()
    data.foreachPartition(records => {
      records.foreach(record=> {
        val put = new Put(Bytes.toBytes(reverseOrderID(record.orderId)))
        put.addColumn(Bytes.toBytes("C1"), Bytes.toBytes("orderId"), Bytes.toBytes(record.orderId))
        put.addColumn(Bytes.toBytes("C1"), Bytes.toBytes("orderProcessTime"), Bytes.toBytes(record.orderProcessTime))
        put.addColumn(Bytes.toBytes("C1"), Bytes.toBytes("city"), Bytes.toBytes(record.city))
        put.addColumn(Bytes.toBytes("C1"), Bytes.toBytes("country"), Bytes.toBytes(record.country))
        put.addColumn(Bytes.toBytes("C1"), Bytes.toBytes("companyName"), Bytes.toBytes(record.companyName))
        put.addColumn(Bytes.toBytes("C1"), Bytes.toBytes("companyId"), Bytes.toBytes(record.companyId))
        put.addColumn(Bytes.toBytes("C1"), Bytes.toBytes("region"), Bytes.toBytes(record.region))
        put.addColumn(Bytes.toBytes("C1"), Bytes.toBytes("managerName"), Bytes.toBytes(record.managerName))
        put.addColumn(Bytes.toBytes("C1"), Bytes.toBytes("managerPhone"), Bytes.toBytes(record.managerPhone))
        put.addColumn(Bytes.toBytes("C1"), Bytes.toBytes("managerId"), Bytes.toBytes(record.managerId))
        put.addColumn(Bytes.toBytes("C1"), Bytes.toBytes("productName"), Bytes.toBytes(record.productName))
        list.add(put)
      }
      )
      val config = HBaseConfiguration.create()
      config.set("hbase.zookeeper.property.clientPort", "2181")
      config.set("hbase.zookeeper.quorum", "master,slave1,slave2")
      val connection = ConnectionFactory.createConnection(config)
      val table = connection.getTable(TableName.valueOf("income_data:income"))
      table.put(list)
      table.close
    }
    )








  }
}
