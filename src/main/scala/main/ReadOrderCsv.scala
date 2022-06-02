package main

import main.ReadAndPut.reverseOrderID
import model.{EconData, Orderscript}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.net.URI
import java.util
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}

object ReadOrderCsv {
  def readCSV(sparkSession:SparkSession,inputfile:String): RDD[Orderscript] ={
    val csvread:RDD[Array[String]] = sparkSession.sparkContext.hadoopFile(inputfile,classOf[TextInputFormat]
      ,classOf[LongWritable],classOf[Text]).map(pair=>new String(pair._2.getBytes,0,pair._2.getLength,"GBK"))
      .map(x=>x.split(","))
    val csvreadRDD:RDD[Orderscript] =
      csvread.map(x=>Orderscript(x(1),x(3),x(4),x(6),x(8),x(9),x(10),x(11)))
    return csvreadRDD
  }

  //生成FileSystem
  def getHdfs(path: String): FileSystem = {
    val conf = new Configuration()
    FileSystem.newInstance(URI.create(path), conf)
  }
  //获取目录下的一级文件和目录
  def getFilesAndDirs(path: String): Array[Path] = {
    val fs = getHdfs(path).listStatus(new Path(path))
    FileUtil.stat2Paths(fs)
  }

  //获取目录下的所有文件
  def getAllFiles(path: String): ArrayBuffer[String] = {
    val arr = ArrayBuffer[String]()
    val hdfs = getHdfs(path)
    val getPath = getFilesAndDirs(path)
    getPath.foreach(patha => {
      if (hdfs.getFileStatus(patha).isFile())
        arr += patha.toString
      else {
        arr ++= getAllFiles(patha.toString())
      }
    })
    arr
  }

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("ReadOrderCsv").getOrCreate()
    val fileDir= "hdfs://master:9000/user/root/orderexcel"
    val orderCsvfiles = getAllFiles(fileDir)
    var data: RDD[Orderscript] = sparkSession.sparkContext.emptyRDD[Orderscript]
    orderCsvfiles.foreach(eachfile=>{
      data = data.union(readCSV(sparkSession,eachfile))
    })
    val list = new util.ArrayList[Put]()
      data.foreachPartition(records => {
        records.foreach(record=> {
          breakable(
          if(record.集团名称 == "集团名称")
            break()
          )
          val put = new Put(Bytes.toBytes(reverseOrderID(record.订单唯一明细)))
          put.addColumn(Bytes.toBytes("order"), Bytes.toBytes("company"), Bytes.toBytes(record.集团名称))
          put.addColumn(Bytes.toBytes("order"), Bytes.toBytes("city"), Bytes.toBytes(record.地市))
          put.addColumn(Bytes.toBytes("order"), Bytes.toBytes("phone"), Bytes.toBytes(record.联系人手机))
          put.addColumn(Bytes.toBytes("order"), Bytes.toBytes("order_Id"), Bytes.toBytes(record.订单唯一明细))
          put.addColumn(Bytes.toBytes("order"), Bytes.toBytes("product_type"), Bytes.toBytes(record.产品类型))
          put.addColumn(Bytes.toBytes("order"), Bytes.toBytes("product"), Bytes.toBytes(record.产品))
          put.addColumn(Bytes.toBytes("order"), Bytes.toBytes("region"), Bytes.toBytes(record.资源池))
          put.addColumn(Bytes.toBytes("order"), Bytes.toBytes("date"),Bytes.toBytes(record.日期))
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
      })





  }
}
