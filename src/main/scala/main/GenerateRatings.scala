package main

import main.ReadAndPut.reverseOrderID
import model.{Complaint, OrderInfo, RatingModel, Satisfaction}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Result}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row.empty.schema
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import java.util
import scala.language.postfixOps
import scala.util.control.Breaks.{break, breakable}
object GenerateRatings {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").appName("generateRatings")
      .getOrCreate()
    val config = HBaseConfiguration.create()
    config.set("hbase.zookeeper.property.clientPort", "2181")
    config.set("hbase.zookeeper.quorum", "master,slave1,slave2")
    config.set(TableInputFormat.INPUT_TABLE, "income_data:income")
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val orderDF= loadDatafromHBase(sparkSession,config)
    orderDF.createOrReplaceGlobalTempView("order")
//    val users = sparkSession.sql("select company from order group by company")
//    val producType = sparkSession.sql("select producType from order group by producType")
    //val satisfactionRDD = loadDatafromCsvFile(sparkSession)._1

    //val satisfactionRowRDD = satisfactionRDD.map(fields=>Row(fields.company, fields.wholefeeling,fields.investigatedate))
    val satisfactionSchema:StructType=StructType(Array(StructField("company",StringType),StructField("wholefeeling",StringType),
      StructField("investigatedate",StringType)))
    val satisfactionDF = sparkSession.read.option("header", "true").option("encoding","gbk")
      .option("delimiter", ",").schema(satisfactionSchema).csv("hdfs://master:9000/user/root/csvfiles/质检清单.csv")
    //val satisfactionDF = sparkSession.createDataFrame(satisfactionRowRDD,satisfactionSchema)
    satisfactionDF.createOrReplaceTempView("satisfaction")
    val satisfactionCleaned= satisfactionDF.filter("wholefeeling is not null ")
//    val complaintRDD = loadDatafromCsvFile(sparkSession)._2
//    val complaintRowRDD = complaintRDD.map(fields=>Row(fields.companyName,fields.Eproduct,fields.complaintDate))
    val complaintSchema=StructType(Array(StructField("complaintDate",StringType),StructField("companyName",StringType),
      StructField("Eproduct",StringType)))
//    val complaintDF = sparkSession.createDataFrame(complaintRowRDD,complaintSchema)
    val complaintDF =  sparkSession.read.option("header", "true").schema(complaintSchema).option("encoding","gbk")
      .option("delimiter", ",").csv("hdfs://master:9000/user/root/csvfiles/云网支撑(220511).csv")
    complaintDF.createOrReplaceTempView("complaint")
    val complaintCleaned = complaintDF.filter(" companyName is not null and Eproduct != '无'")

    val eachRecord = sparkSession.sql("select global_temp.order.company,global_temp.order.producType from global_temp.order group by global_temp.order.company,global_temp.order.producType").collectAsList()
    eachRecord.forEach(
      x=>{
        val modeList = new util.ArrayList[RatingModel]()
        modeList.add(computeRatings(x(0).toString,x(1).toString))
      }
    )




  }

  def loadDatafromHBase(sparkSession:SparkSession,config:Configuration)={
    val RDD = sparkSession.sparkContext.newAPIHadoopRDD(config, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    import sparkSession.implicits._
    val orderRDD:RDD[OrderInfo]=RDD.map{case (_,result) =>{
      val rowkey = Bytes.toString(result.getRow)
      //通过列族和列名获取列
      val 集团名称: String = Bytes.toString(result.getValue(Bytes.toBytes("order"), Bytes.toBytes("company")))
      val 产品类型: String = Bytes.toString(result.getValue(Bytes.toBytes("order"), Bytes.toBytes("product_type")))
      val 日期:String = Bytes.toString(result.getValue(Bytes.toBytes("order"), Bytes.toBytes("date")))

      new OrderInfo(集团名称,日期,产品类型)
    }
    }
    orderRDD.toDF()

  }

  def computePunish(company: String, producType: String) = {
    //

  }

  def computeRatings(company: String, producType: String)={
    var rating = new RatingModel(company,producType,0.0)
    val punish = computePunish(company,producType)


    rating
  }


//  def loadDatafromCsvFile(sparkSession:SparkSession)={
//    val satisfactionCsv = "hdfs://master:9000/user/root/csvfiles/质检清单.csv"
//    val satisfactionCsvRead:RDD[Array[String]] = sparkSession.sparkContext.hadoopFile(satisfactionCsv,classOf[TextInputFormat]
//      ,classOf[LongWritable],classOf[Text]).map(pair=>new String(pair._2.getBytes,0,pair._2.getLength,"GBK"))
//      .map(x=>x.split(","))
//
//    val satisfactionRDD:RDD[Satisfaction] = satisfactionCsvRead.map(x=>Satisfaction(x(0),x(14),x(15)))
//    val complaintCsv = "hdfs://master:9000/user/root/csvfiles/云网支撑(220511).csv"
//    val complaintCsvRead:RDD[Array[String]] = sparkSession.sparkContext.hadoopFile(complaintCsv,classOf[TextInputFormat]
//      ,classOf[LongWritable],classOf[Text]).map(pair=>new String(pair._2.getBytes,0,pair._2.getLength,"GBK"))
//      .map(x=>x.split(","))
//    val complaintRDD:RDD[Complaint] = complaintCsvRead.map(x=>Complaint(x(0),x(6),x(10)))
//    var tuple= (satisfactionRDD,complaintRDD)
//    tuple
//  }


}
