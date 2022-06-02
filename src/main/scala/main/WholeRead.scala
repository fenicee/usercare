package main

import model.{EconWholeData}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.util

object WholeRead {
  def readCSV(sparkSession:SparkSession,inputfile:String): RDD[EconWholeData] ={
    val csvread:RDD[Array[String]] = sparkSession.sparkContext.hadoopFile(inputfile,classOf[TextInputFormat]
      ,classOf[LongWritable],classOf[Text]).map(pair=>new String(pair._2.getBytes,0,pair._2.getLength,"GBK"))
      .map(x=>x.split(","))
    val csvreadRDD:RDD[EconWholeData] = csvread.map(x=>EconWholeData(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12)
      ,x(13),x(14),x(15),x(16),x(17),x(18),x(19),x(20),x(21),x(22),x(23),x(24),x(25),x(26),x(27),x(28),x(29),x(30),x(35),x(36),x(37)
      ,x(38),x(39),x(40),x(41),x(42),x(43),x(44),x(45),x(46),x(47),x(48),x(49),x(50),x(51),x(52),x(53),x(54)
      ,x(55),x(56),x(57),x(58),x(81),x(82),x(83)))
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
        //订购ID	订单受理时间	订单生效时间	订单到期时间	订单状态	移动云口令_帐号状态	移动云口令_帐号生效时间
        // 移动云口令_帐号失效时间	资源池ID	资源池名称	产品规格	物理核数	单价_元
        // LICENSE数	地市	区县	集团客户名称	总部集团编号	省内集团编号	看管人员姓名
        // 看管人员工号	看管人手机号码	归属网格名称	微格名称	当月预估收入_元	集团行业大类
        // 集团行业小类
        val put = new Put(Bytes.toBytes(reverseOrderID(record.订购ID)))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("订购ID"), Bytes.toBytes(record.订购ID))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("订单受理时间"), Bytes.toBytes(record.订单受理时间))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("订单生效时间"), Bytes.toBytes(record.订单生效时间))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("订单到期时间"), Bytes.toBytes(record.订单到期时间))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("订单状态"), Bytes.toBytes(record.订单状态))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("移动云口令_帐号状态"), Bytes.toBytes(record.移动云口令_帐号状态))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("移动云口令_帐号生效时间"), Bytes.toBytes(record.移动云口令_帐号生效时间))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("移动云口令_帐号失效时间"), Bytes.toBytes(record.移动云口令_帐号失效时间))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("资源池ID"), Bytes.toBytes(record.资源池ID))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("资源池名称"), Bytes.toBytes(record.资源池名称))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("产品规格"), Bytes.toBytes(record.产品规格))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("物理核数"), Bytes.toBytes(record.物理核数))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("单价_元"), Bytes.toBytes(record.单价_元))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("LICENSE数"), Bytes.toBytes(record.LICENSE数))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("地市"), Bytes.toBytes(record.地市))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("区县"), Bytes.toBytes(record.区县))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("集团客户名称"), Bytes.toBytes(record.集团客户名称))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("总部集团编号"), Bytes.toBytes(record.总部集团编号))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("省内集团编号"), Bytes.toBytes(record.省内集团编号))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("看管人员姓名"), Bytes.toBytes(record.看管人员姓名))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("看管人员工号"), Bytes.toBytes(record.看管人员工号))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("看管人手机号码"), Bytes.toBytes(record.看管人手机号码))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("归属网格名称"), Bytes.toBytes(record.归属网格名称))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("微格名称"), Bytes.toBytes(record.微格名称))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("当月预估收入_元"), Bytes.toBytes(record.当月预估收入_元))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("集团行业大类"), Bytes.toBytes(record.集团行业大类))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("集团行业小类"), Bytes.toBytes(record.集团行业小类))
        //集团行业类型	集团价值等级	建档时间	集团状态
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("集团行业类型"), Bytes.toBytes(record.集团行业类型))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("集团价值等级"), Bytes.toBytes(record.集团价值等级))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("建档时间"), Bytes.toBytes(record.建档时间))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("集团状态"), Bytes.toBytes(record.集团状态))
        //客户经理姓名	客户经理电话	客户经理ID	受理状态	省份代码	客户最后一次修改时间
        // 基础口令支付方式	是否测试客户	产品编码	产品名称	EBOSS产品三级编码	EBOSS产品三级名称
        // EBOSS产品二级编码	EBOSS产品二级名称	EBOSS产品一级编码	EBOSS产品一级名称	EBOSS产品零级编码
        // EBOSS产品零级名称	资费计划编码	资费计划名称	OP侧订单号	OP明细的唯一编号	OP侧订单状态	资源ID
        //折扣	记录状态	数据来源
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("客户经理姓名"), Bytes.toBytes(record.客户经理姓名))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("客户经理电话"), Bytes.toBytes(record.客户经理电话))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("客户经理ID"), Bytes.toBytes(record.客户经理ID))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("受理状态"), Bytes.toBytes(record.受理状态))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("省份代码"), Bytes.toBytes(record.省份代码))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("客户最后一次修改时间"), Bytes.toBytes(record.客户最后一次修改时间))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("基础口令支付方式"), Bytes.toBytes(record.基础口令支付方式))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("是否测试客户"), Bytes.toBytes(record.是否测试客户))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("产品编码"), Bytes.toBytes(record.产品编码))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("产品名称"), Bytes.toBytes(record.产品名称))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("EBOSS产品三级编码"), Bytes.toBytes(record.EBOSS产品三级编码))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("EBOSS产品三级名称"), Bytes.toBytes(record.EBOSS产品三级名称))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("EBOSS产品二级编码"), Bytes.toBytes(record.EBOSS产品二级编码))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("EBOSS产品二级名称"), Bytes.toBytes(record.EBOSS产品二级名称))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("EBOSS产品一级编码"), Bytes.toBytes(record.EBOSS产品一级编码))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("EBOSS产品一级名称"), Bytes.toBytes(record.EBOSS产品一级名称))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("EBOSS产品零级编码"), Bytes.toBytes(record.EBOSS产品零级编码))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("EBOSS产品零级名称"), Bytes.toBytes(record.EBOSS产品零级名称))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("资费计划编码"), Bytes.toBytes(record.资费计划编码))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("资费计划名称"), Bytes.toBytes(record.资费计划名称))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("OP侧订单号"), Bytes.toBytes(record.OP侧订单号))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("OP明细的唯一编号"), Bytes.toBytes(record.OP明细的唯一编号))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("资源ID"), Bytes.toBytes(record.资源ID))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("折扣"), Bytes.toBytes(record.折扣))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("记录状态"), Bytes.toBytes(record.记录状态))
        put.addColumn(Bytes.toBytes("C2"), Bytes.toBytes("数据来源"), Bytes.toBytes(record.数据来源))





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
