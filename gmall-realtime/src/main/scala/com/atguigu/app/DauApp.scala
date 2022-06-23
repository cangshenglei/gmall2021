package com.atguigu.app

import java.sql.Date
import java.text.SimpleDateFormat



import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constants.GmallConstants
import com.atguigu.handle.DauHandle
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

object DauApp {
  def main(args: Array[String]): Unit = {
    //1,创建SparkConf
    val sparkConf :SparkConf= new SparkConf().setAppName("DauApp").setMaster("local[*]")
    //2,创建StreamingContext
    val ssc :StreamingContext= new StreamingContext(sparkConf,Seconds(3))
    //3,通过Kafka工具类消费启动日志
    val kafkaDStream :InputDStream[ConsumerRecord[String,String]]= MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP,ssc)
    //4,将消费到的json字符串转为样例类,为了方便操作,并补全logdate&loghour这两个字段
    val sdf :SimpleDateFormat= new SimpleDateFormat("yyyy-MM-dd HH")
    val startUpLogDStream:DStream[StartUpLog] =kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        //将json字符串转为样例类
        val startUpLog :StartUpLog= JSON.parseObject(record.value(),classOf[StartUpLog])
        //补全两个字段
        val times:String = sdf.format(new Date(startUpLog.ts))
        startUpLog.logDate = times.split(" ")(0)
        startUpLog.logHour = times.split(" ")(1)
        startUpLog
      })
    }
    )
    //缓存数据
    startUpLogDStream.cache()
    //打印原始数据条数
    startUpLogDStream.count().print()
    //5,对相同日期相同mid的数据做批次间去重
    val filterByRedisDStream  :DStream[StartUpLog]= DauHandle.filterByRedis(startUpLogDStream,ssc.sparkContext)

    //缓存数据
    filterByRedisDStream.cache()
    //打印经过批次间去重后的数据条数
    filterByRedisDStream.count().print()
    //6,再对经过批次间去重后的数据做批次内去重
    val filterByGroupDStream: DStream[StartUpLog] = DauHandle.filterByGroup(filterByRedisDStream)
    filterByGroupDStream.cache()
    filterByGroupDStream.count().print()

    //7,将最终去重后的mid写入redis,为了方便批次去重
    DauHandle.saveToRedis(filterByRedisDStream)
    //8,将最终去重后的明细数据写入hbase
    filterByGroupDStream.foreachRDD( rdd => {
      rdd.saveToPhoenix("GMALL220212_DAU",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        HBaseConfiguration.create,
        Some("hadoop102,hadoop103,hadoop104:2181")
      )
    })
    //打印测试
      startUpLogDStream.print()
    //启动并阻塞
    ssc.start()
    ssc.awaitTermination()
  }
}
