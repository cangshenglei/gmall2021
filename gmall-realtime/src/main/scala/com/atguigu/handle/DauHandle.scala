package com.atguigu.handle

import java.sql.Date
import java.text.SimpleDateFormat
import java.{lang, util}

import com.atguigu.bean.StartUpLog
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DauHandle {
  /**
  * 批次内去重
  * */

  def filterByGroup(filterByRedisDStream: DStream[StartUpLog])= {
    //1,将数据转为kv类型,为了方便使用groupBykey ,k:logdate+mid  v :startuplog
    val midWithLogDataToLogDStream: DStream[((String,String),StartUpLog)] = filterByRedisDStream.map(log => {
      ((log.mid,log.logDate),log)
    })
    //2,对相同日期的mid的数据聚合到一起
    val midWithLogDataToIterLogDStream: DStream[((String, String), Iterable[StartUpLog])] = midWithLogDataToLogDStream.groupByKey()
    //3,将迭代器转为lisy集合,然后按照时间戳由小到大排序,取第一条
    val midWithLogDataToListLogDStream :DStream[((String,String),List[StartUpLog])] = midWithLogDataToIterLogDStream.mapValues(iter => {
    iter.toList.sortWith(_.ts<_.ts).take(1)
    })
    //4,使用flatMap算子打散list集合中的数据
    val value: DStream[StartUpLog] = midWithLogDataToListLogDStream.flatMap(_._2)
   value
  }


  /**
    * 批次间去重
    * startUpLogDStream
    * */
  def filterByRedis(startUpLogDStream: DStream[StartUpLog], sc: SparkContext) = {
    //使用filter算子将当前mid与redis中已经保存的mid做对比,如有重复的,则过滤到当前的数据
    /*val value :DStream[StartUpLog]= startUpLogDStream.filter(log =>{
      //1,创建redis连接
      val jedis: Jedis = new Jedis("hadoop102",6379)
      //2,查询redis中mid是否与当前的mid重复
      val redisKey:String ="Dau"+log.logDate
      //使用sismember方法判断一个值是否在redisset类型中,在的话返回true,不在的话返回false
      val boolean: lang.Boolean = jedis.sismember(redisKey,log.mid)
      jedis.close()
      //在的话取反,为false的时候则滤过该数据
      !boolean
    })
    value*/

    //优化二:在每个分区下获取连接
    /*val value: DStream[StartUpLog] = startUpLogDStream.mapPartitions(partition => {
      //1.在分区下创建redis连接，减少连接个数
    val jedis: Jedis = new Jedis("hadoop102",6379)
      val logs : Iterator[StartUpLog] = partition.filter(log => {
        //2,查询redis中mid是否与当前的mid重复
        val redisKey :String = "Dau:" + log.logDate
        //3,使用sismember方法判断一个值是否在redisset类型中,在的话返回true,不在的话返回false
        val boolean: lang.Boolean = jedis.sismember(redisKey, log.mid)
        ! boolean
      })
      jedis.close()
    }*/


    //优化三:在每个批次下获取连接以减少连接次数

    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val value : DStream[StartUpLog] = startUpLogDStream.transform(rdd => {
      //1,在每个批次下创建redis连接,此时连接是在Driver端连接
      val jedis: Jedis = new Jedis("hadoop102",6379)
      //2,在driver端查讯到redis中保存的数据
      val time: String = sdf.format(new Date(System.currentTimeMillis()))
      val redisKey :String = "Dau" + time
      val mids : util.Set[String] = jedis.smembers(redisKey)

      //3,将查询出来的set集合广播到executor端
      val midsBc : Broadcast[util.Set[String]] = sc.broadcast(mids)

      //4,遍历每个rdd中的每条数据去重
      val logs :RDD[StartUpLog] = rdd.filter(log => {
        //取到广播来的数据,然后进行对比
        val bool : Boolean = midsBc.value.contains(log.mid)
        !bool
      })
      jedis.close()
      logs
    })
    value
  }

  def saveToRedis(startUpLogDStream:DStream[StartUpLog] ) ={
    startUpLogDStream.foreachRDD(rdd => {
      //foreachRdd是运行在Driver端
      rdd.foreachPartition(partition => {
        //foreachPartition是运行在executor端
        //在风区下创建redis连接 ,1 可以减少连接个数 2,可以避免序列化错误
        val jedis : Jedis= new Jedis("hadoop102",6379)
        partition.foreach(log => {
          //将mid写入redis
          val redisKey:String = "Dau:" + log.logDate
          jedis.sadd(redisKey,log.mid)
        })
        //关闭redis连接
        jedis.close()
      })
    })
  }

}
