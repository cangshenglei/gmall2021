package com.atguigu.bean

case class StartUpLog (mid:String,
                       uid:String,
                       appid:String,
                       area:String,
                       os:String,
                       ch:String,
                       `type`:String,
                       vs:String,
                       var logDate:String,//精确到天的日期
                       var logHour:String,//精确到小时
                       var ts:Long)
