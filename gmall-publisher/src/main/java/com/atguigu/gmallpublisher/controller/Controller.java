package com.atguigu.gmallpublisher.controller;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@RestController
public class Controller {
    @Autowired
    private PublisherService publisherService;

    @RequestMapping("realtime-total")
    public String getDauTotal(@RequestParam("date") String date){

        //1.获取日活总数数据
        Integer dauTotal = publisherService.getDauTotal(date);

        //2.创建List集合用来存放结果数据
        ArrayList<Map> result = new ArrayList<>();

        //3.创建存放新增日活的map集合
        HashMap<String, Object> dauMap = new HashMap<>();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", dauTotal);

        //4.创建存放新增设备的map集合
        HashMap<String, Object> devMap = new HashMap<>();
        devMap.put("id", "new_mid");
        devMap.put("name","新增设备");
        devMap.put("value",233);

        result.add(dauMap);
        result.add(devMap);

        return JSONObject.toJSONString(result);
    }

    @RequestMapping("realtime-hours")
    public String getDauHoutTotal(@RequestParam("id") String id, @RequestParam("date") String date) {
        //1.获取经过Service层处理过后的数据
        Map todayMap = publisherService.getDauHourTotal(date);

        //2.根据传入的date获取前一天的date
        String yesterday = LocalDate.parse(date).plusDays(-1).toString();

        //3.再根据前一天的日期获取到前一天的分时数据
        Map yesterdayMap = publisherService.getDauHourTotal(yesterday);

        //4.创建存放最终结果的map集合
        HashMap<String, Map> result = new HashMap<>();

        result.put("today", todayMap);
        result.put("yesterday", yesterdayMap);

        return JSONObject.toJSONString(result);
    }
}
