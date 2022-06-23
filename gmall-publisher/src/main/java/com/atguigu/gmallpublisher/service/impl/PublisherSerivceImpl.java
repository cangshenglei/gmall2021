package com.atguigu.gmallpublisher.service.impl;

import com.atguigu.gmallpublisher.mapper.DauMapper;
import com.atguigu.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherSerivceImpl implements PublisherService {

    @Autowired
    private DauMapper dauMapper;

    @Override
    public Integer getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauHourTotal(String date) {
        //1.获取DAO（Mapper） 层的数据
        List<Map> list = dauMapper.selectDauTotalHourMap(date);

        //2.创建Map集合用来存放返回的数据 k：老map中LH对应的值 v：老map中CT对应的值
        HashMap<String, Long> result = new HashMap<>();

        //3.遍历list集合获取到老map然后封装新的map
        for (Map map : list) {
            result.put((String) map.get("LH"), (Long) map.get("CT"));
        }
        return result;
    }
}
