package com.atom.producer.controller;

import com.atom.producer.infrastructure.msg.SpringMsgProducer;
import com.atom.producer.infrastructure.stream.StreamMsgProducer;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.Map;

/**
 * producer
 * <p>
 * Created by Atom on 2017/5/23.
 */
@Controller
@RequestMapping("/producer")
public class ProducterController {

    @Resource
    private SpringMsgProducer msgProducer;

    @Resource
    private StreamMsgProducer streamMsgProducer;

    @ResponseBody
    @RequestMapping("/sendMsg")
    public Map<String, Object> product() {
        Map<String, Object> map = new HashMap<>();
        msgProducer.sendMsg();
        map.put("result", true);
        return map;
    }

    @ResponseBody
    @RequestMapping("/sendStreamMsg")
    public Map<String, Object> productStreamMsg() {
        Map<String, Object> map = new HashMap<>();
        streamMsgProducer.sendMsg();
        map.put("result", true);
        return map;
    }
}
