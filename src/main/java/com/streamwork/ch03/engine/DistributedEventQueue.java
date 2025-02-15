package com.streamwork.ch03.engine;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.streamwork.ch03.rpc.io.RpcNode;
import com.streamwork.ch03.api.Event;

public class DistributedEventQueue extends RpcNode implements CommonEventQueue {
    String address;
    int port;
    int id;

    public DistributedEventQueue(String address, int port, int id) {
        this.address = address;
        this.port = port;
        this.id = id;
    }

    @Override
    public void put(Event e) throws Exception {
        String EventString = JSON.toJSONString(e, SerializerFeature.WriteClassName);
        Object r = call(address, port, "receiveEvent", new Object[]{EventString, id});
//        return JSON.parseObject(r.toString(), int.class);
    }
}
