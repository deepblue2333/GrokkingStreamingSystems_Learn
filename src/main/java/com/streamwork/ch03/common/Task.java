package com.streamwork.ch03.common;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamwork.ch03.api.Component;
import com.streamwork.ch03.api.Event;
import com.streamwork.ch03.api.Operator;
import com.streamwork.ch03.api.Source;
import com.streamwork.ch03.job.VehicleEvent;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

@Data

@NoArgsConstructor
public class Task {
    private Integer id;
    private String taskType; // source or operator
    private Integer parallelism;
    private String ipaddress;
    private Component component;

    // 构造函数
    public Task(Integer id, String taskType, Integer parallelism, String ipaddress, Component component) {
        this.id = id;
        this.taskType = taskType;
        this.parallelism = parallelism;
        this.ipaddress = ipaddress;
        this.component = component;
    }

    // 获取任务 ID
    public Integer getId() {
        return id;
    }

    // 获取任务类型
    public String getTaskType() {
        return taskType;
    }

    // 获取并执行任务
    public void executeTask(String taskjson) {
        if ("source".equalsIgnoreCase(taskType)) {
            // 如果是 Source 类型任务，调用 Source 的方法
            if (component instanceof Source) {
                Source source = (Source) component;
                // 生成事件并处理
                List<Event> eventCollector = new ArrayList<>();
                source.getEvents(eventCollector);
                eventCollector.forEach(event -> System.out.println("Generated event: " + event));
            }
        } else if ("operator".equalsIgnoreCase(taskType)) {
            // 如果是 Operator 类型任务，调用 Operator 的方法
            if (component instanceof Operator) {
                Operator operator = (Operator) component;
                // 处理事件
                Event event = new VehicleEvent(component.getName());
                List<Event> eventCollector = new ArrayList<>();
                System.out.println("Event before applying operator: " + event);
                operator.apply(event, eventCollector);
                System.out.println("Event after applying operator: " + eventCollector);
                operator.apply(event, eventCollector);
            }
        } else {
            System.out.println("Unknown task type: " + taskType);
        }
    }
    public String toJson() {
        return JSON.toJSONString(this);
    }
}

