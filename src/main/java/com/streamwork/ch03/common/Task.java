package com.streamwork.ch03.common;

import com.alibaba.fastjson.annotation.JSONField;
import com.streamwork.ch03.api.Component;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor

public class Task {
    private Integer id;
    private String taskType; // source or operator
    private Integer parallelism;
    private String ipaddress;
//    @JSONField(name = "componentType")
//    private String componentType;
    private Component component;

    public String toString() {
        return "Task{" +
                "id=" + id +
                ", taskType='" + taskType + '\'' +
                ", parallelism=" + parallelism +
                ", ipaddress='" + ipaddress + '\'' +
                ", component=" + (component != null ? component.getClass().getSimpleName() : "null") +
                '}';
    }
}
