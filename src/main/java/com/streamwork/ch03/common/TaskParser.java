package com.streamwork.ch03.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamwork.ch03.api.Component;
import com.streamwork.ch03.api.Source;
import com.streamwork.ch03.api.Operator;
import com.streamwork.ch03.job.ContinuousVehicleSource;
import com.streamwork.ch03.api.DistributedOperator;
import com.streamwork.ch03.func.ApplyFunc;
import com.streamwork.ch03.common.Task;

import java.util.List;

public class TaskParser {

    // 解析 taskjson 并创建 Task 实例
    public static Task parseTaskJson(String taskJson) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode rootNode = mapper.readTree(taskJson);

        // 获取 task 的基本信息
        Integer id = rootNode.get("id").asInt();
        String taskType = rootNode.get("taskType").asText();
        Integer parallelism = rootNode.get("parallelism").asInt();
        String ipaddress = rootNode.get("ipaddress").asText();

        // 获取 component 信息
        JsonNode componentNode = rootNode.get("component");
        String componentName = componentNode.get("name").asText();

        // 动态加载 component 实例
        Component component = createComponent(componentName, componentNode);

        // 根据 taskType 创建 Task 实例
        Task task = new Task(id, taskType, parallelism, ipaddress, component);

        return task;
    }

    // 根据 componentName 和 componentNode 创建对应的组件实例
    public static Component createComponent(String componentName, JsonNode componentNode) throws Exception {
        // 判断 component 的类型并创建对应的实例
        switch (componentName) {
            case "ContinuousVehicleSource":
                return createSourceComponent(componentNode);
            case "VehicleOperator":
                return createOperatorComponent(componentNode);
            default:
                throw new Exception("Unknown component name: " + componentName);
        }
    }

    // 根据 componentNode 创建 ContinuousVehicleSource
    public static Source createSourceComponent(JsonNode componentNode) {
        int parallelism = componentNode.get("parallelism").asInt();
        int interval = componentNode.get("interval").asInt();
        // 其他属性可以继续从 componentNode 中提取
        return new ContinuousVehicleSource("ContinuousVehicleSource", parallelism, interval);
    }

    // 根据 componentNode 创建 Operator
    public static Operator createOperatorComponent(JsonNode componentNode) throws Exception {
        // 获取 ApplyFunc 类名并动态加载类
        String applyFuncClassName = componentNode.get("applyFuncClassName").asText();
        Class<?> clazz = Class.forName(applyFuncClassName);

        if (!ApplyFunc.class.isAssignableFrom(clazz)) {
            throw new Exception("Class " + applyFuncClassName + " does not implement ApplyFunc interface");
        }

        ApplyFunc applyFunc = (ApplyFunc) clazz.getDeclaredConstructor().newInstance();

        return new DistributedOperator("VehicleOperator", componentNode.get("parallelism").asInt(), applyFunc);
    }
}
