package com.streamwork.ch03;

import com.streamwork.ch03.api.Event;
import com.streamwork.ch03.api.Source;
import com.streamwork.ch03.api.Operator;
import com.streamwork.ch03.job.ContinuousVehicleSource;
import com.streamwork.ch03.api.DistributedOperator;
import com.streamwork.ch03.func.ApplyFunc;
import com.streamwork.ch03.common.Task;
import com.streamwork.ch03.job.VehicleEvent;
import org.junit.Test;

import java.util.List;

public class TestTask {

    @Test
    public void testExistingOperators() {
        // 直接使用已有的算子和源组件

        // 创建一个Source实例
        Source continuousVehicleSource = new ContinuousVehicleSource("ContinuousVehicleSource", 2, 1000);

        // 创建一个ApplyFunc实例作为Operator
        ApplyFunc applyFunc = new ApplyFunc() {
            @Override
            public void apply(Event event, List<Event> eventCollector) {
                // 事件处理逻辑
                System.out.println("Applying function on event: " + event);
            }
        };

        // 创建一个DistributedOperator实例作为算子
        Operator vehicleOperator = new DistributedOperator("VehicleOperator", 2, applyFunc);

        // 创建Source任务
        Task sourceTask = new Task(1, "source", 2, "192.168.0.1", continuousVehicleSource);

        // 创建Operator任务
        Task operatorTask = new Task(2, "operator", 2, "192.168.0.2", vehicleOperator);

        // 执行Source任务
        System.out.println(sourceTask);
        sourceTask.executeTask(sourceTask.toJson());

        // 执行Operator任务
        System.out.println(operatorTask);
        operatorTask.executeTask(operatorTask.toJson());
    }
}
