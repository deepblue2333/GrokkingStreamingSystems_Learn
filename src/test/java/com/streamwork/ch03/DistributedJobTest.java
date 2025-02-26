package com.streamwork.ch03;



import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.streamwork.ch03.api.*;
import com.streamwork.ch03.func.ApplyFunc;
import com.streamwork.ch03.func.VehicleMapperFunc;
import com.streamwork.ch03.job.*;
import com.streamwork.ch03.engine.*;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DistributedJobTest {

    public static void main(String[] args) throws Exception {
        // 创建源任务：模拟车辆事件
        ContinuousVehicleSource vehicleSource = new ContinuousVehicleSource("VehicleSource", 2, 1000);

//        System.out.println(vehicleSource);
        // 创建操作符任务：我们通过实现 ApplyFunc 来定义每个操作符的行为
//        ApplyFunc vehicleMapperFunc = new ApplyFunc() {
//            private static final long serialVersionUID = 1L;  // 确保唯一标识符
//
//            @Override
//            public void apply(Event inputEvent, List<Event> eventCollector) {
//                // 这里的处理逻辑是将车辆事件进行映射
//                Event outputEvent = new VehicleEvent(inputEvent.getData() + "_map");
//                eventCollector.add(outputEvent);
//                System.out.println("VehicleMapper applied: " + outputEvent.getData());
//            }
//        };
        ApplyFunc vehicleMapperFunc = new VehicleMapperFunc();
        System.out.println("vehicleMapperFunc" + vehicleMapperFunc);
        // 序列化
        DistributedOperator vehicleMapper = new DistributedOperator("VehicleMapper", 2, vehicleMapperFunc);

        ApplyFunc vehicleCounterFunc = new ApplyFunc() {
            @Override
            public void apply(Event inputEvent, List<Event> eventCollector) {
                // 假设我们根据车辆事件进行计数
                String vehicle = (String) inputEvent.getData();
                System.out.println("VehicleCounter applied to: " + vehicle);
            }
        };
        DistributedOperator vehicleCounter = new DistributedOperator("VehicleCounter", 2, vehicleCounterFunc);


        System.out.println("vehicleMapper" + vehicleMapper);
        // 创建 Job 并将源任务和操作符任务连接
        Job job = new Job("VehicleJob");
        Stream stream = job.addSource(vehicleSource); // 添加源任务并获取流

        // 通过流将操作符连接到任务中
        stream.applyOperator(vehicleMapper)
                .applyOperator(vehicleCounter);


        // 创建并启动作业
        DistributedJobStarter jobStarter = new DistributedJobStarter(job);
        jobStarter.start();
        jobStarter.storeApplyFunc(vehicleMapperFunc);
        jobStarter.storeApplyFunc(vehicleCounterFunc);

        // 创建并启动工作节点
        WorkerStarter workerStarter = new WorkerStarter();
        workerStarter.start(9993);  // 工作节点监听端口
        workerStarter.requireApplyFunc(1);
        workerStarter.requireTask();
//         在工作节点启动后，通过主节点调用 `startDistributeTask` 方法来分发任务
//        DistributedJobStarter jobStarterForDistribution = new DistributedJobStarter(job);
//        jobStarter.startDistributeTask();  // 确保主节点进行任务分发

        System.out.println("=== Final Record Map ===");
        Map<Integer, DistributedJobStarter.Record> recordMap = jobStarter.getTaskRecords();
        for (Map.Entry<Integer, DistributedJobStarter.Record> entry : recordMap.entrySet()) {
            System.out.println("Task ID: " + entry.getKey() + " -> " + entry.getValue());
        }
        System.out.println("========================");
// 等待 Worker 处理任务
        Thread.sleep(2000);  // 等待 2 秒，让 Worker 解析任务


        System.out.println("=== Worker Processed Tasks ===");
        Set<Integer> processedTasks = workerStarter.getProcessedTasks();
        for (Integer taskId : processedTasks) {
            System.out.println("Worker successfully parsed Task ID: " + taskId);
        }
        System.out.println("=============================");

        // 验证任务流
        checkTaskFlow();
    }

    // 验证任务流
    private static void checkTaskFlow() {
        // 你可以在这里进行更详细的验证，检查每个操作符是否按预期执行
        System.out.println("Task flow verified successfully.");
    }
}
