package com.streamwork.ch03;

import com.streamwork.ch03.api.Event;
import com.streamwork.ch03.api.Source;
import com.streamwork.ch03.api.Operator;
import com.streamwork.ch03.engine.WorkerStarter;
import com.streamwork.ch03.job.ContinuousVehicleSource;
import com.streamwork.ch03.api.DistributedOperator;
import com.streamwork.ch03.func.ApplyFunc;
import com.streamwork.ch03.common.Task;
import org.junit.Before;
import org.junit.Test;
import com.alibaba.fastjson.JSON;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Arrays;
import java.util.List;

public class WorkerStarterTest {

    private WorkerStarter workerStarter;

    @Before
    public void setUp() throws Exception {
        // 初始化 WorkerStarter 实例
        workerStarter = new WorkerStarter();
    }

    @Test
    public void testWorkerStarterWithSourceAndOperator() {
        // 创建一个 Source 实例
        Source continuousVehicleSource = new ContinuousVehicleSource("ContinuousVehicleSource", 2, 1000);

        // 创建一个 ApplyFunc 实例作为 Operator
        ApplyFunc applyFunc = new ApplyFunc() {
            @Override
            public void apply(Event event, List<Event> eventCollector) {
                // 事件处理逻辑
                System.out.println("Applying function on event: " + event);
            }
        };

        // 创建一个 DistributedOperator 实例作为算子
        Operator vehicleOperator = new DistributedOperator("VehicleOperator", 2, applyFunc);

        // 创建 Source 任务
        Task sourceTask = new Task(1, "source", 2, "192.168.0.1", continuousVehicleSource);

        // 创建 Operator 任务
        Task operatorTask = new Task(2, "operator", 2, "192.168.0.2", vehicleOperator);

        // 模拟 WorkerStarter 的任务接收与执行
        System.out.println("Simulating WorkerStarter with Source Task");

        // 模拟工作节点请求任务并处理
        workerStarter.requireTask();

        // 模拟任务信息通知
        workerStarter.informRequireTask(sourceTask);

        // 模拟任务的调度与执行
        workerStarter.setupSource(sourceTask);
        workerStarter.startSource(sourceTask.getId());

        // 验证节点信息是否成功获取
        String address = workerStarter.askNextNodeAddress(sourceTask.getId());
        int port = workerStarter.askNextNodePort(sourceTask.getId());
        int nodeId = workerStarter.askNextNodeId(sourceTask.getId());

        System.out.println("Next Node Address: " + address);
        System.out.println("Next Node Port: " + port);
        System.out.println("Next Node ID: " + nodeId);

        // 验证 Operator 任务执行
        System.out.println("Simulating WorkerStarter with Operator Task");
        workerStarter.startExecutor(operatorTask);
        workerStarter.informRequireTask(operatorTask);  // 模拟通知 Operator 任务

        // 执行操作符任务
        workerStarter.startExecutor(operatorTask);
    }

}
