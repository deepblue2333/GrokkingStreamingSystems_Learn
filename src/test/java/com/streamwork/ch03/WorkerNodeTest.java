package com.streamwork.ch03;

import com.streamwork.ch03.api.Operator;
import com.streamwork.ch03.api.Source;
import com.streamwork.ch03.common.Task;
import com.streamwork.ch03.engine.WorkerStarter;
import com.streamwork.ch03.job.ContinuousVehicleSource;
import com.streamwork.ch03.job.VehicleMapper;

public class WorkerNodeTest {
    public static void main(String[] args) throws Exception {
        WorkerStarter workerStarter = TestSource();
        TestOperator(workerStarter);

        TestSourceStart(workerStarter);
    }

    private static WorkerStarter TestSource() throws Exception {
        Source source = new ContinuousVehicleSource("Continuous-reader", 2, 100);
        Task t = new Task(0, "Source", 2, "127.0.0.1", source);
        WorkerStarter workNode = new WorkerStarter();
        workNode.setupSource(t);
        workNode.start(9991);
        return workNode;
    }

    private static void TestSourceStart(WorkerStarter workNode) {
        workNode.startSource(0);
    }

    private static void TestOperator(WorkerStarter workNode) throws Exception {
        Operator operator = new VehicleMapper("vehicle-Mapper", 2);
        Task t = new Task(1, "Operator", 2, "127.0.0.1", operator);
        workNode.startExecutor(t);
    }
}
