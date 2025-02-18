package com.streamwork.ch03.engine;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.streamwork.ch03.api.*;
import com.streamwork.ch03.common.Task;
import com.streamwork.ch03.func.ApplyFunc;
import com.streamwork.ch03.job.ContinuousVehicleSource;
import com.streamwork.ch03.rpc.io.RpcNode;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.Executor;

public class WorkerStarter extends RpcNode {
    private final Set<Integer> processedTasks = new HashSet<>();
    // 设置队列容量
    private final static int QUEUE_SIZE = 64;
    // The job to start
    // List of executors and stream managers执行器队列和流管理器队列
//    private final List<ComponentExecutor> executorList = new ArrayList<ComponentExecutor>();
//    private final List<EventDispatcher> dispatcherList = new ArrayList<EventDispatcher>();
//    // Connections between component executors 组件间的连接器
//    private final List<Connection> connectionList = new ArrayList<Connection>();

    // 工作节点运行的执行器
    public static class MyExecutor {
        int id; // 执行器实例id
        int parallelism; // 执行器实例数
        EventDispatcher dispatcher; // 流管理器
        ComponentExecutor executor; // 执行器

    }

    // 执行器索引
    public final HashMap<Integer, MyExecutor> executorMap = new HashMap<Integer, MyExecutor>();

    public WorkerStarter() {
    }


    public void start(int port) throws Exception {
        setPort(port);
        serve();
        System.out.println("success worker");
//        requireTask();
    }

    /* 被调用以获取算子任务 */
    public synchronized void requireTask() {
        System.out.println("WorkerStarter: Requiring task...");

        Task t = JSON.parseObject(
                call("127.0.0.1", 9992, "requireTask", new Object[]{}).toString(),
                Task.class);
        // 模拟一个任务数据
//        Task t = new Task(1, "Source", 2, "192.168.0.1", new ContinuousVehicleSource("ContinuousVehicleSource", 2, 1000));

        if (t.getTaskType().equals("Source")) {
            setupSource(t);
            System.out.println("SUCCESS");

        } else {
            startExecutor(t);
        }
//        informRequireTask(t);
        processedTasks.add(t.getId());
    }

    // 获取已处理的任务
    public Set<Integer> getProcessedTasks() {
        return processedTasks;
    }

    public void setupSource(Task t) {

        SourceExecutor executor = new SourceExecutor((Source) t.getComponent());
        System.out.println(t.getId());
//        EventQueue downstream = new EventQueue(QUEUE_SIZE);
        String address = askNextNodeAddress(t.getId());
        System.out.println(t.getId());
        int port = askNextNodePort(t.getId());
        int id = askNextNodeId(t.getId());

        DistributedEventQueue downstream = new DistributedEventQueue(address, port, id);

        executor.setOutgoingQueue(downstream);
        executor.start();


        MyExecutor e = new MyExecutor();
        e.executor = executor;
        e.id = t.getId();
        e.parallelism = t.getParallelism();
        executorMap.put(e.id, e);


    }

    public int askNextNodeId(Integer id) {
        int nextId = JSON.parseObject(
                call("127.0.0.1", 9992, "askNextNodeId", new Object[]{id}).toString(),
                int.class);
        return nextId;
//          return 1;
    }

    public int askNextNodePort(Integer id) {
        int port = JSON.parseObject(
                call("127.0.0.1", 9992, "askNextNodePort", new Object[]{id}).toString(),
                int.class);
        return port;
//        return  8000;
    }

    public String askNextNodeAddress(Integer id) {
        String address = JSON.parseObject(
            call("127.0.0.1", 9992, "askNextNodeAddress", new Object[]{id}).toString(),
            String.class);
        return address;
//        return "192.168.88.1";
    }

    public void startSource(int id) {
        MyExecutor mySourceExecutor = executorMap.get(id);
        mySourceExecutor.executor.start();
    }

    public void startExecutor(Task t) {
        OperatorExecutor operatorExecutor = new OperatorExecutor((Operator) t.getComponent());
        EventDispatcher dispatcher = new EventDispatcher(operatorExecutor);
        // 绑定上游输入队列
        EventQueue upstream = new EventQueue(QUEUE_SIZE);
        dispatcher.setIncomingQueue(upstream);
        // Connect to downstream (to each instance).
        int parallelism = t.getParallelism();
        EventQueue [] downstream = new EventQueue[parallelism];
        for (int i = 0; i < parallelism; ++i) {
            downstream[i] = new EventQueue(QUEUE_SIZE);
        }

        EventQueue outcomingQueue = new EventQueue(QUEUE_SIZE);
        operatorExecutor.setOutgoingQueue(outcomingQueue);

        operatorExecutor.setIncomingQueues(downstream);
        dispatcher.setOutgoingQueues(downstream);
        operatorExecutor.start();
        dispatcher.start();

        MyExecutor e = new MyExecutor();
        e.executor = operatorExecutor;
        e.id = t.getId();
        e.dispatcher = dispatcher;
        e.parallelism = t.getParallelism();
        executorMap.put(e.id, e);
    }

    public synchronized int receiveEvent(String eventString, Integer id) {
        Event event = JSON.parseObject(eventString, Event.class);
        MyExecutor myExecutor = executorMap.get(id);
        if (myExecutor == null) {
            return -1;
        }
        myExecutor.dispatcher.incomingQueue.add(event);
        return 1;
    }
    public Object call(String ipAddress, int port, String method, Object[] params) {
        try {
            // 与 Netcat 服务建立连接
            Socket socket = new Socket(ipAddress, port);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            // 构造请求内容
//            String request = method + " " + Arrays.toString(params);
            JSONObject request = new JSONObject();
            request.put("method", method);
            request.put("params", params);
            out.println(request.toJSONString());

            System.out.println("Sending request: " + request);
            out.println(request);  // 发送请求

            // 等待响应
            String response = in.readLine();  // 读取响应
            System.out.println("Received response: " + response);

            // 返回响应
            return response;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    // 测试方法
    public void test() {
        // 模拟调用 askNextNodeId
//        int nodeId = Integer.parseInt((String) call("127.0.0.1", 9992, "askNextNodeId", new Object[]{1}));
//        System.out.println("Next Node ID: " + nodeId);
//
//        // 模拟调用 askNextNodePort
//        int port = Integer.parseInt((String) call("127.0.0.1", 9992, "askNextNodePort", new Object[]{1}));
//        System.out.println("Next Node Port: " + port);

        // 模拟调用 askNextNodeAddress
        String address = (String) call("127.0.0.1", 9992, "askNextNodeAddress", new Object[]{1});
        System.out.println("Next Node Address: " + address);
    }
}

    /**
     * Create all source and operator executors.
     */
//    private void setupComponentExecutors() {
//        // Start from sources in the job and traverse components to create executors
//        // 为每个源都创建执行器
//        for (Source source: job.getSources()) {
//            SourceExecutor executor = new SourceExecutor(source);
//            executorList.add(executor);
//            // For each source, traverse the operations connected to it.
//            // 为每个源递归的创建联系
//            traverseComponent(source, executor);
//        }
//    }

    /* 找到指派的算子 */
//    private ComponentExecutor findExecutor(Task task) {
//        for (Source source: job.getSources()) {
//            if (source.getName().equals(task.getTaskType())){
//                SourceExecutor executor = new SourceExecutor(source);
//                return executor;
//            }
//            SourceExecutor executor = new SourceExecutor(source);
//            executorList.add(executor);
//            // For each source, traverse the operations connected to it.
//            // 为每个源递归的创建联系
//            return traverseComponent(source, executor, task);
//        }
//        return null;
//    }
//
//    /**
//     * Set up connections (intermediate queues) betw
//     */
//    private void setupConnections() {
//        for (Connection connection: connectionList) {
//            connectExecutors(connection);
//        }
//    }
//
//    /**
//     * Start all the processes for the job.
//     */
//    private void startProcesses() {
//        Collections.reverse(executorList);
//        for (ComponentExecutor executor: executorList) {
//            executor.start();
//        }
//        for (EventDispatcher dispatcher: dispatcherList) {
//            dispatcher.start();
//        }
//    }
//
//    private void connectExecutors(Connection connection) {
//        // Each component executor could connect to multiple downstream operator executors.
//        // 每个执行器组件可以连接多个下游执行器算子
//        // For each of the downstream operator executor, there is a stream manager.
//        // 对每个下游操作执行器，都有一个流管理器
//        // Each instance executor of the upstream executor connects to the all the stream managers
//        // of the downstream executors first. And each stream manager connects to all the instance
//        // executors of the downstream executor.
//        // 上游执行器的每个执行器实例首先连接下游的全部流管理器，每个流管理器连接所有的下游执行器实例
//        // Note that in this version, there is no shared "from" component and "to" component.
//        // The job looks like a single linked list.
//        EventDispatcher dispatcher = new EventDispatcher(connection.to);
//        dispatcherList.add(dispatcher);
//
//        // Connect to upstream.
//        EventQueue upstream = new EventQueue(QUEUE_SIZE);
//        connection.from.setOutgoingQueue(upstream);
//        dispatcher.setIncomingQueue(upstream);
//
//        // Connect to downstream (to each instance).
//        int parallelism = connection.to.getComponent().getParallelism();
//        EventQueue [] downstream = new EventQueue[parallelism];
//        for (int i = 0; i < parallelism; ++i) {
//            downstream[i] = new EventQueue(QUEUE_SIZE);
//        }
//        connection.to.setIncomingQueues(downstream);
//        dispatcher.setOutgoingQueues(downstream);
//    }
//
//    private ComponentExecutor traverseComponent(Component component, ComponentExecutor executor, Task task) {
//        Stream stream = component.getOutgoingStream();
//
//        for (Operator operator: stream.getAppliedOperators()) {
//            if (operator.getName().equals(task.getTaskType())) {
//                OperatorExecutor operatorExecutor = new OperatorExecutor(operator);
//                return operatorExecutor;
//            }
//            OperatorExecutor operatorExecutor = new OperatorExecutor(operator);
//            executorList.add(operatorExecutor);
//
//            // 为组件添加连接
//            connectionList.add(new Connection(executor, operatorExecutor));
//            // Setup executors for the downstream operators.
//            return traverseComponent(operator, operatorExecutor, task);
//        }
//        return null;
//    }
//}
