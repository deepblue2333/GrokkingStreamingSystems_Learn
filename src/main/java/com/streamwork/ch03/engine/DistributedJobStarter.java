package com.streamwork.ch03.engine;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;

import com.streamwork.ch03.api.Component;
import com.streamwork.ch03.api.Job;
import com.streamwork.ch03.api.Operator;
import com.streamwork.ch03.api.Source;
import com.streamwork.ch03.api.Stream;
import com.streamwork.ch03.common.Task;
import com.streamwork.ch03.func.ApplyFunc;
import com.streamwork.ch03.func.VehicleMapperFunc;
import com.streamwork.ch03.rpc.io.RpcNode;

public class DistributedJobStarter extends RpcNode {
    // 设置队列容量
    private final static int QUEUE_SIZE = 64;

    // 初始化算子id号
    private int id = 0;

    // The job to start
    public final Job job;
    // List of executors and stream managers执行器队列和流管理器队列
    private final List<ComponentExecutor> executorList = new ArrayList<ComponentExecutor>();
    private final List<EventDispatcher> dispatcherList = new ArrayList<EventDispatcher>();

    // 解析完成后的任务列表
    private final HashMap<Integer, Task> tasksMap = new HashMap<Integer, Task>();
    private final ConcurrentLinkedDeque<Task> tasks = new ConcurrentLinkedDeque<>();

    private final List<String> know_hosts = new ArrayList<String>(List.of("127.0.0.2", "127.0.0.3"));
    private final Set<String> allocatedAddresses = new TreeSet<>();

    private int count = 0;

    // Connections between component executors 组件间的连接器
    private final List<Connection> connectionList = new ArrayList<Connection>();
    private static Map<Integer, ApplyFunc> applyFuncMap = new HashMap<>();
    private static int funcId = 1; // 用于生成唯一的 ID
    // 把job赋给该实例类
    public DistributedJobStarter(Job job) {
        this.job = job;
    }
    private final Map<Integer, Record> taskRecords = new HashMap<>();

    public static class Record {
        int id;
        String host;
        int port;
        Record(int id, String host, int port) {
            this.id = id;  // 添加初始化
            this.host = host;
            this.port = port;
        }
        @Override
        public String toString() {
            return "Record{id=" + id + ", host='" + host + "', port=" + port + "}";
        }
    }
    public Map<Integer, Record> getTaskRecords() {
        return taskRecords;
    }


    public void start() throws Exception {
        // Set up executors for all the components.
        setupComponentExecutors();

        // Start web server
        new WebServer(job.getName(), connectionList).start();

        // 设置服务端口为9992
        setPort(9992);
        serve();
        System.out.println("success startjob");
        // All components are created now. Build the connections to connect the components together.
//        setupConnections();

        // Start all the processes.
//        startProcesses();
    }
    public void startDistributeTask() {
        /* ToDo 轮流给每个工作节点指派任务 */
        informRequireTask();
        System.out.println("TASKS"+tasks);

//        new HashMap<Integer, Record>();
    }
    public synchronized void informRequireTask() {
        // 遍历 know_hosts 列表，逐个向工作节点发送任务请求
        System.out.println("MasterStarter: Informing worknode " );
        for (String ip : know_hosts) {
            System.out.println("MasterStarter: Informing worknode " + ip);
            call(ip, 9993, "requireTask", new Object[]{});
            System.out.println("Success");
        }
    }

    /**
     * 指派任务
     *
     * @return
     */
    private synchronized Task requireTask() {

        if (tasks.isEmpty()) {
            return null;
        }
        System.out.println("TASKS"+tasks);

        return tasks.poll();

    }

    private synchronized Task findNextTask(Integer nodeId) {
        // 获取当前任务
        Task currentTask = tasksMap.get(nodeId);
        if (currentTask == null) {
            System.out.println("Task with ID " + nodeId + " not found.");
            return null;
        }

        // 获取当前任务的组件
        Component currentComponent = currentTask.getComponent();

        // 假设每个组件都有一个 "outgoingStream" 来表示任务流
        Stream currentStream = currentComponent.getOutgoingStream();

        // 遍历流中的每个操作符
        for (Operator operator : currentStream.getAppliedOperators()) {
            // 对每个操作符，查找连接的下游任务
            for (Task task : tasksMap.values()) {
                // 如果该任务的组件与当前操作符相匹配，则认为它是下游任务
                if (task.getComponent().equals(operator)) {
                    return task;  // 找到下一个任务（下游任务）
                }
            }
        }

        // 如果没有找到下游任务，返回 null
        return null;
    }
//    public int askNextNodeId(int nodeId) {
//        Task t = findNextTask(nodeId);
//        return t.getId();
//    }
////    public int askNextNodeport(int nodeId) {
////        Task t = findNextTask(nodeId);
//////        return t.get();
////    }
//    public String askNextNodeAddress(int nodeId) {
////        Task t = findNextTask(nodeId);
////        return t.getIpaddress();
//    }
    /**
     * Create all source and operator executors.
     */
    private void setupComponentExecutors() {
        // Start from sources in the job and traverse components to create executors
        // 为每个源都创建执行器
        for (Source source: job.getSources()) {

            SourceExecutor executor = new SourceExecutor(source);
            executorList.add(executor);

            Task t = new Task(allocateId(), source.getName(), source.getParallelism(), allocateAddress(), source);


            tasksMap.put(t.getId(), t);
            tasks.add(t);
            // For each source, traverse the operations connected to it.
            // 为每个源递归的创建联系
            traverseComponent(source, executor);
        }
    }


    /**
     * Set up connections (intermediate queues) between all component executors.
     */
    private void setupConnections() {
        for (Connection connection: connectionList) {
            connectExecutors(connection);
        }
    }

    /**
     * Start all the processes for the job.
     */
    private void startProcesses() {
        Collections.reverse(executorList);
        for (ComponentExecutor executor: executorList) {
            executor.start();
        }
        for (EventDispatcher dispatcher: dispatcherList) {
            dispatcher.start();
        }
    }

    private void connectExecutors(Connection connection) {
        // Each component executor could connect to multiple downstream operator executors.
        // 每个执行器组件可以连接多个下游执行器算子
        // For each of the downstream operator executor, there is a stream manager.
        // 对每个下游操作执行器，都有一个流管理器
        // Each instance executor of the upstream executor connects to the all the stream managers
        // of the downstream executors first. And each stream manager connects to all the instance
        // executors of the downstream executor.
        // 上游执行器的每个执行器实例首先连接下游的全部流管理器，每个流管理器连接所有的下游执行器实例
        // Note that in this version, there is no shared "from" component and "to" component.
        // The job looks like a single linked list.
        EventDispatcher dispatcher = new EventDispatcher(connection.to);
        dispatcherList.add(dispatcher);

        // Connect to upstream.
        EventQueue upstream = new EventQueue(QUEUE_SIZE);
        connection.from.setOutgoingQueue(upstream);
        dispatcher.setIncomingQueue(upstream);

        // Connect to downstream (to each instance).
        int parallelism = connection.to.getComponent().getParallelism();
        EventQueue [] downstream = new EventQueue[parallelism];
        for (int i = 0; i < parallelism; ++i) {
            downstream[i] = new EventQueue(QUEUE_SIZE);
        }
        connection.to.setIncomingQueues(downstream);
        dispatcher.setOutgoingQueues(downstream);
    }

    private void traverseComponent(Component component, ComponentExecutor executor) {
        Stream stream = component.getOutgoingStream();
        System.out.println(stream);
        for (Operator operator: stream.getAppliedOperators()) {
            System.out.println(operator);
            OperatorExecutor operatorExecutor = new OperatorExecutor(operator);
            executorList.add(operatorExecutor);

            Task t = new Task(allocateId(), operator.getName(), operator.getParallelism(), allocateAddress(), operator);


            tasksMap.put(t.getId(), t);
            tasks.add(t);
            System.out.println("tasks2"+tasks);
            for (Task task : tasks) {

                Record record = assignTaskToNode(task);
                System.out.println("Task assigned: " + record);
            }

            // 为组件添加连接
            connectionList.add(new Connection(executor, operatorExecutor));
            // Setup executors for the downstream operators.
            traverseComponent(operator, operatorExecutor);
        }
    }

    private Record assignTaskToNode(Task task) {
        String address = allocateAddress();
        String[] parts = address.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);

        Record record = new Record(task.getId(), host, port);
        taskRecords.put(task.getId(), record);

        return record;
    }

    private String allocateAddress() {
        String address;
        String ip;
        String port;

        do {
            if (count >= know_hosts.size()) {
                count = 0;
            }
            count++;
            ip = know_hosts.get(count - 1);
            port = String.valueOf((int) ((Math.random() * (15000 - 14000)) + 14000));
            address = ip + ":" + port;
        } while (allocatedAddresses.contains(address));

        return address;
    }

    private Integer allocateId() {
        return id++;
    }
    public void storeApplyFunc(ApplyFunc applyFunc) {
        applyFuncMap.put(funcId++, applyFunc);
        System.out.println("4444"+applyFuncMap);
    }

    public synchronized String requireApplyFunc(Integer id) throws IOException {

        // 将 ApplyFunc 对象序列化为 JSON 字符串
        VehicleMapperFunc applyFunc = (VehicleMapperFunc) applyFuncMap.get(id);
        String serializedFunc = serialize(applyFunc);
        System.out.println("Serialized ApplyFunc: " + serializedFunc);
        System.out.println(applyFunc);
        // 将 ApplyFunc 对象序列化为 JSON 字符串
//        String serializedApplyFunc = JSON.toJSONString(applyFunc);
        System.out.println(serializedFunc);
        // 返回序列化后的 JSON 字符串
        return serializedFunc;
    }
    public static String serialize(Serializable obj) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {
            objectOutputStream.writeObject(obj);
        }

        // 将字节数组转换为 Base64 编码的字符串
        return Base64.getEncoder().encodeToString(byteArrayOutputStream.toByteArray());
    }

}
