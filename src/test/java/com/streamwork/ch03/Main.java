package com.streamwork.ch03;

import com.streamwork.ch03.engine.WorkerStarter;

public class Main {
    public static void main(String[] args) {
        // 创建 WorkerStarter 实例
        WorkerStarter workerStarter = new WorkerStarter();

        // 执行测试
        workerStarter.test();
    }
}
