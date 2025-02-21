package com.streamwork.ch03.engine;

import com.streamwork.ch03.api.Event;
import com.streamwork.ch03.api.GroupingStrategy;

/**
 * EventDispatcher is responsible for transporting events from
 * the incoming queue to the outgoing queues with a grouping strategy.
 * 事件调度器
 */
public class EventDispatcher extends Process {
  private final OperatorExecutor downstreamExecutor;
  public EventQueue incomingQueue = null;
  private EventQueue [] outgoingQueues = null;

  public EventDispatcher(OperatorExecutor downstreamExecutor) {
    this.downstreamExecutor = downstreamExecutor;
  }

  @Override
  boolean runOnce() {
    try {
      Event event = incomingQueue.take();

      GroupingStrategy grouping = downstreamExecutor.getGroupingStrategy();

      // 传入事件和并行度以获得实例id
      int instance = grouping.getInstance(event, outgoingQueues.length);

      // 推入指定实例id号的输出队列
      outgoingQueues[instance].put(event);
    } catch (InterruptedException e) {
      return false;
    }
    return true;
  }

  public void setIncomingQueue(EventQueue queue) {
    incomingQueue = queue;
  }

  public void setOutgoingQueues(EventQueue [] queues) {
    outgoingQueues = queues;
  }
}
