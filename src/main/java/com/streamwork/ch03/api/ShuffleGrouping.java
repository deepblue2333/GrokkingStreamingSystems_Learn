package com.streamwork.ch03.api;

import java.io.Serializable;

/**
 * With shuffle grouping, the events are routed to downstream
 * instances relatively evenly. This implementation is round robin
 * based instead of random number based because it is simpler and
 * deterministic.
 *
 * shuffle grouping采用轮询法将数据均匀分布在不同实例间
 */
public class ShuffleGrouping implements GroupingStrategy, Serializable {
  private static final long serialVersionUID = -1763295335424683087L;

  private int count = 0;

  public ShuffleGrouping() { }

  /**
   * Get target instance id from an event and component parallelism.
   * @param event The event object to route to the component.
   * @param The parallelism of the component.
   * @return The integer key of this event.
   */
  @Override
  public int getInstance(Event event, int parallelism) {
    if (count >= parallelism) {
      count = 0;
    }
    count++;
    return count - 1;
  }
}
