package com.streamwork.ch03.job;

import com.streamwork.ch03.api.Event;

import java.io.Serializable;

public class VehicleEvent extends Event implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String type;

  public VehicleEvent(String type) {
    this.type = type;
  }

  @Override
  public String getData() {
    return type;
  }
}
