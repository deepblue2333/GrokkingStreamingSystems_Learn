package com.streamwork.ch03.func;

import com.streamwork.ch03.api.Event;
import com.streamwork.ch03.job.VehicleEvent;

import java.io.Serializable;
import java.util.List;

public class VehicleMapperFunc implements ApplyFunc, Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    public void apply(Event inputEvent, List<Event> eventCollector) {
        Event outputEvent = new VehicleEvent(inputEvent.getData() + "_map");
        eventCollector.add(outputEvent);
        System.out.println("VehicleMapper applied: " + outputEvent.getData());
    }
}