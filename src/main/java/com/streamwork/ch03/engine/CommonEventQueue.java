package com.streamwork.ch03.engine;

import com.streamwork.ch03.api.Event;

public interface CommonEventQueue {

    void put(Event e) throws Exception;
}
