package org.davidmoten.eq.internal;

import org.davidmoten.eq.internal.event.Event;

public interface HasEventQueue {
    
    void send(Event event);
    
 // avoid two drains by offering this method
    void send(Event event1, Event event2);

}
