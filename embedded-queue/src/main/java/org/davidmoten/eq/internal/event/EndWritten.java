package org.davidmoten.eq.internal.event;

import org.davidmoten.eq.internal.event.Event;

public class EndWritten implements Event {
    
    private static final EndWritten instance = new EndWritten();
    
    public static EndWritten instance() {
        return instance;
    }
    
    private EndWritten() {
        
    }

}
