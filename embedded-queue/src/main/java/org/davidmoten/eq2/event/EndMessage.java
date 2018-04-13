package org.davidmoten.eq2.event;

public final class EndMessage implements Part {

    private static final EndMessage INSTANCE = new EndMessage();
    
    public static EndMessage instance() {
        return INSTANCE;
    }
    
    private EndMessage() {
    }

}
