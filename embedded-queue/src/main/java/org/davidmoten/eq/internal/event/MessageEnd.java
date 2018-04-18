package org.davidmoten.eq.internal.event;

public final class MessageEnd implements Part {

    private static final MessageEnd INSTANCE = new MessageEnd();

    public static MessageEnd instance() {
        return INSTANCE;
    }

    private MessageEnd() {
    }

    @Override
    public int length() {
        throw new RuntimeException("unexpected, report to developer");
    }

}
