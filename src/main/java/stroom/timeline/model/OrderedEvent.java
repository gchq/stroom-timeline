package stroom.timeline.model;

import java.time.Instant;

public class OrderedEvent {

    //provides uniqueness in case of a clash on time (which is quite likely)
    private final SequentialIdentifierProvider sequentialIdentifierProvider;
    private final Event event;


    public OrderedEvent(Instant eventTime, byte[] content, SequentialIdentifierProvider sequentialIdentifierProvider) {
        this.event = new Event(eventTime, content);
        this.sequentialIdentifierProvider = sequentialIdentifierProvider;
    }
    public OrderedEvent(Instant eventTime, byte[] content) {
        this(eventTime, content, new DefaultSequentialIdentifierProviderImpl());
    }

    public byte[] getSequentialIdentifier() {
        return sequentialIdentifierProvider.getBytes();
    }

    public SequentialIdentifierProvider getSequentialIdentifierProvider() {
        return sequentialIdentifierProvider;
    }

    public Instant getEventTime(){
        return event.getEventTime();
    }

    public byte[] getContent() {
        return event.getContent();
    }

    public Event getEvent() {
        return event;
    }

    @Override
    public String toString() {
        return "OrderedEvent{" +
                "event=" + event +
                ", sequentialIdentifier=" + sequentialIdentifierProvider.toHumanReadable() +
                '}';
    }
}