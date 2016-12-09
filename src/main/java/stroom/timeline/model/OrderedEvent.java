package stroom.timeline.model;

import stroom.timeline.model.identifier.SequentialIdentifierProvider;
import stroom.timeline.model.identifier.UuidIdentifier;

import java.time.Instant;

public class OrderedEvent implements Comparable<OrderedEvent> {

    //provides uniqueness in case of a clash on time (which is quite likely)
    private final SequentialIdentifierProvider sequentialIdentifier;
    private final Event event;


    public OrderedEvent(Instant eventTime, byte[] content, SequentialIdentifierProvider sequentialIdentifier) {
        this.event = new Event(eventTime, content);
        this.sequentialIdentifier = sequentialIdentifier;
    }
    public OrderedEvent(Instant eventTime, byte[] content) {
        this(eventTime, content, new UuidIdentifier());
    }

    public byte[] getSequentialIdentifierBytes() {
        return sequentialIdentifier.getBytes();
    }

    public SequentialIdentifierProvider getSequentialIdentifier() {
        return sequentialIdentifier;
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
                ", sequentialIdentifier=" + sequentialIdentifier.toHumanReadable() +
                '}';
    }

    @Override
    public int compareTo(OrderedEvent other) {
        return this.event.compareTo(other.getEvent());
    }
}