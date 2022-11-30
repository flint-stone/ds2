package ch.ethz.systems.strymon.ds2.flink.nexmark.sources.dirigo.model.data;

//import model.Auction;
//import model.Bid;
//import model.Person;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * An event in the auction system, either a (new) {@link model.Person}, a (new) {@link model.Auction}, or a
 * {@link model.Bid}.
 */
public class Event {

    public @Nullable
    Person newPerson;
    public @Nullable
    Auction newAuction;
    public @Nullable
    Bid bid;
    public Type type;
    public @Nullable short sourceIndex;

    /** The type of object stored in this event. * */
    public enum Type {
        PERSON(0),
        AUCTION(1),
        BID(2);

        public final int value;

        Type(int value) {
            this.value = value;
        }
    }

    public Event(Person newPerson) {
        this.newPerson = newPerson;
        newAuction = null;
        bid = null;
        type = Type.PERSON;
    }

    public Event(Auction newAuction) {
        newPerson = null;
        this.newAuction = newAuction;
        bid = null;
        type = Type.AUCTION;
    }

    public Event(Bid bid) {
        newPerson = null;
        newAuction = null;
        this.bid = bid;
        type = Type.BID;
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Event event = (Event) o;
        return Objects.equals(newPerson, event.newPerson)
                && Objects.equals(newAuction, event.newAuction)
                && Objects.equals(bid, event.bid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(newPerson, newAuction, bid);
    }

    @Override
    public String toString() {
        if (newPerson != null) {
            return newPerson.toString();
        } else if (newAuction != null) {
            return newAuction.toString();
        } else if (bid != null) {
            return bid.toString();
        } else {
            throw new RuntimeException("invalid event");
        }
    }

    public Long getEventTimestamp() {
        if (newPerson != null) {
            return newPerson.dateTime.toEpochMilli();
        } else if (newAuction != null) {
            return newAuction.dateTime.toEpochMilli();
        } else if (bid != null) {
            return bid.dateTime;
        } else {
            throw new RuntimeException("invalid event");
        }
    }
}

