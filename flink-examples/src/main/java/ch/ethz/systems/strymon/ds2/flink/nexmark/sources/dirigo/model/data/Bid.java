package ch.ethz.systems.strymon.ds2.flink.nexmark.sources.dirigo.model.data;

import ch.ethz.systems.strymon.ds2.flink.nexmark.sources.dirigo.utils.NexmarkUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;

import java.io.Serializable;
import java.util.Objects;

/** A bid for an item on auction. */
public class Bid implements Serializable {

    /** Id of auction this bid is for. */
    @JsonProperty
    public long auction; // foreign key: Auction.id

    /** Id of person bidding in auction. */
    @JsonProperty public long bidder; // foreign key: Person.id

    /** Price of bid, in cents. */
    @JsonProperty public long price;

    /**
     * Instant at which bid was made (ms since epoch). NOTE: This may be earlier than the system's
     * event time.
     */
//    @JsonProperty public Instant dateTime;

    @JsonProperty public long dateTime;
    /** Additional arbitrary payload for performance testing. */
    @JsonProperty public String extra="";

    public Bid(){}

    public Bid(long auction, long bidder, long price, long dateTime, String extra) {
        this.auction = auction;
        this.bidder = bidder;
        this.price = price;
        this.dateTime = dateTime;
        this.extra = extra;
    }

    @Override
    public boolean equals(Object otherObject) {
        if (this == otherObject) {
            return true;
        }
        if (otherObject == null || getClass() != otherObject.getClass()) {
            return false;
        }

        Bid other = (Bid) otherObject;
        return Objects.equals(auction, other.auction)
                && Objects.equals(bidder, other.bidder)
                && Objects.equals(price, other.price)
                && Objects.equals(dateTime, other.dateTime)
                && Objects.equals(extra, other.extra);
    }

    @Override
    public int hashCode() {
        return Objects.hash(auction, bidder, price, dateTime, extra);
    }

    @Override
    public String toString() {
        try {
            return NexmarkUtils.MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
