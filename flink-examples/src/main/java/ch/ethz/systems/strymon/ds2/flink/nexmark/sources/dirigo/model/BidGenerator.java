package ch.ethz.systems.strymon.ds2.flink.nexmark.sources.dirigo.model;

import ch.ethz.systems.strymon.ds2.flink.nexmark.sources.dirigo.GeneratorConfig;
import ch.ethz.systems.strymon.ds2.flink.nexmark.sources.dirigo.model.data.Bid;

import java.util.Random;

/** Generates bids. */
public class BidGenerator {

    /**
     * Fraction of people/auctions which may be 'hot' sellers/bidders/auctions are 1 over these
     * values.
     */
    private static final int HOT_AUCTION_RATIO = 100;

    private static final int HOT_BIDDER_RATIO = 100;

    /** Generate and return a random bid with next available id. */
    public static Bid nextBid(long eventId, Random random, long timestamp, GeneratorConfig config) {

        long auction;
        // Here P(bid will be for a hot auction) = 1 - 1/hotAuctionRatio.
        if (random.nextInt(config.getHotAuctionRatio()) > 0) {
            // Choose the first auction in the batch of last HOT_AUCTION_RATIO auctions.
            auction = (AuctionGenerator.lastBase0AuctionId(config, eventId) / HOT_AUCTION_RATIO) * HOT_AUCTION_RATIO;
        } else {
            auction = AuctionGenerator.nextBase0AuctionId(eventId, random, config);
        }
        auction += GeneratorConfig.FIRST_AUCTION_ID;

        long bidder;
        // Here P(bid will be by a hot bidder) = 1 - 1/hotBiddersRatio
        if (random.nextInt(config.getHotBiddersRatio()) > 0) {
            // Choose the second person (so hot bidders and hot sellers don't collide) in the batch of
            // last HOT_BIDDER_RATIO people.
            bidder = (PersonGenerator.lastBase0PersonId(config, eventId) / HOT_BIDDER_RATIO) * HOT_BIDDER_RATIO + 1;
        } else {
            bidder = PersonGenerator.nextBase0PersonId(eventId, random, config);
        }
        bidder += GeneratorConfig.FIRST_PERSON_ID;

        long price = PriceGenerator.nextPrice(random);
        int currentSize = 8 + 8 + 8 + 8;
        String extra = StringsGenerator.nextExtra(random, currentSize, config.getAvgBidByteSize());
        // return new Bid(auction, bidder, price, Instant.ofEpochMilli(timestamp), extra);
        return new Bid(auction, bidder, price, timestamp, extra);
    }
}

