package nexmark.sources;

import static org.apache.beam.sdk.nexmark.sources.generator.model.LongGenerator.nextLong;
import static org.apache.beam.sdk.nexmark.sources.generator.model.PersonGenerator.lastBase0PersonId;
import static org.apache.beam.sdk.nexmark.sources.generator.model.PersonGenerator.nextBase0PersonId;
import static org.apache.beam.sdk.nexmark.sources.generator.model.PriceGenerator.nextPrice;
import static org.apache.beam.sdk.nexmark.sources.generator.model.StringsGenerator.nextExtra;
import static org.apache.beam.sdk.nexmark.sources.generator.model.StringsGenerator.nextString;

import java.util.Random;

import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorConfig;
import org.apache.beam.sdk.nexmark.sources.generator.model.AuctionGenerator;


public class CustomAuctionGenerator extends AuctionGenerator{

    private static final int NUM_CATEGORIES = 5;
    private static final int HOT_SELLER_RATIO = 100; 
    
    public static Auction nextAuction(
        long eventsCountSoFar, long eventId, Random random, long timestamp, GeneratorConfig config, double skew) {
  
      long id = lastBase0AuctionId(eventId) + GeneratorConfig.FIRST_AUCTION_ID;
  
      long seller;
      // Here P(auction will be for a hot seller) = 1 - 1/hotSellersRatio.
      if (random.nextDouble() < skew) {
        // Choose the first person in the batch of last HOT_SELLER_RATIO people.
        seller = (lastBase0PersonId(eventId) / HOT_SELLER_RATIO) * HOT_SELLER_RATIO;
      } else {
        seller = nextBase0PersonId(eventId, random, config);
      }
      seller += GeneratorConfig.FIRST_PERSON_ID;
  
      long category = GeneratorConfig.FIRST_CATEGORY_ID + random.nextInt(NUM_CATEGORIES);
      long initialBid = nextPrice(random);
      long expires = timestamp + nextAuctionLengthMs(eventsCountSoFar, random, timestamp, config);
      String name = nextString(random, 20);
      String desc = nextString(random, 100);
      long reserve = initialBid + nextPrice(random);
      int currentSize = 8 + name.length() + desc.length() + 8 + 8 + 8 + 8 + 8;
      String extra = nextExtra(random, currentSize, config.getAvgAuctionByteSize());
      return new Auction(id, name, desc, initialBid, reserve, timestamp, expires, seller, category, extra);
    }

    private static long nextAuctionLengthMs(
        long eventsCountSoFar, Random random, long timestamp, GeneratorConfig config) {
  
      // What's our current event number?
      long currentEventNumber = config.nextAdjustedEventNumber(eventsCountSoFar);
      // How many events till we've generated numInFlightAuctions?
      long numEventsForAuctions =
          ((long) config.getNumInFlightAuctions() * GeneratorConfig.PROPORTION_DENOMINATOR)
              / GeneratorConfig.AUCTION_PROPORTION;
      // When will the auction numInFlightAuctions beyond now be generated?
      long futureAuction =
          config
              .timestampAndInterEventDelayUsForEvent(currentEventNumber + numEventsForAuctions)
              .getKey();
      // System.out.printf("*** auction will be for %dms (%d events ahead) ***\n",
      //     futureAuction - timestamp, numEventsForAuctions);
      // Choose a length with average horizonMs.
      long horizonMs = futureAuction - timestamp;
      return 1L + nextLong(random, Math.max(horizonMs * 2, 1L));
    }
  
}
