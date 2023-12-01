package nexmark.sources;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorConfig;
import org.apache.beam.sdk.nexmark.sources.generator.model.AuctionGenerator;
import org.apache.beam.sdk.nexmark.sources.generator.model.PersonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.beam.sdk.nexmark.sources.generator.model.BidGenerator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.Random;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;

import java.util.UUID;  

import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.Person;


/**
 * BidPersonAuctionSourceFunction contains all functionality expected for a generator for Bids, Auctions and person
 * records. It is extended by BidPersonAuctionParallelFunction to allow for parlallel execution of multiple source
 * functions.
 */
public class BidPersonAuctionSourceFunction extends Thread {
    String PERSON_TOPIC = "personsSource";
    String BID_TOPIC = "bidsSource";
    String AUCTION_TOPIC = "auctionsSource";

    Producer<byte[], byte[]> producer;
    ObjectMapper objectMapper;
    GeneratorConfig generatorConfig;

    long epochStartTimeMs;
    final long epochDurationMs;
    public long eventsPerEpoch;
    long firstEpochEvent;
    long eventsCountSoFar;

    boolean enablePersonTopic;
    boolean enableAuctionTopic;
    boolean enableBidTopic;

    int uniPersonsPartitions;
    int uniAuctionsPartitions;
    int uniBidsPartitions;

    double skew;

    int stoppedIterationNumber;
    Random roundRobinPartitioner = new Random();

    /**
     * Constructor of BidPersonAuctionSourceFunction
     * @param kafkaServer String providing the location (url) of the kafka server.
     * @param epochDurationMs Amount of ms an epoch takes.
     * @param enablePersonTopic Whether to generate person records.
     * @param enableAuctionTopic Whether to generate auction records.
     * @param enableBidTopic Whether to generate bid records.
     */
    public BidPersonAuctionSourceFunction(String kafkaServer,
                                          long epochDurationMs,
                                          boolean enablePersonTopic,
                                          boolean enableAuctionTopic,
                                          boolean enableBidTopic,
                                          int uniBidsPartitions, 
                                          int uniAuctionsPartitions, 
                                          int uniPersonsPartitions,
                                          double skew){
        // Create producer
        if (kafkaServer != null) {
            // Not in testing environment
            Properties props = new Properties();
            props.put("bootstrap.servers", kafkaServer);
            props.put("acks", "1");
            props.put("retries", "0");
            props.put("linger.ms", "10");
            props.put("batch.size", "50000");
            props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
            this.producer = new KafkaProducer<>(props);
        }

        // Creating object mapper
        this.objectMapper = new ObjectMapper();
        // ConfigGenerator

        NexmarkConfiguration nexmarkConfiguration = NexmarkConfiguration.DEFAULT;
        // Set hot ratio to 1 to prevent possible skew. Chance of getting a hot item is 1 - 1 / ratio.
        // Setting it to 1 disables picking hot values.
        // if (skew){        
        //     nexmarkConfiguration.hotAuctionRatio = 2;
        //     nexmarkConfiguration.hotBiddersRatio = 2;
        //     nexmarkConfiguration.hotSellersRatio = 2;
        // }
        // else{
        //     nexmarkConfiguration.hotAuctionRatio = 1;
        //     nexmarkConfiguration.hotBiddersRatio = 1;
        //     nexmarkConfiguration.hotSellersRatio = 1;
        // }
        this.skew= skew;

        this.generatorConfig = new GeneratorConfig(
                nexmarkConfiguration,
                1,
                1000L,
                0,
                0
        );

        // Topics to generate
        this.enablePersonTopic = enablePersonTopic;
        this.enableAuctionTopic = enableAuctionTopic;
        this.enableBidTopic = enableBidTopic;

        // Set epoch settings
        this.epochDurationMs = epochDurationMs;
        this.epochStartTimeMs = 0;
        this.eventsPerEpoch = 0;
        this.firstEpochEvent = 0;
        this.eventsCountSoFar = 0;

        // Highest iteration number that is stopped
        this.stoppedIterationNumber = 0;

        // Set universalis source partitions
        this.uniBidsPartitions = uniBidsPartitions;
        this.uniAuctionsPartitions = uniAuctionsPartitions;
        this.uniPersonsPartitions = uniPersonsPartitions;

    }

    /**
     * Get the current event number and increment it for next round.
     * @return the next event Id.
     */
    public long getNextEventNumber(){
        return this.eventsCountSoFar++;
    }

    /**
     * Increment the current event number.
     * @param increment How much to increment the current eventsCountSoFar.
     */
    public void incrementEventNumber(long increment) {
        this.eventsCountSoFar += increment;
    }


    /**
     * Get the corresponding timestamp based on eventNumber.
     * @param eventNumber Event number to determine a timestamp for
     * @return Timestamp of event number in Ms.
     */
    public Long getTimestampsMsforEvent(long eventNumber) {
        long timestampUs = this.getTimestampUsforEvent(eventNumber);
        return timestampUs / 1000L;
    }

    /**
     * Get the corresponding timestamp based on eventNumber.
     * @param eventNumber Event number to determine a timestamp for
     * @return Timestamp of event number in Us.
     */
    public Long getTimestampUsforEvent(long eventNumber){
        return this.getTimestampUsforEvent(
                this.epochStartTimeMs,
                this.epochDurationMs,
                this.eventsPerEpoch,
                this.firstEpochEvent,
                eventNumber
        );
    }

    /***
     * Get the timestamp for the currentEvent. This is calculated by calculating the place of the event in the current
     * epoch. Then, using the interEventDelayUs, the event is given a timestamp in Us.
     * If the eventID is from a previous epoch, the first eventIe
     * @param epochStartTimeMs The start of the epoch in Ms.
     * @param epochDurationMs The duration of the epoch in Ms.
     * @param eventsPerEpoch The amount of epochs per second.
     * @param firstEventCurrentEpoch The frist eventId of the current epoch.
     * @param eventNumber The eventNumber to determine the timestamp for.
     * @return The timestamp of the current eventNumber in Us.
     */
    public Long getTimestampUsforEvent(long epochStartTimeMs,
                                       long epochDurationMs,
                                       long eventsPerEpoch,
                                       long firstEventCurrentEpoch,
                                       long eventNumber) {
        long epochStartTimeUs = epochStartTimeMs * 1000;
        long epochDurationUs = epochDurationMs * 1000;
        long n = eventNumber - firstEventCurrentEpoch;
        if (n < 0) {
            // Something went wrong: n is eventNumber is from a previous epoch. Using the smallest timestamp from current
            // epoch.
            n=0;
        }
        double interEventDelayUs = (double) epochDurationUs / (double) eventsPerEpoch;
        long eventEpochOffsetUs = (long)(interEventDelayUs * n);
        return epochStartTimeUs + eventEpochOffsetUs;
    }

    /**
     * Make generator ready for the next epoch generation.
     * * This adds the epochDurationMS to epochStartTimeMs,
     * * Set the eventsCountSoFar to eventsAfterEpoch (in case we were not able to produce all events)
     * * Set the eventsAfterEpoch to the next amount of epochs we'll generate
     * @param totalEpochEvents Amount of events this epoch will generate.
     */
    public void setNextEpochSettings(long totalEpochEvents) {
        if (this.eventsCountSoFar != 0) {
            this.epochStartTimeMs += this.epochDurationMs;
            this.firstEpochEvent = this.firstEpochEvent + this.eventsPerEpoch;
            this.eventsCountSoFar = this.firstEpochEvent;
        }
        this.eventsPerEpoch = totalEpochEvents;
     }

     public int getTotalProportion() {
        int eventsPerEpoch = 0;
        if (this.enablePersonTopic) {
            eventsPerEpoch += GeneratorConfig.PERSON_PROPORTION;
        }
        if (this.enableAuctionTopic) {
            eventsPerEpoch += GeneratorConfig.AUCTION_PROPORTION;
        }
        if (this.enableBidTopic) {
            int bidsProportion = GeneratorConfig.PROPORTION_DENOMINATOR
                    - GeneratorConfig.PERSON_PROPORTION - GeneratorConfig.AUCTION_PROPORTION;
            eventsPerEpoch += bidsProportion;
        }
        return eventsPerEpoch;
     }

    /**
     * Given the total amount of events that have to be generated, how much will the ID grow by at the end of
     * the epoch.
     * @param amountOfEvents Amount of events to generate this epoch
     * @return The increase of eventID after generating the provided amount of events
     */
     public int getTotalIdIncrease(long amountOfEvents) {
        double usedProportion = this.getTotalProportion();
        int epochs = (int) Math.ceil((double) amountOfEvents / usedProportion);
        return epochs * GeneratorConfig.PROPORTION_DENOMINATOR;
     }

    /**
     * Generate a portion of all the epoch events.
     * The portion that is generated is on index firstEventIndex - firstEventIndex + eventsToGenerate.
     * @param totalEpochEvents Total events generated in this epoch. Used for ensuring correct ID's.
     * @param firstEventIndex The index of the first event it has to generate.
     * @param eventsToGenerate The amount of events that have to be generated.
     * @throws JsonProcessingException Generator error.
     */
     public void generatePortionOfEpochEvents(long totalEpochEvents, long firstEventIndex, long eventsToGenerate,
                                              int currentIterationNumber) throws JsonProcessingException {
         int totalIdIncrease = this.getTotalIdIncrease(totalEpochEvents);
         this.setNextEpochSettings(totalIdIncrease);
         int beforeIdIncrease = this.getTotalIdIncrease(firstEventIndex);
         this.incrementEventNumber(beforeIdIncrease);
         this.generateEvents(eventsToGenerate, currentIterationNumber);
     }

    /**
     * Generate all events for the upcomming epoch.
     * @param totalEpochEvents Events to produce in this epoch.
     * @param currentIterationNumber The number of the iteration the generator is currently generating events for.
     *                               The generator is only allowed to generate iterations when the currentIterationNumber
     *                               is larger than the stoppedIterationNumber
     * @throws JsonProcessingException Processing error thrown by event generation.
     */
    private void generateEvents(long totalEpochEvents, int currentIterationNumber) throws JsonProcessingException {
        long remainingEpochEvents = totalEpochEvents;
        while (remainingEpochEvents > 0 && currentIterationNumber > this.stoppedIterationNumber){
            long eventNumber = getNextEventNumber();
            long timestampMs = getTimestampsMsforEvent(eventNumber);
            long eventId = eventNumber + this.generatorConfig.firstEventId;
            Random rnd = new Random(eventId);
            long offset = eventId % GeneratorConfig.PROPORTION_DENOMINATOR;
            try {
                if (offset < GeneratorConfig.PERSON_PROPORTION) {
                    if (this.enablePersonTopic) {
                        // produce topic if enabled
                        this.producePersonEvent(eventId, rnd, timestampMs, this.uniPersonsPartitions);
                        remainingEpochEvents--;
                    } else {
                        // if not enabled, skip ID's concerning the topic.
                        // Note that we already incremented the eventNumber by 1.
                        this.incrementEventNumber((GeneratorConfig.PERSON_PROPORTION - offset) - 1);
                    }
                } else if (offset < GeneratorConfig.PERSON_PROPORTION + GeneratorConfig.AUCTION_PROPORTION) {
                    if (this.enableAuctionTopic) {
                        // produce topic if enabled
                        this.produceAuctionEvent(eventNumber, eventId, rnd, timestampMs, this.uniAuctionsPartitions);
                        remainingEpochEvents--;
                    } else {
                        // if not enabled, skip ID's concerning the topic
                        this.incrementEventNumber((GeneratorConfig.AUCTION_PROPORTION -
                                (offset - GeneratorConfig.PERSON_PROPORTION)) - 1);
                    }
                } else {
                    if (this.enableBidTopic) {
                        // produce topic if enabled
                        this.produceBidEvent(eventId, rnd, timestampMs, this.uniBidsPartitions);
                        remainingEpochEvents--;
                    } else {
                        // if not enabled, skip ID's concerning the topic
                        long bidProportion = GeneratorConfig.PROPORTION_DENOMINATOR - GeneratorConfig.PERSON_PROPORTION
                                - GeneratorConfig.AUCTION_PROPORTION;
                        this.incrementEventNumber((bidProportion -
                                (offset - GeneratorConfig.PERSON_PROPORTION - GeneratorConfig.AUCTION_PROPORTION)) - 1);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }


    /**
     * Produce a person event
     * @param eventId Id of the event.
     * @param rnd Random class used for creating the person event.
     * @param eventTimestampMs Timestamp of the event.
     * @throws JsonProcessingException Exception thrown by objectMapper.
     */
    public void producePersonEvent(long eventId, Random rnd, long eventTimestampMs, int uniPersonsPartitions) throws JsonProcessingException {
        
        Person person = PersonGenerator.nextPerson(eventId, rnd, eventTimestampMs, this.generatorConfig);

        int partition = roundRobinPartitioner.nextInt(uniPersonsPartitions);


        try {
            byte[] serByteArray = {Integer.valueOf(0).byteValue(), Integer.valueOf(1).byteValue()};
            byte[] msgByteArray = packEvent(person, partition, partition).toByteArray();
            ByteBuffer buff = ByteBuffer.wrap(new byte[ serByteArray.length + msgByteArray.length]);
            buff.put(serByteArray);
            buff.put(msgByteArray);

            MessageBufferPacker keyPacker = MessagePack.newDefaultBufferPacker();
            keyPacker.packLong(UUID.randomUUID().getMostSignificantBits());
            byte[] key = keyPacker.toByteArray();

            this.producer.send(new ProducerRecord<byte[], byte[]>(
                this.PERSON_TOPIC,
                partition,
                key,
                buff.array()
            ));

        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.exit(1000);
        }

    }
    /**
     * Produce a person event
     * @param eventId Id of the event.
     * @param eventNumber Number of the event (eventsIDs start at this.generatorConfig.firstEventId)
     * @param rnd Random class used for creating the person event.
     * @param eventTimestampMs Timestamp of the event.
     * @throws JsonProcessingException Exception thrown by objectMapper.
     */
    public void produceAuctionEvent(long eventId, long eventNumber, Random rnd, long eventTimestampMs, int uniAuctionsPartitions) throws JsonProcessingException{
        
        Auction auction = CustomAuctionGenerator.nextAuction(eventNumber, eventId, rnd, eventTimestampMs, this.generatorConfig, skew);

        int partition = roundRobinPartitioner.nextInt(uniAuctionsPartitions);

        try {
            byte[] serByteArray = {Integer.valueOf(0).byteValue(), Integer.valueOf(1).byteValue()};
            byte[] msgByteArray = packEvent(auction, auction.id, partition).toByteArray();
            ByteBuffer buff = ByteBuffer.wrap(new byte[ serByteArray.length + msgByteArray.length]);
            buff.put(serByteArray);
            buff.put(msgByteArray);

            MessageBufferPacker keyPacker = MessagePack.newDefaultBufferPacker();
            keyPacker.packLong(UUID.randomUUID().getMostSignificantBits());
            byte[] key = keyPacker.toByteArray();

            this.producer.send(new ProducerRecord<byte[], byte[]>(
                this.AUCTION_TOPIC,
                partition,
                key,
                buff.array()
            ));

        } catch (Exception e) {
            e.printStackTrace();;
            System.exit(-1);
        }
    }

    /**
     * Produce a bid event
     * @param eventId Id of the event.
     * @param rnd Random class used for creating the bid event.
     * @param eventTimestampMs Timestamp of the event.
     * @throws JsonProcessingException Exception thrown by objectMapper.
     */
    public void produceBidEvent(long eventId, Random rnd, long eventTimestampMs, int uniBidsPartitions) throws JsonProcessingException{
        
        Bid bid = CustomBidGenerator.nextBid(eventId, rnd, eventTimestampMs, this.generatorConfig, this.skew);
        int partition = roundRobinPartitioner.nextInt(uniBidsPartitions);

        try {
            byte[] serByteArray = {Integer.valueOf(0).byteValue(), Integer.valueOf(1).byteValue()};
            byte[] msgByteArray = packEvent(bid, partition, partition).toByteArray();
            ByteBuffer buff = ByteBuffer.wrap(new byte[ serByteArray.length + msgByteArray.length]);
            buff.put(serByteArray);
            buff.put(msgByteArray);

            MessageBufferPacker keyPacker = MessagePack.newDefaultBufferPacker();
            keyPacker.packLong(UUID.randomUUID().getMostSignificantBits());
            byte[] key = keyPacker.toByteArray();

            this.producer.send(new ProducerRecord<byte[], byte[]>(
                this.BID_TOPIC,
                partition,
                key,
                buff.array()
            ));

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }


    public <E> MessageBufferPacker packEvent(E event, long key, int uniEventPartitions) throws IOException{

        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
        packer
            .packMapHeader(2)
            .packString("__COM_TYPE__")
            .packString("RUN_FUN")
            .packString("__MSG__")
            .packMapHeader(5)
            .packString("__OP_NAME__");

        if(event instanceof Person){
            packer.packString("personsSource");
        }
        else if(event instanceof Auction){
            packer.packString("auctionsSource");
        }
        else{
            packer.packString("bidsSource");
        }
        
        packer
            .packString("__KEY__")
            .packLong(key)
            .packString("__FUN_NAME__")
            .packString("read")
            .packString("__PARAMS__");

        if(event instanceof Person){
            Person person = (Person) event; 
            packer
                .packArrayHeader(8)
                .packLong(person.id)
                .packString(person.name)
                .packString(person.emailAddress)
                .packString(person.creditCard)
                .packString(person.city)
                .packString(person.state)
                .packLong(person.dateTime)
                .packString(person.extra)
            ;
        }
        else if(event instanceof Auction){
            Auction auction = (Auction) event;
            packer
                .packArrayHeader(10)
                .packLong(auction.id)
                .packString(auction.itemName)
                .packString(auction.description)
                .packLong(auction.initialBid)
                .packLong(auction.reserve)
                .packLong(auction.dateTime)
                .packLong(auction.expires)
                .packLong(auction.seller)
                .packLong(auction.category)
                .packString(auction.extra)
            ;
        }
        else{
            Bid bid = (Bid) event;
            packer
                .packArrayHeader(5)
                .packLong(bid.auction)
                .packLong(bid.bidder)
                .packLong(bid.price)
                .packLong(bid.dateTime)
                .packString(bid.extra)
            ;
        }

        packer
            .packString("__PARTITION__")
            .packInt(uniEventPartitions);

        return packer;
    }

}
