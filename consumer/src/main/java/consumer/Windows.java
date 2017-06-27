package consumer;

import java.util.*;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import java.io.*;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.apache.flink.streaming.api.TimeCharacteristic;



public class Windows  {

    // Some window parameters
    private final static int SPIDERSN  = 80;
    private final static long TUMBLINGW = 60L;
    private final static long SESSIONWS = 60L;

    public static void main(String[] args) throws Exception {

        // Set up the flink streaming environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Reading configureations
        JSONParser parser = new JSONParser();
        Object obj = parser.parse(new FileReader("../myconfigs.json"));
        JSONObject jsonObject =  (JSONObject) obj;
        String WORKERSIP = (String) jsonObject.get("WORKERS_IP");
        String MASTERIP  = (String) jsonObject.get("MASTER_IP");
        String TOPIC     = (String) jsonObject.get("TOPIC"); 
        String REDISIP = (String) jsonObject.get("REDIS_IP"); 

        // Kafka connector
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers",WORKERSIP);
        properties.setProperty("zookeeper.connect", MASTERIP);
        properties.setProperty("group.id", "test");
        FlinkKafkaConsumer09<String> kafkaSource = new FlinkKafkaConsumer09<>(TOPIC, new SimpleStringSchema(), properties);

        //Input string to tupleof 4: <ID, Time-in, Time-out, Count>
        //Adds the watermark: Here approximate that the data from Kafka arrives in ascending order which simplifies the coding.
        //Consider adding more sophisticated watermark function
        DataStream<Tuple4<String, Long, Long, Integer>> datain = env
            .addSource(kafkaSource)
            .flatMap(new LineSplitter())
            .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple4<String, Long, Long, Integer>>() {
                @Override
                public long extractAscendingTimestamp(Tuple4<String, Long, Long, Integer> element) {
                    return element.f1;
                }
        });


        // Calculate the number of clicks during the given period of time
        DataStream<Tuple4<String, Long, Long, Integer>> clickcount = datain
            .keyBy(0)
            .timeWindow(Time.seconds(TUMBLINGW))
            .reduce(new MyReducer());

        // If the number of clicks during the summed window (clickcount) is larger
        SplitStream<Tuple4<String, Long, Long, Integer>> detectspider = clickcount
            .split(new SpiderSelector());

        // Calculates the sessions...
        DataStream<Tuple4<String, Long, Long, Integer>> usersession = datain
            .keyBy(0)
            .window(EventTimeSessionWindows.withGap(Time.seconds(SESSIONWS)))
            .reduce (new MyReducer());

        // Configure the Redis
        FlinkJedisPoolConfig redisConf = new FlinkJedisPoolConfig.Builder().setHost(REDISIP).setPort(6379).build();

        // Sink data to Redis
        clickcount.addSink(new RedisSink<>(redisConf, new ViewerCountMapper()));
        detectspider
            .select("spider")
            .addSink(new RedisSink<>(redisConf, new SpidersIDMapper()));
        detectspider
            .select("spider")
            .addSink(new RedisSink<>(redisConf, new SpidersSortedMapper()));
        usersession.addSink(new RedisSink<>(redisConf, new EngagementMapper()));


        // Execute the Program 
        env.execute("Sessionization");
    }

    // figure out what does "serialVersionUID" means. Many codes include it but dont know why....
    public static class SpiderSelector implements OutputSelector<Tuple4<String, Long, Long, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Iterable<String> select(Tuple4<String, Long, Long, Integer> value) {
            List<String> output = new ArrayList<>();

            if (value.f3 > SPIDERSN) {
                output.add("spider");
            } else {
                output.add("legit");
            }
            return output;
        }
    }

    public static class LineSplitter implements FlatMapFunction<String, Tuple4<String, Long, Long, Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple4<String, Long, Long, Integer>> out) {
            String[] word = line.split(";");
            out.collect(new Tuple4<>(word[0], Long.parseLong(word[1]), Long.parseLong(word[1]), 1));
        }
    }

    public static class MyReducer implements ReduceFunction< Tuple4<String, Long, Long, Integer>> {

        public Tuple4<String, Long, Long, Integer> reduce(Tuple4<String, Long, Long, Integer> value1, Tuple4<String, Long, Long, Integer> value2) {
            return new Tuple4<>(value1.f0, value1.f1, value2.f1, value1.f3+value2.f3);
        }
    }

    public static class EngagementMapper implements RedisMapper<Tuple4<String, Long, Long, Integer>> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.ZADD, "EngagementTime");
        }

        @Override
        public String getKeyFromData(Tuple4<String, Long, Long, Integer> data) {
            return data.getField(0);
        }

        @Override
        public String getValueFromData(Tuple4<String, Long, Long, Integer> data) {
            Double stime =  ((Long) data.getField(1)).doubleValue();			
            Double etime =  ((Long) data.getField(2)).doubleValue();
            //fixme
            return Double.toString(Math.floor(etime-stime));
        }
    }

    public static class ViewerCountMapper implements RedisMapper<Tuple4<String, Long, Long, Integer>> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.ZADD, "ViewerCount");
        }

        @Override
        public String getKeyFromData(Tuple4<String, Long, Long, Integer> data) {
            return data.getField(0);
        }

        @Override
        public String getValueFromData(Tuple4<String, Long, Long, Integer> data) {
            return data.getField(3).toString();
        }
    }

    public static class SpidersIDMapper implements RedisMapper<Tuple4<String, Long, Long, Integer>>{

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "SPIDERS");
        }

        @Override
        public String getKeyFromData(Tuple4<String, Long, Long, Integer> data) {
            return data.f0;
        }

        @Override
        public String getValueFromData(Tuple4<String, Long, Long, Integer> data) {
            return (String) "True";
        }
    }

    public static class SpidersSortedMapper implements RedisMapper<Tuple4<String, Long, Long, Integer>>{

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.ZADD, "SORTEDSPIDERS");
        }

        @Override
        public String getKeyFromData(Tuple4<String, Long, Long, Integer> data) {
            return data.f0;
        }

        @Override
        public String getValueFromData(Tuple4<String, Long, Long, Integer> data) {
            return data.f2.toString();
        }
    }

}

