package consumer;

import java.util.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.functions.TimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.Config;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.*;
import java.util.Iterator;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import org.apache.flink.api.java.utils.ParameterTool;



public class Windows  {

    public static String propertiesFile = "../myjob.properties";

    // Some window parameters
    public static int SPIDERSN  = 8; 
    public static int TUMBLINGW = 60;
    public static int SESSIONWS = 60;

    public static void main(String[] args) throws Exception {

        ParameterTool parameter = ParameterTool.fromPropertiesFile(propertiesFile);
        // Set up the flink streaming environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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
        DataStream<Tuple4<String, Long, Long, Integer>> datain = env
            .addSource(kafkaSource)
            .flatMap(new LineSplitter());

        // Calculate the number of clicks during the given period of time
        DataStream<Tuple4<String, Long, Long, Integer>> clickcount = datain
            .keyBy(0)
            .timeWindow(Time.seconds(TUMBLINGW))
            .reduce(new MyReducer());

        //The totalcount will perform on a single machine by nature: can be optimized ...
        DataStream<Tuple4<String, Long, Long, Integer>> totalcount = datain
            .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(TUMBLINGW)))
            .sum(3);

        // If the number of clicks during the summed window (clickcount) is larger
        SplitStream<Tuple4<String, Long, Long, Integer>> detectspider = clickcount
            .split(new SpiderSelector());

        //Adds the watermark: Here I assume that the data from Kafka arrives in ascending order which simplifies the coding.
        //Consider adding more sophisticated watermark function
        DataStream<Tuple4<String, Long, Long, Integer>> withTimestampsAndWatermarks =
            datain.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple4<String, Long, Long, Integer>>() {
                @Override
                public long extractAscendingTimestamp(Tuple4<String, Long, Long, Integer> element) {
                    return element.f1;
                }
        });

        // Calculates the sessions...
        DataStream<Tuple4<String, Long, Long, Integer>> usersession = withTimestampsAndWatermarks
            .keyBy(0)
            .window(ProcessingTimeSessionWindows.withGap(Time.seconds(SESSIONWS)))
            .reduce (new MyReducer());

        // Configure the Redis
        FlinkJedisPoolConfig redisConf = new FlinkJedisPoolConfig.Builder().setHost(REDISIP).setPort(6379).build();

        // Sink data to Redis
        clickcount.addSink(new RedisSink<Tuple4<String, Long, Long, Integer>>(redisConf, new ViewerCountMapper()));
        totalcount.addSink(new RedisSink<Tuple4<String, Long, Long, Integer>>(redisConf, new TotalCountMapper()));
        detectspider
            .select("spider")
            .addSink(new RedisSink<Tuple4<String, Long, Long, Integer>>(redisConf, new SpidersIDMapper()));
        usersession.addSink(new RedisSink<Tuple4<String, Long, Long, Integer>>(redisConf, new EngagementMapper()));


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
            out.collect(new Tuple4<String, Long, Long, Integer>(word[0], Long.parseLong(word[1]), Long.parseLong(word[1]), 1));
        }
    }

    public static class MyReducer
            implements ReduceFunction< Tuple4<String, Long, Long, Integer>> {

        public Tuple4<String, Long, Long, Integer> reduce(Tuple4<String, Long, Long, Integer> value1, Tuple4<String, Long, Long, Integer> value2) {
            return new Tuple4<String, Long, Long, Integer>(value1.f0, value1.f1, value2.f1, value1.f3+value2.f3);
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

	public static class TotalCountMapper implements RedisMapper<Tuple4<String, Long, Long, Integer>>{

		@Override
		public RedisCommandDescription getCommandDescription() {
		return new RedisCommandDescription(RedisCommand.HSET, "TOTAL_COUNT");
		}

		@Override
		public String getKeyFromData(Tuple4<String, Long, Long, Integer> data) {
		return (String) "totalcount";
		}

		@Override
		public String getValueFromData(Tuple4<String, Long, Long, Integer> data) {
		return data.f3.toString();
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

}

