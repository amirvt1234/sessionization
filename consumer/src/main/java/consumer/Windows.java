package consumer;


import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import java.util.*;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.functions.TimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;


import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;

import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;

//import org.apache.flink.api.java.tuple.Tuple7;


//import wikiedits.Mappers.*;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import com.typesafe.config.ConfigFactory;
import com.typesafe.config.Config;


//import org.json.JSONObject;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;



public class Windows {





    public static void main(String[] args) throws Exception {

		// Set up the flink streaming environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// Reading configureations
		JSONParser parser = new JSONParser();
		String WORKERSIP="", MASTERIP="", TOPIC="";
		try {
			//Object obj = parser.parse(new FileReader("/home/ubuntu/project/sessionization/myconfigs.json"));
			Object obj = parser.parse(new FileReader("../myconfigs.json"));
			JSONObject jsonObject =  (JSONObject) obj;
			WORKERSIP = (String) jsonObject.get("WORKERS_IP");
			MASTERIP  = (String) jsonObject.get("MASTER_IP");
			TOPIC     = (String) jsonObject.get("TOPIC");
		} catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }




		// Kafka connector
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers",WORKERSIP);
		properties.setProperty("zookeeper.connect", MASTERIP);
        properties.setProperty("group.id", "test");

        FlinkKafkaConsumer09<String> kafkaSource = new FlinkKafkaConsumer09<>(TOPIC, new SimpleStringSchema(), properties);


        DataStream<Tuple4<String, Long, Long, Integer>> datain = env
	        .addSource(kafkaSource)
            .flatMap(new LineSplitter());


        DataStream<Tuple4<String, Long, Long, Integer>> withTimestampsAndWatermarks =
            datain.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple4<String, Long, Long, Integer>>() {

                @Override
                public long extractAscendingTimestamp(Tuple4<String, Long, Long, Integer> element) {
                    return element.f1;
                }
        });




        DataStream<Tuple4<String, Long, Long, Integer>> clickcount = datain
            .keyBy(0)
            .timeWindow(Time.seconds(10))
            .sum(3);






/*
		SplitStream<Tuple4<String, Long, Long, Integer>> detectspider = clickcount
				.split(new MySelector());
		DataStream<Tuple4<String, Long, Long, Integer>> spiders = detectspider.select("spider");





        DataStream<Tuple4<String, Long, Long, Integer>> testsession = withTimestampsAndWatermarks
            .keyBy(0)
            //.window(EventTimeSessionWindows.withGap(Time.milliseconds(2L)))
            //.emitWatermark(new Watermark(Long.MAX_VALUE))
            .window(ProcessingTimeSessionWindows.withGap(Time.seconds(1)))
            //.sum(2);
            .reduce (new ReduceFunction<Tuple4<String, Long, Long, Integer>>() {
                public Tuple4<String, Long, Long, Integer> reduce(Tuple4<String, Long, Long, Integer> value1, Tuple4<String, Long, Long, Integer> value2) throws Exception {
                    return new Tuple4<String, Long, Long, Integer>(value1.f0, value1.f1, value2.f1, value1.f3+value2.f3);
                }
            });



        FlinkJedisPoolConfig redisConf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").setPort(6379).build();

		spiders.addSink(new RedisSink<Tuple4<String, Long, Long, Integer>>(redisConf, new ViewerCountMapper()));
*/


	FlinkJedisPoolConfig redisConf = new FlinkJedisPoolConfig.Builder().setHost("172.31.53.147").setPort(6379).build();
//        FlinkJedisPoolConfig redisConf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").setPort(6379).build();
	clickcount.addSink(new RedisSink<Tuple4<String, Long, Long, Integer>>(redisConf, new ViewerCountMapper()));
        clickcount.print();
        //testsession.print();
	//	spiders.print();


        env.execute("Window WordCount");


    }

	// figure out what does "serialVersionUID" means....
	public static class MySelector implements OutputSelector<Tuple4<String, Long, Long, Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Iterable<String> select(Tuple4<String, Long, Long, Integer> value) {
			List<String> output = new ArrayList<>();
			if (value.f3 > 3) {
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


	public static class ViewerCountMapper implements RedisMapper<Tuple4<String, Long, Long, Integer>>{

		@Override
		public RedisCommandDescription getCommandDescription() {
		return new RedisCommandDescription(RedisCommand.HSET, "HASH_NAME");
		}

		@Override
		public String getKeyFromData(Tuple4<String, Long, Long, Integer> data) {
		return data.f0;
		}

		@Override
		public String getValueFromData(Tuple4<String, Long, Long, Integer> data) {
		return data.f1.toString();
		}
	}


	/*
	public class ViewerCountMapper implements RedisMapper<Tuple4<String, Long, Long, Integer>> {

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
	*/




//        FlinkKafkaProducer09<String> myProducer = new FlinkKafkaProducer09<String>(
//                "localhost:9092",            // broker list
//                "jtestc",                  // target topic
//                new SimpleStringSchema());   // serialization schema

//		        StringKafka.addSink(myProducer);
//        DataStream<String> StringKafka = aggregated.map(new TupleToStr());

/*
    public static class TupleToStr implements MapFunction<Tuple3<String, Long, Integer>, String> {
      @Override
      public String map(Tuple3<String, Long, Integer> in) {
        return in.f0 + ';' + in.f1.toString() + ';' + in.f2.toString();
      }
    }
*/

}






/*

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.api.common.functions.MapFunction;

//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
//import org.apache.flink.streaming.api.windowing.time.Time;
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
//import java.text.SimpleDateFormat;
import java.util.*;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
//import org.json.JSONObject;
//import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
//import com.datastax.driver.core.Cluster;
//import com.typesafe.config.ConfigFactory;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


//////////
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
//import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
//import org.apache.flink.streaming.api.windowing.assigners;
import org.apache.flink.streaming.api.windowing.time.Time;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.api.java.functions.KeySelector;

import org.apache.flink.api.common.functions.ReduceFunction;
//////////


import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;


//import com.typesafe.config.Config;
//import org.apache.flink.api.common.functions.FlatMapFunction;
//import org.apache.flink.api.common.functions.FoldFunction;
//import org.apache.flink.api.java.functions.KeySelector;
//import org.apache.flink.api.java.tuple.Tuple4;
//import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
//import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
//import org.apache.flink.streaming.connectors.redis.RedisSink;
//import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
//import org.apache.flink.util.Collector;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
//import java.text.SimpleDateFormat;
import java.util.*;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
//import org.json.JSONObject;
//import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
//import com.datastax.driver.core.Cluster;
//import com.typesafe.config.ConfigFactory;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;

import org.apache.flink.streaming.api.functions.TimestampExtractor;

//import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;

import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;


import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;




public class WikipediaAnalysis {

  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "ec2-52-203-199-182.compute-1.amazonaws.com:9092");
        // only required for Kafka 0.8
    properties.setProperty("zookeeper.connect", "ec2-52-203-199-182.compute-1.amazonaws.com:2181");
    properties.setProperty("group.id", "test");
        //DataStream<String> dataStream = env
    DataStream<Tuple4<String, Long, Long, Integer>> datain = env
        .addSource(new FlinkKafkaConsumer09<>("jtest", new SimpleStringSchema(), properties))
        .flatMap(new LineSplitter());

    DataStream<Tuple4<String, Long, Long, Integer>> clickcount = datain
        .keyBy(0)
        .timeWindow(Time.seconds(1))
        .sum(3);

	DataStream<Tuple4<String, Long, Long, Integer>> usersession = datain
				.window(EventTimeSessionWindows.withGap(Time.milliseconds(60000L)));
//                .reduce (new ReduceFunction<Tuple4<String, Long, Long, Integer>>() {
//                    public Tuple4<String, Long, Long, Integer> reduce(Tuple4<String, Long, Long, Integer> value1, Tuple4<String, Long, Long, Integer> value2) throws Exception {
//                        return new Tuple4<String, Long, Long, Integer>(value1.f0, value1.f1, value2.f1, value1.f3+value2.f3);
//                    }
//                });

	usersession.print();
	env.execute("Window WordCount");
  }

    public static class LineSplitter implements FlatMapFunction<String, Tuple4<String, Long, Long, Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple4<String, Long, Long, Integer>> out) {
            String[] word = line.split(";");
            out.collect(new Tuple4<String, Long, Long, Integer>(word[0], Long.parseLong(word[1]), Long.parseLong(word[1]), 1));
        }
    }

}

*/
