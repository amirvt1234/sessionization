package wikiedits.Mappers;

import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.api.java.tuple.Tuple4;


public class ViewerCountMapper implements RedisMapper<Tuple4<String, Long, Long, Integer>>{

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
