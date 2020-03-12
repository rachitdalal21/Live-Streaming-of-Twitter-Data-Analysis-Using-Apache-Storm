
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

import twitter4j.*;

public class HashTagEmitBolt extends BaseRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;

    }

    @Override
    public void execute(Tuple tuple) {

        Status tweet = (Status) tuple.getValueByField("tweets");
        for(HashtagEntity hashtag : tweet.getHashtagEntities()) {
            if( hashtag != null && hashtag.getText().length() > 0 ) {
//System.out.println("Hashtag: " + hashtag.getText());
                this.collector.emit(new Values(hashtag.getText()));
            }
        }
        this.collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("hashTag"));

    }
}
