import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;


import org.apache.storm.utils.Utils;
import twitter4j.*;

import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private LinkedBlockingQueue<Status> queue = null;
    private TwitterStream twitterStream;
    private FilterQuery tweetFilterQuery;

    String consumerKey = "w5zQaVbDRndLECZPwmRP3kup4";
    String consumerSecretKey = "iBrIym9rWuSfaumDpo6MEyfkdkxhq4gPfUY5Ih5FqLTXiMjmsb";
    String accessToken = "2260215012-MOPvK7sd9bxE936WcITVURoX2w77mjY0CPcAFu9";
    String accessTokenSecretKey = "v6Rgh3vkYzfoyRw8OZFn1xKglNP7tC0dNLej4nUW5cFLy";

    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {

        this.collector = spoutOutputCollector;
        this.queue = new LinkedBlockingQueue<Status>(1000);

        StatusListener listener = new StatusListener() {

            @Override
            public void onException(Exception e) {

            }

            @Override
            public void onStatus(Status status) {
                queue.offer(status);

            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

            }

            @Override
            public void onTrackLimitationNotice(int i) {

            }

            @Override
            public void onScrubGeo(long l, long l1) {

            }

            @Override
            public void onStallWarning(StallWarning stallWarning) {

            }
        };

        twitterStream = new TwitterStreamFactory(
                new ConfigurationBuilder().setJSONStoreEnabled(true).build())
                .getInstance();
        twitterStream.addListener(listener);
        twitterStream.setOAuthConsumer(consumerKey,consumerSecretKey);
        AccessToken token = new AccessToken(accessToken, accessTokenSecretKey);
        twitterStream.setOAuthAccessToken(token);

        double [][] us_bounding_box = {{ -126.562500, 30.448674}, {-61.171875, 44.087585}};
        tweetFilterQuery = new FilterQuery().locations(us_bounding_box).language("en");
        twitterStream.filter(tweetFilterQuery);

    }

    @Override
    public void nextTuple() {
        Status ret = queue.poll();
        /*System.out.println("Test Emmit");*/
        if (ret == null) {
            Utils.sleep(50);
        } else {
            this.collector.emit(new Values(ret));
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("tweets"));
    }

    @Override
    public void close() {
        twitterStream.shutdown();
        super.close();
    }
}
