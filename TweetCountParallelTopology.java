import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class TweetCountParallelTopology {

    private static final String TWEET_SPOUT_ID = "tweet-spout";
    private static final String HASH_TAG_BOLT_ID = "hash-tab-bolt";
    private static final String COUNT_BOLT_ID = "hash-tag-count-bolt";
    private static final String LOSSY_COUNT_BOLT = "lossy-count-bolt";
    private static final String REPORT_BOLT_ID = "report-bolt";
    private static final String TOPOLOGY_NAME = "hash-tag-topology";


    public static void main( String args[]) throws Exception {
        TwitterSpout tspout = new TwitterSpout();
        HashTagEmitBolt hbolt = new HashTagEmitBolt();
        LossyCounting lca = new LossyCounting(args[2]);
        TweetCountBolt tcount = new TweetCountBolt(args[1]);
/*LossyCounting lca = new LossyCounting(args[1]);
        TweetCountBolt tcount = new TweetCountBolt(args[0]);*/



        TopologyBuilder builder  = new TopologyBuilder();

        builder.setSpout(TWEET_SPOUT_ID, tspout);
        builder.setBolt(HASH_TAG_BOLT_ID, hbolt).shuffleGrouping(TWEET_SPOUT_ID);
        builder.setBolt(LOSSY_COUNT_BOLT, lca, 4).fieldsGrouping(HASH_TAG_BOLT_ID, new Fields("hashTag") );
        builder.setBolt(COUNT_BOLT_ID, tcount).globalGrouping(LOSSY_COUNT_BOLT);

        // builder.setBolt(REPORT_BOLT_ID, reportBolt).globalGrouping(COUNT_BOLT_ID);
        Config conf = new Config();
        conf.setNumWorkers(4);
/*LocalCluster lc = new LocalCluster();
        lc.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());
        Thread.sleep(200000);
        lc.killTopology(TOPOLOGY_NAME);
        lc.shutdown(); */



        StormSubmitter.submitTopology(args[0],
                conf, builder.createTopology());
    }
}
