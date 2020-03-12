import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.spi.LocaleNameProvider;
import java.util.stream.Collectors;


public class TweetCountBolt extends BaseRichBolt {
    private Map<String, ElementsDetails> dataMapPerBucket;
    /*private HashMap<String, Integer> finalCounts = null;*/
    private OutputCollector collector;
    private long intervalToWaitBeforeEmit = 10 ; // In Seconds
    //private long now;
    private long initialStartTime;
    private Map<String, Long> counter;
    private String fileName;
    PrintWriter writer;
    FileWriter fileWriter;
    DateFormat simple;
    private long count;

    TweetCountBolt( String fileName ) {
     this.fileName = fileName;
    }

    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.dataMapPerBucket = new ConcurrentHashMap<>();
        this.collector = outputCollector;
        initialStartTime = System.currentTimeMillis();
        counter = new TreeMap<>();
        simple = new SimpleDateFormat("dd MMM yyyy HH:mm:ss:SSS Z");
        count = 0;
        try {
            fileWriter = new FileWriter(fileName, true );
            writer = new PrintWriter(fileWriter, true);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void execute(Tuple tuple) {
//System.out.println("lasan ");
       // System.out.println("lasan 11 ");
        String hashTag = tuple.getStringByField("hashTag");
        long frequency =  tuple.getLongByField("frequency");
        int bucket =  tuple.getIntegerByField("bucket");
        //Long delta = Long.valueOf( tuple.getStringByField("delta") );

        /*ElementsDetails ele = new ElementsDetails();
        ele.frequency = frequency;
        ele.error = delta;*/
        //System.out.println("Newly Added  ");

        if( counter.containsKey( hashTag ) ) {
            frequency = frequency + counter.get(hashTag);
        }

        counter.put(hashTag, frequency);
//System.out.println("After Added Size  "+ counter.size());
        /*Integer count = finalCounts.get(tweet);
        if( count == null ) {
            count = 1;
        } else {
            count++;
        }
        this.finalCounts.put(tweet, count);*/
        long now = System.currentTimeMillis();
        long logPeriodSec = ( ( now - initialStartTime ) / 1000 );
        if( logPeriodSec >= intervalToWaitBeforeEmit ) {
//System.out.println("Inside Condition ");
          //  SortedMap<Long, String> top = new TreeMap<Long, String>();
            SortedMap< String, Long> top1 = new TreeMap< String, Long>();

            for (Map.Entry<String, Long> entry : counter.entrySet()) {
                long frq = entry.getValue();
                String tag = entry.getKey();

                /*if( top1.get(tag) != null ) {
                    frq = frq +
                }*/
                top1.put( tag, frq);
// System.out.println("Before Remove: size " + top1.size());


                /*if (top.size() > 100) {
                    top.remove(top.firstKey());
                }*/
            }
            Map<String, Long> top = top1.entrySet()
                    .stream()
                    .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                    .limit(100)
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            Map.Entry::getValue,
                            (e1, e2) -> e1,
                            LinkedHashMap::new
                    ));

            Date result = new Date(System.currentTimeMillis());


            if( top.size() > 0 ) {
                writer.print(" TimeStamp " + simple.format(result) + " " );
                for (Map.Entry<String, Long> entry : top.entrySet()) {
                    //for (Map.Entry<String, Long> entry : top.entrySet()) {
                    //   System.out.println(new StringBuilder(" top -  ").append(entry.getValue()).append(" | ").append(entry.getKey()).toString());
                    //System.out.println("Top 100: " + top.size() + " Value: " +   entry.getValue() + " TAGS: " + entry.getKey()  );

//System.out.println("bucket Id: "  + bucket + " TimeStamp " + simple.format(result) + " Hastag " + entry.getKey() + " freq " +  entry.getValue());
                    //  writer.print("bucket Id: "  + bucket + " TimeStamp " + simple.format(result) + " Hastag " + entry.getKey() + " freq " +  entry.getValue());
                    writer.print(" < " + entry.getKey() + " > " );
                    writer.flush();
                    // this.collector.emit(new Values(entry.getValue(), entry.getKey()));
                }
            } else {
                writer.print(" TimeStamp " + simple.format(result) + " " );
            }


            counter.clear();
            writer.println("\n \n");
//System.out.println("Cleared ");
            initialStartTime = System.currentTimeMillis();
        }

        /*now = System.currentTimeMillis();*/

        this.collector.ack(tuple);
    }


    /*public void cleanup() {
        System.out.println("----------Final In Report Bolt---------" + this.finalCounts.size());
        List<String> keys = new ArrayList<String>();
        keys.addAll(this.finalCounts.keySet());
        Collections.sort(keys);
        for( String word: keys ) {
            System.out.println(word + " : " +  this.finalCounts.get(word));
        }
    }*/

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
       // outputFieldsDeclarer.declare(new Fields("frequency", "tag"));
    }

    @Override
    public void cleanup() {
        writer.close();
        super.cleanup();
    }
}
