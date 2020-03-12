import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.util.Enumeration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


public class LossyCounting extends BaseRichBolt {
    private OutputCollector collector;
    private double e ;
    private double threshold ;
    private int bcurrent;
    private int bucketWidth ;
    private Map<String, ElementsDetails> dataMap;
    private int totalElmPerBucket;
   private long initialStartTime;

    private String fileName;
    PrintWriter writer;
    FileWriter fileWriter;

    public LossyCounting( String fileName) {
        this.e = 0.02;
        this.fileName = fileName;

    };

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.dataMap = new ConcurrentHashMap<>();
        this.bcurrent = 1;
        this.bucketWidth = (int) Math.ceil(1 / 0.01);
        this.threshold = 0.01; // s - e : s = 0.003, e = 0.002
        this.totalElmPerBucket = 0;
        initialStartTime = System.currentTimeMillis();

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

    @Override
    public void execute(Tuple tuple) {
        String hashTag = tuple.getStringByField("hashTag").toLowerCase();

        lossyCount( hashTag );

        long now = System.currentTimeMillis();
        int timeDiff = ( int ) ( now - initialStartTime ) / 1000 ;
        //if( timeDiff >= 10 ) {
            emitItems(this.dataMap);
          //  System.out.println("Emitted Elements size: " + this.dataMap.size() );
            initialStartTime = System.currentTimeMillis();
        //}

        this.collector.ack(tuple);
    }
    private synchronized void lossyCount(String hashTag ) {

        if( totalElmPerBucket < bucketWidth ) {
            if( this.dataMap.containsKey(hashTag) ) {
               // System.out.println("Element Again  " + hashTag );
                ElementsDetails tmp = this.dataMap.get(hashTag);
                tmp.increment();
                this.dataMap.put(hashTag, tmp);
            } else {
               //System.out.println("Element First Time  " + hashTag );
                ElementsDetails tmp = new ElementsDetails((long)( bcurrent - 1));
                /*tmp.frequency = 1;
                tmp.error = bcurrent - 1;*/
                this.dataMap.put(hashTag, tmp);
            }
            totalElmPerBucket = totalElmPerBucket + 1;
        }
        if( totalElmPerBucket == bucketWidth ) {
            //System.out.println( "Total Element in Bucket when Delete calls: " + totalElmPerBucket + " lossy totalElementsInBucket " + totalElementsInBucket );
            //System.out.println("Total Bucket Width: " + bucketWidth );
            Set keyset = this.dataMap.keySet();
            for( Object key : keyset ) {
                long frequency = this.dataMap.get(key).frequency;
                long error = this.dataMap.get(key).error;
                if( frequency + error <= (long) this.bcurrent ) {
                    /*writer.println("Removed Key:  "  + key + " Freq :  " + frequency + " Bucket  " + bcurrent + " Error + Freq  " + ( frequency + error ));
                    writer.flush();*/
                  //  System.out.println("Removed Key: "  + key + " Freq :  " + frequency + " Bucket  " + bcurrent + " Error + Freq  " + ( frequency + error ));
                    this.dataMap.remove(key);
                }

            }
//System.out.println("After Delete: " + this.dataMap.size());
            /*for( String key: this.dataMap.keySet() ) {

                ElementsDetails ele = this.dataMap.get(key);
                long frequency = ele.frequency;
                if( frequency >= ( threshold * totalElmPerBucket ) ) {

                    this.collector.emit(new Values(key, ele.frequency, ele.error));
                    System.out.println("Emitted Value from lossy counting");
                    System.out.println("KEY: " + key + " FREQ: " +  ele.frequency + " ERROR : " +  ele.error);
                }
            }*/
            //_emitItems(this.dataMap);

            this.bcurrent = this.bcurrent + 1;
            totalElmPerBucket = 0;
        }
    }

    /*private synchronized void _emitItems(Map<String, ElementsDetails> hashTagDetails) {*/
    private synchronized void emitItems(Map<String, ElementsDetails> hashTagDetails) {
        for( String key: hashTagDetails.keySet() ) {

            ElementsDetails ele = hashTagDetails.get(key);
            long frequency = ele.frequency;
            //System.out.println("Bucket Id :  "  + this.bcurrent + " threshold Multi: " + threshold * totalElmPerBucket + " Freq : " + frequency + " Totoal Ele " + totalElmPerBucket );
           if( frequency >= ( threshold * this.dataMap.size()  ) ) {

                this.collector.emit(new Values(key, ele.frequency, ele.error, this.bcurrent));
            //System.out.println("KEY: " + key + " FREQ: " +  ele.frequency + " ERROR : " +  ele.error + " bucket Id: " + this.bcurrent);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("hashTag", "frequency", "delta",  "bucket"));
    }
}
