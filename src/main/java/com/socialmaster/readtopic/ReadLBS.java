package com.socialmaster.readtopic;

import java.util.Arrays;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy.TimeUnit;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

public class ReadLBS {
    public static class KafkaToRecoderBolt extends BaseRichBolt {
        private static final Log LOG = LogFactory.getLog(KafkaToRecoderBolt.class);
        private static final long serialVersionUID = -5207232012035109026L;
        private OutputCollector collector;

        public void prepare(Map stormConf, TopologyContext context,
                            OutputCollector collector) {
            this.collector = collector;
        }

        public void execute(Tuple input) {
            String line = input.getString(0).trim();
            String newLine = "", head = "", body = "", tail = "";
            //LOG.info("RECV[kafka -> splitter] " + line);

            if (line.length()>0) {
                int pos1 = line.indexOf("{");
                int pos2 = line.indexOf("}");
                if ((pos2 > pos1) && (pos1 > 0)) {
                    head = line.substring(0, pos1);
                    body = line.substring(pos1 + 1, pos2);
                    tail = line.substring(pos2 + 1);

                    //把多条流量数据拆分开
                    String[] arrItem = body.split(";");
                    int len = arrItem.length;
                    for (int i = 0; i < len; i++) {
                        newLine = head + arrItem[i] + tail;
                        //LOG.info("EMIT[splitter -> newLine] " + newLine);
                        collector.emit(input, new Values(newLine));
                    }
                }
            }
            collector.ack(input);
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("line"));
        }

    }

    public static class RealtimeBolt extends BaseRichBolt {
        private static final Log LOG = LogFactory.getLog(RealtimeBolt.class);
        private static final long serialVersionUID = -4115132557403913367L;
        private OutputCollector collector;

        public void prepare(Map stormConf, TopologyContext context,
                            OutputCollector collector) {
            this.collector = collector;
        }

        public void execute(Tuple input) {
            String line = input.getString(0).trim();
            LOG.info("REALTIME: " + line);
            collector.ack(input);
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }

    }

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException {
        // Configure Kafka
        String zks = "ZK01:2181,ZK02:2181,ZK03:2181";
        String topic = "lbs";
        String zkRoot = "/storm"; // default zookeeper root configuration for storm
        String id = "read-lbs";
        BrokerHosts brokerHosts = new ZkHosts(zks, "/kafka/brokers");
        SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, id);
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConf.forceFromStart = false;
        spoutConf.zkServers = Arrays.asList(new String[] {"ZK01", "ZK02", "ZK03"});
        spoutConf.zkPort = 2181;

        // Configure HDFS bolt
        RecordFormat format = new DelimitedRecordFormat()
                .withFieldDelimiter(",");
        SyncPolicy syncPolicy = new CountSyncPolicy(1000);
        FileRotationPolicy rotationPolicy = new TimedRotationPolicy(1.0f, TimeUnit.DAYS);
        FileNameFormat fileNameFormat = new MyFileNameFormat()
                .withPath("/data/lbs").withPrefix("lbs_").withExtension(".log");
        HdfsBolt hdfsBolt = new HdfsBolt()
                .withFsUrl("hdfs://master:8020")
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy);

        // configure & build topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-reader", new KafkaSpout(spoutConf), 1);
        builder.setBolt("to-recoder", new KafkaToRecoderBolt(), 1).shuffleGrouping("kafka-reader");
        builder.setBolt("hdfs-bolt", hdfsBolt, 1).shuffleGrouping("to-recoder");
        //builder.setBolt("realtime", new RealtimeBolt(), 2).shuffleGrouping("to-recoder");

        // submit topology
        Config conf = new Config();
        String name = ReadLBS.class.getSimpleName();
        if (args != null && args.length > 0) {
            String nimbus = args[0];
            conf.put(Config.NIMBUS_HOST, nimbus);
            conf.setNumWorkers(2);  // set storm supervisor number
            StormSubmitter.submitTopologyWithProgressBar(name, conf, builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(name, conf, builder.createTopology());
            Thread.sleep(60000);
            cluster.shutdown();
        }
    }
}
