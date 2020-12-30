package bursty_event_detection;

import backtype.storm.tuple.Fields;
import bursty_event_detection.bolt.*;
import bursty_event_detection.spout.FileReaderSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;


public class WordCountTopology {

    private static TopologyBuilder builder = new TopologyBuilder();

    public static void main(String[] args) {

        Config config = new Config();

        builder.setSpout("RandomSentence", new FileReaderSpout(), 1);
        builder.setBolt("SentencePartition", new SentencePartionBolt(), 1).shuffleGrouping("RandomSentence");
        builder.setBolt("WordNormalizer", new WordNormalizerBolt(), 1).shuffleGrouping("SentencePartition");
        builder.setBolt("WordCount", new WordCountBolt(), 1).fieldsGrouping("WordNormalizer", new Fields("word"));
        builder.setBolt("Print", new PrintBolt(), 1).shuffleGrouping("WordCount");
        builder.setBolt("GraphBuilder", new GraphBuilderBolt(), 1).fieldsGrouping("Print", new Fields("date"));
        builder.setBolt("GroupingBolt", new GroupingBolt(), 1).shuffleGrouping("GraphBuilder");
        builder.setBolt("GroupingBolt2", new GroupingBolt2(), 1).shuffleGrouping("GroupingBolt");

        config.setDebug(false);

        // 通过是否有参数来控制是否启动集群，或者本地模式执行
        if (args != null && args.length > 0) {
            try {
                config.setNumWorkers(1);
                StormSubmitter.submitTopology(args[0], config,
                        builder.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            config.setMaxTaskParallelism(1);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wordcount", config, builder.createTopology());
        }
    }
}
