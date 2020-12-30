package bursty_event_detection.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

public class PrintBolt extends BaseRichBolt {

    private OutputCollector _collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector outputCollector) {
        _collector = outputCollector;
    }

    @Override
    public void execute(Tuple input) {
        String str = input.getString(0);
        int rate = input.getInteger(1);
        int date = input.getInteger(2);

        System.out.println("day" + date + ": [" + str + " seems to have a high occurence rate! | "+ "current rate: " + rate + ']');
        _collector.emit(new Values(str, date));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "date"));
    }
}
