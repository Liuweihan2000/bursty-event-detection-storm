package bursty_event_detection.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

public class SentencePartionBolt extends BaseRichBolt {

    private OutputCollector _collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String line = tuple.getString(0);
        int date = tuple.getInteger(1);
        String[] sentences = line.split("\\.|,");

        for (String sentence : sentences) {
            _collector.emit(new Values(sentence, date));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word", "date"));
    }
}
