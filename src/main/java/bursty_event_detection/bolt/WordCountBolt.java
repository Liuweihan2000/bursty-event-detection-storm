package bursty_event_detection.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import bursty_event_detection.util.AnalysePrefix;

import java.util.*;

public class WordCountBolt implements IRichBolt {

    Map<String, ArrayList<Integer>> map;
    private OutputCollector _collector;

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        map = new HashMap<>();
    }

	public void execute(Tuple input) {
	    String str = input.getString(0);
	    int date = input.getInteger(1);

        ArrayList<Integer> list;
        if (map.get(str) == null) {
            list = new ArrayList<>();
            for (int i = 0; i < 30; i++) {
                list.add(0);
            }
            if (date >= list.size()) return;
            list.set(date, 1);
        } else {
            list = map.get(str);
            if (date >= list.size()) return;
            if (list.get(date) == 0) { // 说明是新的一天
                if (AnalysePrefix.isTrending(list, date - 1)) {
                    // 说明在该date的前3天中这个单词称为了一个热词，但不包含date本身
                    _collector.emit(new Values(str, list.get(date - 1) + list.get(date - 2) + list.get(date - 3), date));
                }
            }
            // 自增
            list.set(date, list.get(date) + 1);
            map.remove(str);
        }

        map.put(str, list);
    }

    @Override
    public void cleanup() {}

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "current", "date"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}