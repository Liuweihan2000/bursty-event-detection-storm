package bursty_event_detection.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// GraphBuilderBolt接收PrintBolt传来的热词和其出现日期，构建一个热词组成的二维数组
// GraphBuilderBolt在集群中会有多个实例，所以在接收PrintBolt传过来的键值对的时候
// 需要保证每个GraphBuilderBolt得到的tuple的日期是一样的
// 每个GraphBuilderBolt中各自维护的curDate可能会出现不一致
// 注意到GraphBuilderBolt得到的date信息是从PrintBolt传来的，而PrintBolt的date信息是从WordCountBolt传来的
// 这个date是比真正的date信息要超前一天的，所以在往下传递的时候需要将date--
public class GraphBuilderBolt extends BaseRichBolt {
    private OutputCollector _collector;
    private List<List<Integer>> graph;
    private List<String> wordBag;
    private int curDate;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector outputCollector) {
        _collector = outputCollector;
        wordBag = new ArrayList<>();
        curDate = -1;
    }

    @Override
    public void execute(Tuple input) {
        String str = input.getString(0);
        int date = input.getInteger(1);
        if (date == curDate) {
            wordBag.add(str);
        } else if (curDate == -1) {
            curDate = date;
        } else { // date != curDate && curDate != -1，说明是新的一天
            int size = wordBag.size();
            graph = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                ArrayList<Integer> temp = new ArrayList<>();
                for (int j = 0; j < size; j++) {
                    temp.add(0);
                }
                graph.add(temp);
            }
            _collector.emit(new Values(wordBag, graph, date - 1));
            curDate = date;
            // 新的一天到来了，所以需要把前一天得到的graph丢弃
            wordBag = new ArrayList<>();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("index", "graph", "curDate"));
    }
}
