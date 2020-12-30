package bursty_event_detection.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Map;

// GroupingBolt的作用是构建共词矩阵，它需要把已经读过的文件再读一遍
// 这极有可能成为整个集群的性能瓶颈
// 因为我们已经修改过了热词的定义，所以在读文件的时候需要读包括date在内的三天的文件
public class GroupingBolt extends BaseRichBolt {
    private OutputCollector _collector;
    String FilePathTemplate = "NYTimes\\NYTimes_2020_11_";

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector outputCollector) {
        _collector = outputCollector;
    }

    @Override
    public void execute(Tuple input) {
        // System.out.println("GroupingBolt is running!");
        ArrayList<String> index = (ArrayList) input.getValue(0);
        ArrayList<ArrayList<Integer>> graph = (ArrayList) input.getValue(1);
        int date = input.getInteger(2);
        int size = index.size();
        for (int i = 0; i < 3; i++) {
            try {
                BufferedReader br = new BufferedReader(new FileReader(FilePathTemplate + (date - 2 + i) + ".txt"));
                String readLine;
                while ((readLine = br.readLine()) != null) {
                    // index中的单词两两一对看它们是否出现在了同一段落内，共需C(n, 2)次检查
                    String formattedReadLine = readLine.toLowerCase();
                    for (int j = 0; j < size; j++) {
                        for (int k = j + 1; k < size; k++) {
                            // 以段落为粒度聚合单词 or 以句子为粒度聚合单词 ?
                            if (formattedReadLine.contains(index.get(k)) && formattedReadLine.contains(index.get(k))) {
                                // 自增
                                graph.get(j).set(k, graph.get(j).get(k) + 1);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        _collector.emit(new Values(index, graph));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("index", "graph"));
    }
}
