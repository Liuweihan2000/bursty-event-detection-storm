package bursty_event_detection.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

// GroupingBolt2得到了GroupingBolt传来的共词矩阵和index数组，可以开始聚类了
// 采用绝对聚类的方式
public class GroupingBolt2 extends BaseRichBolt {
    private OutputCollector _collector;
    private int GRANULARITY = 300;
    private final int INITIALGL = 300;
    private int DELTA = 100;
    private final int INITIALDELTA = 100;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector outputCollector) {
        _collector = outputCollector;
    }

    @Override
    public void execute(Tuple input) {
        ArrayList<String> index = (ArrayList) input.getValue(0);
        ArrayList<ArrayList<Integer>> graph = (ArrayList) input.getValue(1);
        ArrayList<Set<String>> result = new ArrayList<>();
        Set<String> curSet = new HashSet<>();
        int size = index.size();
        boolean[] visited = new boolean[size];

        for (int i = 0; i < size; i++) {
            for (int j = i + 1; j < size; j++) {
                // 找到一对凝结核
                if (graph.get(i).get(j) >= GRANULARITY && !visited[i] && !visited[j]) {
                    visited[i] = true;
                    visited[j] = true;
                    curSet.add(index.get(i));
                    curSet.add(index.get(j));
                    condense(graph, visited, curSet, index); // 贪心算法
                    result.add(curSet);
                    curSet = new HashSet<>();
                    if (GRANULARITY > 30) {
                        declineGranularity();
                    }
                }
            }
        }

        for (Set<String> set : result) {
            if (set.size() > 3) {
                System.out.print("Event detected: [");
                for (String word : set) {
                    System.out.print(word + " ");
                }
                System.out.println("]");
            }
        }

        reset();
    }

    private void declineGranularity() {
        this.GRANULARITY -= this.DELTA;
        this.DELTA = (this.DELTA * 2) / 3;
    }

    private void reset() {
        this.GRANULARITY = this.INITIALGL;
        this.DELTA = this.INITIALDELTA;
    }

    private int getIndex(ArrayList<String> index, String target) {
        int i = 0;
        while (i < index.size() && !target.equals(index.get(i))) i++;
        if (i == index.size()) return -1;
        return i;
    }

    private void condense(ArrayList<ArrayList<Integer>> graph, boolean[] visited, Set<String> set, ArrayList<String> index) {
        int size = graph.size();
        Set<Integer> indexSet = new HashSet<>();
        for (String s : set) {
            indexSet.add(getIndex(index, s));
        }

        int j = 0;
        while (j < size) {
            if (indexSet.contains(j) || visited[j]) {
                j++;
                continue;
            }
            boolean flag = true;
            for (int id : indexSet) {
                if (id > j) {
                    if (graph.get(j).get(id) >= GRANULARITY) {
                        continue;
                    } else {
                        flag = false;
                        break;
                    }
                } else if (id < j) {
                    if (graph.get(id).get(j) >= GRANULARITY) {
                        continue;
                    } else {
                        flag = false;
                        break;
                    }
                } else { // id == j;
                    continue;
                }
            }
            if (flag) { // successfully condensed a new element
                set.add(index.get(j));
                indexSet.add(j);
                for (int id : indexSet) {
                    visited[id] = true;
                }
            }
            j++;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
