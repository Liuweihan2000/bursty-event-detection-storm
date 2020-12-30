package bursty_event_detection.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Map;

public class FileReaderSpout extends BaseRichSpout {

	SpoutOutputCollector _collector;
	String FilePathTemplate = "NYTimes\\NYTimes_2020_11_";

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
	}

	// spout是运行在Worker进程中的Executor中的某个Task中。Task会负责不断的无限轮训调用该方法，形成数据流
	@Override
	public void nextTuple() {

		// 一共 30 天
		for (int i = 1; i <= 30; i++) {
			try {
				BufferedReader br = new BufferedReader(new FileReader(FilePathTemplate + i + ".txt"));
				String readLine = null;
				while ((readLine = br.readLine()) != null) {
					_collector.emit(new Values(readLine, i));
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "date"));
	}

}