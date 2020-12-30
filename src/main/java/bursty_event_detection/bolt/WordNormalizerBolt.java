package bursty_event_detection.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

// 将消息标准化
public class WordNormalizerBolt extends BaseRichBolt {

    private OutputCollector _collector;
    private Set<String> wordBag;

    @Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector outputCollector) {
        _collector = outputCollector;
        intializeWordBag();
    }

    // 执行订阅的Tuple逻辑过程的方法
    @Override
    public void execute(Tuple tuple) {
        String sentence = tuple.getString(0);
        int date = tuple.getInteger(1);
        String[] words = sentence.split(" ");

        for (String word : words) {
            String lowerCase = word.toLowerCase();
            for (int i = 0; i < lowerCase.length(); i++) {
                char target = lowerCase.charAt(i);
                if (target == '“' || target == '”' || target == '–') { // 不规范的字符
                    return;
                }
            }

            // 常见的单词没有统计的意义
            if (wordBag.contains(lowerCase) || lowerCase.length() <= 1) return;
            _collector.emit(new Values(lowerCase, date));
        }

    }

    private void intializeWordBag() {
        wordBag = new HashSet<>();
        wordBag.add("last");
        wordBag.add("where");
        wordBag.add("because");
        wordBag.add("make");
        wordBag.add("my");
        wordBag.add("even");
        wordBag.add("only");
        wordBag.add("back");
        wordBag.add("out");
        wordBag.add("would");
        wordBag.add("once");
        wordBag.add("any");
        wordBag.add("which");
        wordBag.add("week");
        wordBag.add("year");
        wordBag.add("weeks");
        wordBag.add("its");
        wordBag.add("a");
        wordBag.add("the");
        wordBag.add("one");
        wordBag.add("A");
        wordBag.add("and");
        wordBag.add("to");
        wordBag.add("of");
        wordBag.add("you");
        wordBag.add("me");
        wordBag.add("is");
        wordBag.add("that");
        wordBag.add("That");
        wordBag.add("new");
        wordBag.add("The");
        wordBag.add("in");
        wordBag.add("for");
        wordBag.add("");
        wordBag.add(" ");
        wordBag.add("on");
        wordBag.add("his");
        wordBag.add("with");
        wordBag.add("as");
        wordBag.add("CNN");
        wordBag.add("by");
        wordBag.add("her");
        wordBag.add("their");
        wordBag.add("they");
        wordBag.add("will");
        wordBag.add("shall");
        wordBag.add("was");
        wordBag.add("from");
        wordBag.add("be");
        wordBag.add("--");
        wordBag.add("said");
        wordBag.add("he");
        wordBag.add("has");
        wordBag.add("at");
        wordBag.add("s");
        wordBag.add("an");
        wordBag.add("not");
        wordBag.add("it");
        wordBag.add("\"");
        wordBag.add("who");
        wordBag.add("this");
        wordBag.add("This");
        wordBag.add("are");
        wordBag.add("can");
        wordBag.add("do");
        wordBag.add("there");
        wordBag.add("+");
        wordBag.add("up");
        wordBag.add("when");
        wordBag.add("how");
        wordBag.add("it's");
        wordBag.add("while");
        wordBag.add("\"the");
        wordBag.add("if");
        wordBag.add("like");
        wordBag.add("we");
        wordBag.add("no");
        wordBag.add("into");
        wordBag.add("more");
        wordBag.add("than");
        wordBag.add("now");
        wordBag.add("had");
        wordBag.add("came");
        wordBag.add("also");
        wordBag.add("come");
        wordBag.add("were");
        wordBag.add("she");
        wordBag.add("many");
        wordBag.add("but");
        wordBag.add("may");
        wordBag.add("us");
        wordBag.add("here");
        wordBag.add("some");
        wordBag.add("com");
        wordBag.add("all");
        wordBag.add("other");
        wordBag.add("says");
        wordBag.add("&nbsp");
        wordBag.add("most");
        wordBag.add("should");
        wordBag.add("dr");
        wordBag.add("been");
        wordBag.add("000");
        wordBag.add("per");
        wordBag.add("say");
        wordBag.add("told");
        wordBag.add("what");
        wordBag.add("these");
        wordBag.add("another");
        wordBag.add("it’s");
        wordBag.add("about");
        wordBag.add("though");
        wordBag.add("then");
        wordBag.add("our");
        wordBag.add("five");
        wordBag.add("before");
        wordBag.add("during");
        wordBag.add("two");
        wordBag.add("have");
        wordBag.add("after");
        wordBag.add("so");
        wordBag.add("every");
        wordBag.add("those");
        wordBag.add("over");
        wordBag.add("still");
        wordBag.add("yet");
        wordBag.add("through");
        wordBag.add("ms");
        wordBag.add("mr");
        wordBag.add("such");
        wordBag.add("’s");
        wordBag.add("did");
        wordBag.add("or");
        wordBag.add("why");
        wordBag.add("just");
        wordBag.add("‘s");
        for (int i = 0; i <= 100; i++) {
            wordBag.add(Integer.toString(i));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "date"));
    }

}
