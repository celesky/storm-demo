package storm.blueprints.chapter1.v1;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.Map;

/**
 * 实现语句分割
 */
public class SplitSentenceBolt extends BaseRichBolt{
    private OutputCollector collector;

    /**
     * 在 bolt 初始化时调用，可以用来准备 bolt 用到的资源，如数据库连接。
     * 和 SentenceSpout 类一样，SplitSentenceBolt 类在初始化时没有额外操作，
     * 因此 prepare() 方法仅仅保存 OutputCollector 对象的引用
     * @param config
     * @param context
     * @param collector
     */
    @Override
    public void prepare(Map config, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    /**
     * SplitSentenceBolt 类的核心功能在 execute() 方法中实现，
     * 这个方法是 IBolt 接口定义 的。每当从订阅的数据流中接收一个 tuple，都会调用这个方法
     *
     * 按照字符串读取“ sentence”字段的值，然后将其拆分为单词，每个单词向后面的输出流发 射一个 tuple
     * @param tuple
     */
    @Override
    public void execute(Tuple tuple) {
        String sentence = tuple.getStringByField("sentence");
        String[] words = sentence.split(" ");
        for(String word : words){
            // 可以通过调用 OutputCollector 中 emit() 的一个重载函数锚定一个或者一组 tuple:
            // 这里，我们将读入的 tuple 和发射的新 tuple 锚定起来，下游的 bolt 就需要对输出的 tuple 进行确认应答或者报错
            // this.collector.emit(tuple,new Values(word));
            this.collector.emit(new Values(word));
        }
    }

    /**
     * 在 declareOutputFields() 方法中，SplitSentenceBolt 声明了一个输出流，
     * 每个 tuple 包 含一个字段“ word ”
     * @param declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
