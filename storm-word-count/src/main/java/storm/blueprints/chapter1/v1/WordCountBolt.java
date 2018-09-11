package storm.blueprints.chapter1.v1;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.HashMap;
import java.util.Map;

/**
 *  topology 中实际进行单词计数的组件
 */
public class WordCountBolt extends BaseRichBolt{
    private OutputCollector collector;
    private HashMap<String, Long> counts = null;

    /**
     * 实例化了一个 HashMap<String，Long> 的实例，用来存储单词和对应的 计数
     * 大部分实例变量通常是在 prepare() 方法中进行实例化，
     * 这个设计模式是由 topology 的部署方式决定的
     *
     * 当 topology 发布时，所有的 bolt 和 spout 组件首先会进行序列化，然后通过网络发送到集群中。
     * 如果 spout 或者 bolt 在序列化之前(比如说在构造函数中生成) 实例化了任何无法序列化的实例变量，
     * 在进行序列化时会抛出 NotSerializableException 异 常，topology 就会部署失败
     *
     * 通常情况下最好是在构造函数中对基本数据类型 和可序列化的对象进行赋值和实例化，
     * 在 prepare() 方法中对不可序列化的对象进行实例化
     * @param config
     * @param context
     * @param collector
     */
    @Override
    public void prepare(Map config, TopologyContext context, 
            OutputCollector collector) {
        this.collector = collector;
        this.counts = new HashMap<String, Long>();
    }

    /**
     * 递增并存储计数，然后将单词和最新 计数作为 tuple 向后发射。
     * 将单词计数作为数据流发射，topology 中的其他 bolt 就可以订
     * 阅这个数据流进行进一步的处理
     * @param tuple
     */
    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        Long count = this.counts.get(word);
        if(count == null){
            count = 0L;
        }
        count++;
        this.counts.put(word, count);
        this.collector.emit(new Values(word, count));
        // 如果是可靠数据流 需要像上游报告ack
        // this.collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }
}
