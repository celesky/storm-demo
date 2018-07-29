package storm.blueprints.chapter1.v1;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.util.Map;
import storm.blueprints.utils.Utils;

/**
 * 数据源,生产原始的sentence
 */
public class SentenceSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;

    private String[] sentences = {
        "my dog has fleas",
        "i like cold beverages",
        "the dog ate my homework",
        "don't have a cow man",
        "i don't think i like fleas"
    };
    private int index = 0;

    /**
     * spout和bolt都必须实现这个接口
     * 通过这个接口告诉storm 这个组件会发送的数据流中tuple的schema   数据结构
     * @param declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }

    /**
     * 所有spout组件 初始化时调用
     * @param config storm配置信息
     * @param context topology中组件信息
     * @param collector 这个对象提供了发射tuple的方法
     */
    @Override
    public void open(Map config, TopologyContext context, 
            SpoutOutputCollector collector) {
        this.collector = collector;
    }

    /**
     * nextTuple() 方法是所有 spout 实现的核心所在，
     * Storm 通过调用这个方法向输出的 collector 发射 tuple
     */
    @Override
    public void nextTuple() {


        this.collector.emit(new Values(sentences[index]));
        index++;
        if (index >= sentences.length) {
            index = 0;
        }
        Utils.waitForMillis(1);
    }
}
