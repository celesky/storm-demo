package storm.blueprints.chapter1.v1;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 对所有单词的计数生成一份报告
 */
public class ReportBolt extends BaseRichBolt {

    private HashMap<String, Long> counts = null;

    @Override
    public void prepare(Map config, TopologyContext context, OutputCollector collector) {
        this.counts = new HashMap<String, Long>();
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        Long count = tuple.getLongByField("count");
        this.counts.put(word, count);
    }

    /**
     * 它是一个位于数据流末端的 bolt，只接收tuple
     * @param declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // this bolt does not emit anything
    }

    /**
     * 利用 cleanup() 方法在 topology 关闭时输出最 终的计数结果。
     * 通常情况下，cleanup() 方法用来释放 bolt 占用的资源，如打开的文件句柄
     * 或者数据库连接
     *
     * 开发 bolt 时需要谨记的是，当 topology 在 Storm 集群上运行时，IBolt.cleanup() 方法
     * 是不可靠的，不能保证会执行
     */
    @Override
    public void cleanup() {
        System.out.println("--- FINAL COUNTS ---");
        List<String> keys = new ArrayList<String>();
        keys.addAll(this.counts.keySet());
        Collections.sort(keys);
        for (String key : keys) {
            System.out.println(key + " : " + this.counts.get(key));
        }
        System.out.println("--------------");
    }
}
