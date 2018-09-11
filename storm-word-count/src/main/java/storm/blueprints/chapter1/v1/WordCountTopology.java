package storm.blueprints.chapter1.v1;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import static storm.blueprints.utils.Utils.*;

public class WordCountTopology {

    private static final String SENTENCE_SPOUT_ID = "sentence-spout";
    private static final String SPLIT_BOLT_ID = "split-bolt";
    private static final String COUNT_BOLT_ID = "count-bolt";
    private static final String REPORT_BOLT_ID = "report-bolt";
    private static final String TOPOLOGY_NAME = "word-count-topology";

    public static void main(String[] args) throws Exception {

        SentenceSpout spout = new SentenceSpout();
        SplitSentenceBolt splitBolt = new SplitSentenceBolt();
        WordCountBolt countBolt = new WordCountBolt();
        ReportBolt reportBolt = new ReportBolt();


        TopologyBuilder builder = new TopologyBuilder();
        // 2代表两个executor线程
        builder.setSpout(SENTENCE_SPOUT_ID, spout, 2);
        // SentenceSpout --> SplitSentenceBolt
        builder.setBolt(SPLIT_BOLT_ID, splitBolt, 2)
                // 并行4个任务
                .setNumTasks(4)
                // 确立订阅关系 shuffleGrouping() 方 法 告 诉 Storm，要将类 SentenceSpout 发射的 tuple 随机均匀的分发给 SplitSentenceBolt 的实例
                .shuffleGrouping(SENTENCE_SPOUT_ID);
        // SplitSentenceBolt --> WordCountBolt
        builder.setBolt(COUNT_BOLT_ID, countBolt, 4)
                .setNumTasks(4)
                // fieldsGrouping() 方法来保证所有“ word”字段值相同的 tuple会被路由到同一个 WordCountBolt 实例中
                .fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));
        // WordCountBolt --> ReportBolt
        builder.setBolt(REPORT_BOLT_ID, reportBolt)
                //我们希望 WordCountBolt 发射的所有 tuple 路由到唯一的 ReportBolt 任务中。 globalGrouping() 方法提供了这种用法
                .globalGrouping(COUNT_BOLT_ID);

        Config config = new Config();


        // 这样就给 topology 分配了两个 worker(jvm) 而不是默认的一个。从而增加了 topology 的计 算资源，也更有效的利用了计算资源。
        // 我们还可以调整 topology 中的 executor 个数以及每 个 executor 分配的 task 数量
        config.setNumWorkers(2);

        LocalCluster cluster = new LocalCluster();
        // 当一个 topology 提交时，Storm 会 将默认配置和 Config 实例中的配置合并后作为参数传递给 submitTopology() 方法。
        // 合并后 的配置被分发给各个 spout 的 bolt 的 open()、prepare() 方法
        // 从这个层面上讲 Config 对象 代表了对 topology 所有组件全局生效的配置参数集合
        cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
        waitForSeconds(10);
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();
    }
}
