package storm.blueprints.chapter1.v1;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.TopologyBuilder;

import java.util.Arrays;

import static backtype.storm.Config.*;

public class SummitToRemote {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("word", new TestWordSpout(), 10);
        //builder.setBolt("exclaim1", new ExclamationBolt(), 3).shuffleGrouping("word");
        //builder.setBolt("exclaim2", new ExclamationBolt(), 2).shuffleGrouping("exclaim1");

        Config conf = new Config();
        conf.put(NIMBUS_HOST,"47.106.140.44"); //配置nimbus连接主机地址，比如：192.168.10.1
        conf.put(NIMBUS_THRIFT_PORT,"6627");//配置nimbus连接端口，默认 6627
        conf.put(STORM_ZOOKEEPER_SERVERS, Arrays.asList("47.106.140.44")); //配置zookeeper连接主机地址，可以使用集合存放多个
        conf.put(STORM_ZOOKEEPER_PORT,"2181"); //配置zookeeper连接端口，默认2181
        conf.setDebug(true);
        conf.setNumWorkers(3);

        //非常关键的一步，使用StormSubmitter提交拓扑时，不管怎么样，都是需要将所需的jar提交到nimbus上去，如果不指定jar文件路径，
        //storm默认会使用System.getProperty("storm.jar")去取，如果不设定，就不能提交
        System.setProperty("storm.jar","d:\\storm-remote-submit-1.0-SNAPSHOT-jar-with-dependencies.jar");
        //StormSubmitter.submitTopology(name, conf, topology);
    }

}
