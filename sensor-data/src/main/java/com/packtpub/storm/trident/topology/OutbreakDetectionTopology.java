package com.packtpub.storm.trident.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import com.packtpub.storm.trident.operator.*;
import com.packtpub.storm.trident.spout.DiagnosisEventSpout;
import com.packtpub.storm.trident.state.OutbreakTrendFactory;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;

public class OutbreakDetectionTopology {

    /**
     * 通常，应用运算需要声明一个输入域集合和一个输出域集合，也就是 funcition 域
     * @return
     */
    public static StormTopology buildTopology() {
        TridentTopology topology = new TridentTopology();
        DiagnosisEventSpout spout = new DiagnosisEventSpout();
        Stream inputStream = topology.newStream("event", spout);

        inputStream.each(new Fields("event"), new DiseaseFilter())
                // CityAssignment 会在 event 字段上运算并且增加一个叫做 city 的新字段， 这个字段会附在 tuple 中向后发射
                .each(new Fields("event"), new CityAssignment(), new Fields("city"))
                .each(new Fields("event", "city"), new HourAssignment(), new Fields("hour", "cityDiseaseHour"))
                .groupBy(new Fields("cityDiseaseHour"))
                .persistentAggregate(new OutbreakTrendFactory(), new Count(), new Fields("count")).newValuesStream()
                .each(new Fields("cityDiseaseHour", "count"), new OutbreakDetector(), new Fields("alert"))
                .each(new Fields("alert"), new DispatchAlert(), new Fields());
        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("cdc", conf, buildTopology());
        Thread.sleep(200000);
        cluster.shutdown();
    }
}
