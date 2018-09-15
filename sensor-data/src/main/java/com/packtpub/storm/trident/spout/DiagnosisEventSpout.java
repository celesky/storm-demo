package com.packtpub.storm.trident.spout;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import storm.trident.spout.ITridentSpout;

import java.util.Map;

/**
 * 发射疾病事件
 * 在 Trident 中，spout 没有真的发射 tuple，
 * 而是把这项工作分解给了 BatchCoordinator 和 Emitter 方 法
 *
 * TridentSpout 函数仅仅是简单地提供了到BatchCoordinator 和 Emitter 的访问方法，
 * 并且声明发射的 tuple 包括哪些字段
 */
public class DiagnosisEventSpout implements ITridentSpout<Long> {
    private static final long serialVersionUID = 1L;
    BatchCoordinator<Long> coordinator = new DefaultCoordinator();
    Emitter<Long> emitter = new DiagnosisEventEmitter();

    @Override
    public BatchCoordinator<Long> getCoordinator(String txStateId, Map conf, TopologyContext context) {
        return coordinator;
    }

    @Override
    public Emitter<Long> getEmitter(String txStateId, Map conf, TopologyContext context) {
        return emitter;
    }

    @Override
    public Map getComponentConfiguration() {
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("event");
    }
}