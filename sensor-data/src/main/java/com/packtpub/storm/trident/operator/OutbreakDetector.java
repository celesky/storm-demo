package com.packtpub.storm.trident.operator;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;

/**
 * 这个 function 提取出了特定城市、疾病、时间的发生次数，
 * 并且检查计数是否超过了 设定的阈值。如果超过，
 * 发送一个新的字段包括一条告警信息。在上面代码里，注意这个 function 实际上扮演了一个过滤器的角色，
 * 但是却作为一个 function 的形式来实现，是因 为需要在 tuple 中添加新的字段。
 * 因为 filter 不能改变 tuple，当我们既想过滤又想添加字段 时必须使用 function
 */
public class OutbreakDetector extends BaseFunction {
    private static final long serialVersionUID = 1L;
    public static final int THRESHOLD = 10000;

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String key = (String) tuple.getValue(0);
        Long count = (Long) tuple.getValue(1);
        if (count > THRESHOLD) {
            List<Object> values = new ArrayList<Object>();
            values.add("Outbreak detected for [" + key + "]!");
            collector.emit(values);
        }
    }
}