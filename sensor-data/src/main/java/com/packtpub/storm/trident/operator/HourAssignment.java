package com.packtpub.storm.trident.operator;

import com.packtpub.storm.trident.model.DiagnosisEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;

/**
 * 数赋 值一个表示小时的时间戳，
 * 并且增加一个 key cityDiseaseHour 到 tuple 的字段中，
 * 这个 key 包括城市、小时和疾病代码
 * 后续就使用这个 key 进行分组统计并使用 persistAggregate 函数对统计量持久性存储
 */
public class HourAssignment extends BaseFunction {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(HourAssignment.class);

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        DiagnosisEvent diagnosis = (DiagnosisEvent) tuple.getValue(0);
        String city = (String) tuple.getValue(1);
        long timestamp = diagnosis.time;
        long hourSinceEpoch = timestamp / 1000 / 60 / 60;
        LOG.debug("Key =  [" + city + ":" + hourSinceEpoch + "]");
        String key = city + ":" + diagnosis.diagnosisCode + ":" + hourSinceEpoch;

        List<Object> values = new ArrayList<Object>();
        values.add(hourSinceEpoch);
        values.add(key);
        collector.emit(values);
    }
}
