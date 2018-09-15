package com.packtpub.storm.trident.spout;

import com.packtpub.storm.trident.model.DiagnosisEvent;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.ITridentSpout.Emitter;
import storm.trident.topology.TransactionAttempt;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 负责发送tuple
 * 需要依靠元数据来恰当的进行批次数据重放
 *
 * 在Storm里，spout使用collector来 发送tuple，Emmiter函数在Trident spout中执行这种功能。
 * 唯一的区别是，使用 TridentCollector 类，发送出去的 tuple 是通过 BatchCoordinator 类初始化的一批数据
 */
public class DiagnosisEventEmitter implements Emitter<Long>, Serializable {
    private static final long serialVersionUID = 1L;
    AtomicInteger successfulTransactions = new AtomicInteger(0);

    /**
     * @param tx 事务的信息
     * @param coordinatorMeta DefaultCoordinator生成的元数据
     * @param collector 事件发射器
     */
    @Override
    public void emitBatch(TransactionAttempt tx, Long coordinatorMeta, TridentCollector collector) {
        for (int i = 0; i < 10000; i++) {
            List<Object> events = new ArrayList<Object>();
            double lat = new Double(-30 + (int) (Math.random() * 75));
            double lng = new Double(-120 + (int) (Math.random() * 70));
            long time = System.currentTimeMillis();

            String diag = new Integer(320 + (int) (Math.random() * 7)).toString();
            DiagnosisEvent event = new DiagnosisEvent(lat, lng, time, diag);
            events.add(event);
            collector.emit(events);
        }
    }

    @Override
    public void success(TransactionAttempt tx) {
        successfulTransactions.incrementAndGet();
    }

    @Override
    public void close() {
    }

}
