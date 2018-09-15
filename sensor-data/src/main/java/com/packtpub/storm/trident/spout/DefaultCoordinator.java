package com.packtpub.storm.trident.spout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.spout.ITridentSpout.BatchCoordinator;

import java.io.Serializable;

/**
 * 负责管理批次和元数据
 * BatchCoordinator 类作为一个 Storm Bolt 运行在一个单线程中。
 * Storm 会在 ZooKeeper 中持久化存储这个元数据。当事务处理完成时会通知到对应的 coordinator
 */
public class DefaultCoordinator implements BatchCoordinator<Long>, Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DefaultCoordinator.class);

    @Override
    public boolean isReady(long txid) {
        return true;
    }

    @Override
    public void close() {
    }

    @Override
    public Long initializeTransaction(long txid, Long prevMetadata, Long currMetadata) {
        LOG.info("Initializing Transaction [" + txid + "]");
        return null;
    }

    @Override
    public void success(long txid) {
        LOG.info("Successful Transaction [" + txid + "]");
    }
}