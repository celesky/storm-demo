package com.packtpub.storm.trident.state;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.state.map.IBackingMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 在我们的 topology 中，实际上没有固化存储数据。我们简单地将数据放入 Concurrent HashMap 中。
 * 显然，对于多个机器的环境下，这样是不可行的。
 * 然而 BackingMap 是一个 非常巧妙的抽象。
 * 只需要将传入 MapState 对象的 backingmap 的实例替换就可以更换持久 层的实现
 */
public class OutbreakTrendBackingMap implements IBackingMap<Long> {
    private static final Logger LOG = LoggerFactory.getLogger(OutbreakTrendBackingMap.class);
    Map<String, Long> storage = new ConcurrentHashMap<String, Long>();

    @Override
    public List<Long> multiGet(List<List<Object>> keys) {
        List<Long> values = new ArrayList<Long>();
        for (List<Object> key : keys) {
            Long value = storage.get(key.get(0));
            if (value == null) {
                values.add(new Long(0));
            } else {
                values.add(value);
            }
        }
        return values;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<Long> vals) {
        for (int i = 0; i < keys.size(); i++) {
            LOG.info("Persisting [" + keys.get(i).get(0) + "] ==> [" + vals.get(i) + "]");
            storage.put((String) keys.get(i).get(0), vals.get(i));
        }
    }
}
