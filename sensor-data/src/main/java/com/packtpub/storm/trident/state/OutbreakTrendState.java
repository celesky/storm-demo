package com.packtpub.storm.trident.state;

import storm.trident.state.map.NonTransactionalMap;

/**
 * Storm 提供了 map 的实现来屏蔽了持久层进行状态管理的复杂性。
 * 尤其是， Trident 提供了 State 实现通过维护额外的信息来实现上面提到的保证。
 * 这些对象命名相似: NonTransactionalMap、TransactionalMap、OpaqueMap。
 * 回到我们的例子里，因为我们没有事务型保证，所以选用 NonTransactionalMap 作为 我们的 State 对象
 */
public class OutbreakTrendState extends NonTransactionalMap<Long> {
    protected OutbreakTrendState(OutbreakTrendBackingMap outbreakBackingMap) {
        super(outbreakBackingMap);
    }
}
