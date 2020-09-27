package com.weison.sbrp.listener;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * @author Weison
 * @date 2020/09/27
 * @see
 */
@Slf4j
public class MessageTransactionListener implements TransactionListener {

    private ConcurrentHashMap<String, LocalTransactionState> transactionMap = new ConcurrentHashMap();

    /**
     * 执行本地事务
     *
     * @param message
     * @param o
     * @return
     */

    @Override
    public LocalTransactionState executeLocalTransaction(Message message, Object o) {
        String transactionId = message.getTransactionId();
        log.info("transactionId-->" + transactionId);

        try {
            //执行数据库操作
            log.info("开始插入数据");
            TimeUnit.SECONDS.sleep(1);
            log.info("数据插入成功");
            transactionMap.put(transactionId, LocalTransactionState.COMMIT_MESSAGE);
            return LocalTransactionState.COMMIT_MESSAGE;
        } catch (InterruptedException e) {
            e.printStackTrace();
            return LocalTransactionState.ROLLBACK_MESSAGE;
        }
    }

    /**
     * 事务回查
     *
     * @param messageExt
     * @return
     */
    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
        String transactionId = messageExt.getTransactionId();
        LocalTransactionState localTransactionState = transactionMap.get(transactionId);
        log.info("transactionId:{},LocalTransactionState:{}", transactionId, localTransactionState);
        return localTransactionState;
    }
}
