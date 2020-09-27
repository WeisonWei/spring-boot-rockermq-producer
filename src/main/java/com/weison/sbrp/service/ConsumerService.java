package com.weison.sbrp.service;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.common.message.MessageExt;

import javax.annotation.PostConstruct;
import java.util.List;

/**
 * @author Weison
 * @date 2020/09/27
 * @see
 */
@Slf4j
public class ConsumerService {

    /**
     * 无序消息
     *
     * @return
     */
    @PostConstruct
    @SneakyThrows
    public void consumer() {
        MessageListenerConcurrently messageListener = (list, consumeConcurrentlyContext) -> consumeConcurrentlyOneByOne(list);
        Runnable runnable = () -> startConsumer("HelloTopic", "HelloGroup", messageListener);
        new Thread(runnable, "consumer-rocket").start();
    }

    /**
     * 顺序消息
     *
     * @return
     */
    @PostConstruct
    @SneakyThrows
    public void orderedConsumer() {
        MessageListenerOrderly messageListener = (list, consumeOrderedContext) -> consumeOrderedOneByOne(list);
        Runnable runnable = () -> startConsumer("HelloOrderTopic", "HelloOrderGroup", messageListener);
        new Thread(runnable, "consumerOrder-rocket").start();
    }

    @SneakyThrows
    private void startConsumer(String group, String topic, MessageListener messageListener) {
        //1
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group);
        //2
        consumer.setNamesrvAddr("localhost:9876");
        //3
        //consumer.subscribe("HelloTopic", "HelloTag");
        consumer.subscribe(topic, "*");
        //4设置每次获取消息数量
        consumer.setConsumeMessageBatchMaxSize(64);
        //5
        //list 默认32
        consumer.setMessageListener(messageListener);

        //6 Launch the consumer instance.
        consumer.start();
    }


    private ConsumeConcurrentlyStatus consumeConcurrentlyOneByOne(List<MessageExt> list) {
        for (MessageExt msg : list) {
            try {
                String msgId = msg.getMsgId();
                log.info("msgId-->" + msgId);
                String topic = msg.getTopic();
                log.info("topic-->" + topic);
                String tags = msg.getTags();
                log.info("tags-->" + tags);
                byte[] body = msg.getBody();
                String bodyStr = new String(body);
                log.info("bodyStr-->" + bodyStr);
            } catch (Exception e) {
                log.info("消费失败，稍后重试-->");
                e.printStackTrace();
                //消息重试
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        }
        //消息消费
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }

    private ConsumeOrderlyStatus consumeOrderedOneByOne(List<MessageExt> list) {
        for (MessageExt msg : list) {
            try {
                String msgId = msg.getMsgId();
                log.info("msgId-->" + msgId);
                String topic = msg.getTopic();
                log.info("topic-->" + topic);
                String tags = msg.getTags();
                log.info("tags-->" + tags);
                byte[] body = msg.getBody();
                String bodyStr = new String(body);
                log.info("bodyStr-->" + bodyStr);
            } catch (Exception e) {
                log.info("消费失败，稍后重试-->");
                e.printStackTrace();
                //消息重试
                return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
            }
        }
        //消息消费
        return ConsumeOrderlyStatus.SUCCESS;
    }
}
