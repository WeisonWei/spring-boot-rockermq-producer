package com.weison.sbrp.config;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * @author Weison
 * @date 2020/09/27
 * @see
 */
@Slf4j
@Configuration
public class ConsumerConfig {

    @SneakyThrows
    @Bean
    public void mqPushConsumer() {
        //1
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("HelloGroup");
        //2
        consumer.setNamesrvAddr("localhost:9876");
        //3
        //consumer.subscribe("HelloTopic", "HelloTag");
        consumer.subscribe("HelloTopic", "*");
        //4设置每次获取消息数量
        consumer.setConsumeMessageBatchMaxSize(64);
        //5
        //list 默认32
        consumer.setMessageListener((MessageListenerConcurrently) (list, consumeConcurrentlyContext) -> getConsumeConcurrentlyStatus(list));

        //6 Launch the consumer instance.
        consumer.start();
    }

    @SneakyThrows
    @Bean
    public void orderMQPushConsumer() {
        //1
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("HelloOrderGroup");
        //2
        consumer.setNamesrvAddr("localhost:9876");
        //3
        //consumer.subscribe("HelloTopic", "HelloTag");
        consumer.subscribe("HelloOrderTopic", "*");
        //4设置每次获取消息数量
        consumer.setConsumeMessageBatchMaxSize(64);
        //5
        //list 默认32
        consumer.setMessageListener((MessageListenerOrderly) (list, consumeConcurrentlyContext) -> getConsumeOrderStatus(list));

        //6 Launch the consumer instance.
        consumer.start();
    }

    @SneakyThrows
    @Bean
    public void transactionMQPushConsumer() {
        //1
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("HelloTransactionGroup");
        //2
        consumer.setNamesrvAddr("localhost:9876");
        //3
        //consumer.subscribe("HelloTopic", "HelloTag");
        consumer.subscribe("HelloTransactionTopic", "*");
        //4设置每次获取消息数量
        consumer.setConsumeMessageBatchMaxSize(64);
        //5
        //list 默认32
        consumer.setMessageListener((MessageListenerConcurrently) (list, consumeConcurrentlyContext) -> getConsumeConcurrentlyStatus(list));

        //6 Launch the consumer instance.
        consumer.start();
    }


    private ConsumeOrderlyStatus getConsumeOrderStatus(List<MessageExt> list) {
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

    private ConsumeConcurrentlyStatus getConsumeConcurrentlyStatus(List<MessageExt> list) {
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


}
