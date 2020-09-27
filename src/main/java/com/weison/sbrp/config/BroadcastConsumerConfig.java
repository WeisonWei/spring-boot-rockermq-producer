package com.weison.sbrp.config;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
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
public class BroadcastConsumerConfig {

    @SneakyThrows
    @Bean
    public void mqPushConsumer1() {
        //1
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("HelloBroadcastGroup");
        //2
        consumer.setNamesrvAddr("localhost:9876");
        //3
        consumer.setMessageModel(MessageModel.BROADCASTING);
        //4
        //consumer.subscribe("HelloTopic", "HelloTag");
        consumer.subscribe("HelloBroadcastTopic", "*");
        //5 设置每次获取消息数量
        consumer.setConsumeMessageBatchMaxSize(64);
        //6
        //list 默认32
        consumer.setMessageListener((MessageListenerConcurrently) (list, consumeConcurrentlyContext) -> getConsumeConcurrentlyStatus(list, "consumer2"));

        //7 Launch the consumer instance.
        consumer.start();
    }

    @SneakyThrows
    public void mqPushConsumer2() {
        //1
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("HelloBroadcastGroup");
        //2
        consumer.setNamesrvAddr("localhost:9876");
        //3
        consumer.setMessageModel(MessageModel.BROADCASTING);
        //4
        //consumer.subscribe("HelloTopic", "HelloTag");
        consumer.subscribe("HelloBroadcastTopic", "*");
        //5 设置每次获取消息数量
        consumer.setConsumeMessageBatchMaxSize(64);
        //6
        //list 默认32
        consumer.setMessageListener((MessageListenerConcurrently) (list, consumeConcurrentlyContext) -> getConsumeConcurrentlyStatus(list, "consumer2"));

        //7 Launch the consumer instance.
        consumer.start();
    }

    private ConsumeConcurrentlyStatus getConsumeConcurrentlyStatus(List<MessageExt> list, String consumer) {
        log.info(consumer + "开始");
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
                log.info(consumer + "结束");
                //消息重试
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        }
        log.info(consumer + "结束");
        //消息消费
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }


}
