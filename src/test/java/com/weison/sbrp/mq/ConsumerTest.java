package com.weison.sbrp.mq;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * @author Weison
 * @date 2020/9/25
 */
@Slf4j
public class ConsumerTest {

    @Test
    public void helloConsumer() throws InterruptedException, MQClientException {
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
        System.out.printf("Consumer Started.%n");
    }

    @Test
    public void consumer() throws InterruptedException, MQClientException {
        // Instantiate with specified consumer group name.
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("please_rename_unique_group_name");

        // Specify name server addresses.
        consumer.setNamesrvAddr("localhost:9876");

        // Subscribe one more more topics to consume.
        consumer.subscribe("TopicTest", "*");
        // Register callback to execute on arrival of messages fetched from brokers.
        consumer.registerMessageListener(new MessageListenerConcurrently() {

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        //Launch the consumer instance.
        consumer.start();

        System.out.printf("Consumer Started.%n");
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
