package com.weison.sbrp.service;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Weison
 * @date 2020/09/27
 * @see
 */
@Slf4j
@Service
public class BroadcastProducerService {

    /**
     * 无序消息
     *
     * @return
     */
    @SneakyThrows
    public String send() {
        //1 Instantiate with a producer group name.
        DefaultMQProducer producer = new DefaultMQProducer("HelloBroadcastGroup");
        //2 Specify name server addresses.
        producer.setNamesrvAddr("localhost:9876");

        //3 Launch the instance.
        producer.start();
        List<Message> messages = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            //4 Create a message instance, specifying topic, tag and message body.
            byte[] bytes = ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET);
            Message msg = new Message("HelloBroadcastTopic",
                    "HelloBroadcastTag",
                    bytes);
            messages.add(msg);

        }

        //5 Call send message to deliver message to one of brokers.
        SendResult sendResult = producer.send(messages);
        log.info("-->" + sendResult.toString());

        //6 Shut down once the producer instance is not longer in use.
        producer.shutdown();
        return "Ok";
    }
}
