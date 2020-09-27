package com.weison.sbrp.service;

import com.weison.sbrp.listener.MessageTransactionListener;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.*;

/**
 * @author Weison
 * @date 2020/09/27
 * @see
 */
@Slf4j
@Service
public class ProducerService {

    /**
     * 无序消息
     *
     * @return
     */
    @SneakyThrows
    public String send() {
        //1 Instantiate with a producer group name.
        DefaultMQProducer producer = new DefaultMQProducer("HelloGroup");
        //2 Specify name server addresses.
        producer.setNamesrvAddr("localhost:9876");
        //3 Launch the instance.
        producer.start();
        for (int i = 0; i < 5; i++) {
            //4 Create a message instance, specifying topic, tag and message body.
            byte[] bytes = ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET);
            Message msg = new Message("HelloTopic",
                    "HelloTag",
                    "HelloKey",
                    bytes);

            //5 Call send message to deliver message to one of brokers.
            SendResult sendResult = producer.send(msg);
            log.info("-" + i + "->" + sendResult.toString());
        }
        //6 Shut down once the producer instance is not longer in use.
        producer.shutdown();
        return "Ok";
    }

    /**
     * 顺序消息
     *
     * @return
     */
    @SneakyThrows
    public String orderedSend() {
        //1 Instantiate with a producer group name.
        DefaultMQProducer producer = new DefaultMQProducer("HelloOrderGroup");
        //2 Specify name server addresses.
        producer.setNamesrvAddr("localhost:9876");
        //3 Launch the instance.
        producer.start();
        for (int i = 0; i < 5; i++) {
            //4 Create a message instance, specifying topic, tag and message body.
            byte[] bytes = ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET);
            Message msg = new Message("HelloOrderTopic",
                    "HelloTag",
                    "HelloKey" + i,
                    bytes);

            //5 Call send message to deliver message to one of brokers.
            SendResult sendResult = producer.send(msg, (list, message, arg) -> getMessageQueue(list, (Integer) arg), 0);
            log.info("-" + i + "->" + sendResult.toString());
        }
        //6 Shut down once the producer instance is not longer in use.
        producer.shutdown();
        return "Ok";
    }

    /**
     * 事务消息
     *
     * @return
     */
    @SneakyThrows
    public String transactionSend() {
        //1 Instantiate with a producer group name.
        TransactionMQProducer producer = new TransactionMQProducer("HelloTransactionGroup");
        //2 Specify name server addresses.
        producer.setNamesrvAddr("localhost:9876");
        //3 指定消息监听对象 执行本地事物和消息回查
        producer.setTransactionListener(new MessageTransactionListener());
        //4 线程池
        ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(2000),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread thread = new Thread(r);
                        thread.setName("client-transaction-msg-check-thread");
                        return thread;
                    }
                });
        producer.setExecutorService(executorService);

        //5 Launch the instance.
        producer.start();
        for (int i = 0; i < 5; i++) {
            //4 Create a message instance, specifying topic, tag and message body.
            byte[] bytes = ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET);
            Message msg = new Message("HelloTransactionTopic",
                    "HelloTag",
                    "HelloKey",
                    bytes);

            //5 Call send message to deliver message to one of brokers.
            SendResult sendResult = producer.sendMessageInTransaction(msg, "Hello-transaction" + i);
            log.info("-" + i + "->" + sendResult.toString());
        }
        //6 Shut down once the producer instance is not longer in use.
        producer.shutdown();
        return "Ok";
    }

    private MessageQueue getMessageQueue(List<MessageQueue> list, Integer arg) {
        Integer queue = arg;
        return list.get(queue);
    }
}
