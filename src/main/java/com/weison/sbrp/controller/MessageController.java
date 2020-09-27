package com.weison.sbrp.controller;

import com.weison.sbrp.service.BroadcastProducerService;
import com.weison.sbrp.service.ProducerService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * @author Weison
 * @date 2020/09/27
 * @see
 */
@RestController
public class MessageController {

    @Resource
    ProducerService producerService;

    @Resource
    BroadcastProducerService broadcastProducerService;

    @PostMapping("/message")
    public String send() {
        return producerService.send();
    }

    @PostMapping("/message/order")
    public String orderedSend() {
        return producerService.orderedSend();
    }

    @PostMapping("/message/transaction")
    public String transactionSend() {
        return producerService.transactionSend();
    }

    @PostMapping("/message/broadcast")
    public String broadcastSend() {
        return broadcastProducerService.send();
    }
}
