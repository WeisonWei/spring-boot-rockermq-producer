package com.weison.sbrp.message;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author Weison
 * @date 2020/9/25
 */
@AllArgsConstructor
@Data
public class Message {
    private String topic;
    private String tag;
    private byte[] body;
}
