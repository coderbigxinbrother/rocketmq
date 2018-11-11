package com.atguigu.rocketmq;

import java.util.List;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;

public class Consumer2 {
	 public static void main(String[] args) throws MQClientException {
         DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("rmq-group");

         consumer.setNamesrvAddr("192.168.2.160:9876;192.168.2.161:9876");
         consumer.setInstanceName("consumer");
          consumer.subscribe("itmayiedu-topic", "TagA");

         consumer.registerMessageListener(new MessageListenerConcurrently() {
               @Override
               public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                    for (MessageExt msg : msgs) {
                          System.out.println(msg.getMsgId() + "---" + new String(msg.getBody()));
                    }
                    try {
                          int i = 1 / 0;
                    } catch (Exception e) {
                          e.printStackTrace();
                              // 需要重试
                          return ConsumeConcurrentlyStatus.RECONSUME_LATER;

                    }
                       // 不需要重试
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
               }
         });
         consumer.start();
         System.out.println("Consumer Started.");
    }
}
