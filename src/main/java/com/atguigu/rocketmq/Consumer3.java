package com.atguigu.rocketmq;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;

public class Consumer3 {
	static private Map<String, String> logMap = new HashMap<>();
	   public static void main(String[] args) throws MQClientException {
           DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("rmq-group");
 
           consumer.setNamesrvAddr("192.168.2.160:9876;192.168.2.161:9876");
           consumer.setInstanceName("consumer");
           consumer.subscribe("itmayiedu-topic", "TagA");
 
           consumer.registerMessageListener(new MessageListenerConcurrently() {
                 @Override
                 public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                      String key = null;
                      String msgId = null;
                      try {
                            for (MessageExt msg : msgs) {
                                  key = msg.getKeys();
                                  if (logMap.containsKey(key)) {
                                       // 无需继续重试。
                                       System.out.println("key:"+key+",无需重试...");
                                       return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                                  }
                                  msgId = msg.getMsgId();
                                  System.out.println("key:" + key + ",msgid:" + msgId + "---" + new String(msg.getBody()));
                                  int i = 1 / 0;
                            }
 
                      } catch (Exception e) {
                            e.printStackTrace();
                            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                      } finally {
                            logMap.put(key, msgId);
                      }
                      return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                 }
           });
           consumer.start();
           System.out.println("Consumer Started.");
      }
}
