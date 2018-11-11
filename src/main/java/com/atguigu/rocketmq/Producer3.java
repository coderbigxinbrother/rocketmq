package com.atguigu.rocketmq;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;

public class Producer3 {
	  public static void main(String[] args) throws MQClientException {
          DefaultMQProducer producer = new DefaultMQProducer("rmq-group");
          producer.setNamesrvAddr("192.168.2.160:9876;192.168.2.161:9876");
          producer.setInstanceName("producer");
          producer.start();
          try {
                for (int i = 0; i < 1; i++) {
                     Thread.sleep(1000); // 每秒发送一次MQ
                     Message msg = new Message("itmayiedu-topic", // topic 主题名称
                                 "TagA", // tag 临时值
                                 ("itmayiedu-6" + i).getBytes()// body 内容
                     );
                     msg.setKeys(System.currentTimeMillis() + "");
                     SendResult sendResult = producer.send(msg);
                     System.out.println(sendResult.toString());
                }
          } catch (Exception e) {
                e.printStackTrace();
          }
          producer.shutdown();
     }

}
