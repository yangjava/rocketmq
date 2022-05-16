/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.client.consumer.listener;

import org.apache.rocketmq.common.message.MessageQueue;

/**
 * Consumer concurrent consumption context
 */
public class ConsumeConcurrentlyContext {
    private final MessageQueue messageQueue;
    /**
     * Message consume retry strategy<br>
     * -1,no retry,put into DLQ directly<br>
     * 0,broker control retry frequency<br>
     * >0,client control retry frequency
     */
    // -1 直接放到死信队列，
    // 0  broker每次对重试消费次数加1来控制重试策略，
    // >0 由consumer控制重试消费策略
    private int delayLevelWhenNextConsume = 0;
    // 最后一个正常消费的消息索引号。
    // 索引号的最大值为传入的消息数量-1。
    // 通过context.setAckIndex(n)，可以说明哪个索引号之后的消息消费失败。
    // 这样从索引ackIndex往后的消息会被重新发送至broker，等待下一次消费。
    // 然后从ProcessQueue中移除消费过的消息。
    private int ackIndex = Integer.MAX_VALUE;

    public ConsumeConcurrentlyContext(MessageQueue messageQueue) {
        this.messageQueue = messageQueue;
    }

    public int getDelayLevelWhenNextConsume() {
        return delayLevelWhenNextConsume;
    }

    public void setDelayLevelWhenNextConsume(int delayLevelWhenNextConsume) {
        this.delayLevelWhenNextConsume = delayLevelWhenNextConsume;
    }

    public MessageQueue getMessageQueue() {
        return messageQueue;
    }

    public int getAckIndex() {
        return ackIndex;
    }

    public void setAckIndex(int ackIndex) {
        this.ackIndex = ackIndex;
    }
}
