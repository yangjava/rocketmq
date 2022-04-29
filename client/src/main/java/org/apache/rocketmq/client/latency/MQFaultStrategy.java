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

package org.apache.rocketmq.client.latency;

import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * 消息失败策略， 延迟实现的门面类。
 * latency Max ，根据currentLatency 本次消息发送延迟，
 * 从latencyMax 尾部向前找到第一个比currentLatency 小的索引index，
 * 如果没有找到，返回0 。
 * 然后根据这个索引从notAvailableDuration 数组中取出对应的时间，
 * 在这个时长内， Broker 将设置为不可用。
 */
public class MQFaultStrategy {
    private final static InternalLogger log = ClientLogger.getLog();
    private final LatencyFaultTolerance<String> latencyFaultTolerance = new LatencyFaultToleranceImpl();

    private boolean sendLatencyFaultEnable = false;

    private long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};
    private long[] notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};

    public long[] getNotAvailableDuration() {
        return notAvailableDuration;
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.notAvailableDuration = notAvailableDuration;
    }

    public long[] getLatencyMax() {
        return latencyMax;
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.latencyMax = latencyMax;
    }

    public boolean isSendLatencyFaultEnable() {
        return sendLatencyFaultEnable;
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.sendLatencyFaultEnable = sendLatencyFaultEnable;
    }

    /**
     * 首先消息发送端采用重试机制，
     * 由retryTimesWhenSendFailed 指定同步方式重试次数，
     * 异步重试机制在收到消息发送结构后执行回调之前进行重试。
     * 由retryTimesWhenSendAsyncFailed指定，接下来就是循环执行，
     * 选择消息队列、发送消息，发送成功则返回，收到异常则重试。
     *
     * 选择消息队列有两种方式。
     * * sendLatencyFaultEnable=false ，默认不启用Broker 故障延迟机制。
     * * sendLatencyFaultEnable=true ，启用Broker 故障延迟机制。
     */
    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
        if (this.sendLatencyFaultEnable) {
            try {
                int index = tpInfo.getSendWhichQueue().getAndIncrement();
                // 根据对消息队列进行轮询获取一个消息队列。
                for (int i = 0; i < tpInfo.getMessageQueueList().size(); i++) {
                    int pos = Math.abs(index++) % tpInfo.getMessageQueueList().size();
                    if (pos < 0)
                        pos = 0;
                    MessageQueue mq = tpInfo.getMessageQueueList().get(pos);
                    // 验证该消息队列是否可用， latencyFaultTolerance.isAvailable(mq.getBrokerName())是关键。
                    if (latencyFaultTolerance.isAvailable(mq.getBrokerName())) {
                        if (null == lastBrokerName || mq.getBrokerName().equals(lastBrokerName))
                            return mq;
                    }
                }
                // 如果返回的MessageQueue 可用， 移除latencyFaultTolerance 关于该topic 条目，
                // 表明该Broker 故障已经恢复。
                final String notBestBroker = latencyFaultTolerance.pickOneAtLeast();
                int writeQueueNums = tpInfo.getQueueIdByBroker(notBestBroker);
                if (writeQueueNums > 0) {
                    final MessageQueue mq = tpInfo.selectOneMessageQueue();
                    if (notBestBroker != null) {
                        mq.setBrokerName(notBestBroker);
                        mq.setQueueId(tpInfo.getSendWhichQueue().getAndIncrement() % writeQueueNums);
                    }
                    return mq;
                } else {
                    latencyFaultTolerance.remove(notBestBroker);
                }
            } catch (Exception e) {
                log.error("Error occurred when selecting message queue", e);
            }

            return tpInfo.selectOneMessageQueue();
        }

        return tpInfo.selectOneMessageQueue(lastBrokerName);
    }

    /**
     * 如果isolation 为true ，则使用30s 作为computeNotAvailableDuration 方法的参数；
     * 如果isolation 为false ，
     * 则使用本次消息发送时延作为computeNotAvailableDuration方法的参数，
     * 那computeNotAvailableDuration的作用是计算因本次消息发送故障需要将Broker规避的时长，
     * 也就是接下来多久的时间内该Broker将不参与消息发送队列负载。
     */
    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        if (this.sendLatencyFaultEnable) {
            long duration = computeNotAvailableDuration(isolation ? 30000 : currentLatency);
            this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration);
        }
    }

    /**
     * 具体算法：从latencyMax 数组尾部开始寻找，
     * 找到第一个比currentLatency 小的下标，
     * 然后从notAvailableDuration 数组中获取需要规避的时长，
     * 该方法最终调用LatencyFaultTolerance的updateFaultltem 。
     */
    private long computeNotAvailableDuration(final long currentLatency) {
        for (int i = latencyMax.length - 1; i >= 0; i--) {
            if (currentLatency >= latencyMax[i])
                return this.notAvailableDuration[i];
        }

        return 0;
    }
}
