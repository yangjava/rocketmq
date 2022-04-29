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
package org.apache.rocketmq.client.impl.producer;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.client.common.ThreadLocalIndex;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;

public class TopicPublishInfo {
    // 是否是顺序消息。默认是false。
    private boolean orderTopic = false;
    //
    private boolean haveTopicRouterInfo = false;
    // 该主题队列的消息队列。
    private List<MessageQueue> messageQueueList = new ArrayList<MessageQueue>();
    // 每选择一次消息队列， 该值会自增l ，如果Integer.MAX_VALUE,则重置为0 ，用于选择消息队列。
    private volatile ThreadLocalIndex sendWhichQueue = new ThreadLocalIndex();
    //
    private TopicRouteData topicRouteData;

    public boolean isOrderTopic() {
        return orderTopic;
    }

    public void setOrderTopic(boolean orderTopic) {
        this.orderTopic = orderTopic;
    }

    public boolean ok() {
        return null != this.messageQueueList && !this.messageQueueList.isEmpty();
    }

    public List<MessageQueue> getMessageQueueList() {
        return messageQueueList;
    }

    public void setMessageQueueList(List<MessageQueue> messageQueueList) {
        this.messageQueueList = messageQueueList;
    }

    public ThreadLocalIndex getSendWhichQueue() {
        return sendWhichQueue;
    }

    public void setSendWhichQueue(ThreadLocalIndex sendWhichQueue) {
        this.sendWhichQueue = sendWhichQueue;
    }

    public boolean isHaveTopicRouterInfo() {
        return haveTopicRouterInfo;
    }

    public void setHaveTopicRouterInfo(boolean haveTopicRouterInfo) {
        this.haveTopicRouterInfo = haveTopicRouterInfo;
    }

    /**
     * 首先在一次消息发送过程中，可能会多次执行选择消息队列这个方法，
     * lastBrokerName就是上一次选择的执行发送消息失败的Broker 。
     * 第一次执行消息队列选择时，lastBrokerName 为null ，
     * 此时直接用sendWhichQueue 自增再获取值，
     * 与当前路由表中消息队列个数取模，
     * 返回该位置的MessageQueue(selectOneMessageQueue（） 方法），
     * 如果消息发送再失败的话，
     * 下次进行消息队列选择时规避上次MesageQueue 所在的Broker，
     * 否则还是很有可能再次失败。
     *
     * 该算法在一次消息发送过程中能成功规避故障的Broker，
     * 但如果Broker 若机，由于路由算法中的消息队列是按Broker 排序的，
     * 如果上一次根据路由算法选择的是若机的Broker的第一个队列，
     * 那么随后的下次选择的是若机Broker 的第二个队列，
     * 消息发送很有可能会失败，再次引发重试，带来不必要的性能损耗，
     * 那么有什么方法在一次消息发送失败后，暂时将该Broker 排除在消息队列选择范围外呢？
     *
     * 或许有朋友会问， Broker 不可用后，
     * 路由信息中为什么还会包含该Broker的路由信息呢？
     * 其实这不难解释：首先， NameServer 检测Broker 是否可用是有延迟的，
     * 最短为一次心跳检测间隔（ 10s ）；
     * 其次， NameServer 不会检测到Broker 岩机后马上推送消息给消息生产者，
     * 而是消息生产者每隔30s 更新一次路由信息，
     * 所以消息生产者最快感知Broker 最新的路由信息也需要30s 。
     * 如果能引人一种机制，在Broker 若机期间，
     * 如果一次消息发送失败后，可以将该Broker 暂时排除在消息队列的选择范围中。
     */
    public MessageQueue selectOneMessageQueue(final String lastBrokerName) {
        if (lastBrokerName == null) {
            return selectOneMessageQueue();
        } else {
            int index = this.sendWhichQueue.getAndIncrement();
            for (int i = 0; i < this.messageQueueList.size(); i++) {
                int pos = Math.abs(index++) % this.messageQueueList.size();
                if (pos < 0)
                    pos = 0;
                MessageQueue mq = this.messageQueueList.get(pos);
                if (!mq.getBrokerName().equals(lastBrokerName)) {
                    return mq;
                }
            }
            return selectOneMessageQueue();
        }
    }

    public MessageQueue selectOneMessageQueue() {
        int index = this.sendWhichQueue.getAndIncrement();
        int pos = Math.abs(index) % this.messageQueueList.size();
        if (pos < 0)
            pos = 0;
        return this.messageQueueList.get(pos);
    }

    public int getQueueIdByBroker(final String brokerName) {
        for (int i = 0; i < topicRouteData.getQueueDatas().size(); i++) {
            final QueueData queueData = this.topicRouteData.getQueueDatas().get(i);
            if (queueData.getBrokerName().equals(brokerName)) {
                return queueData.getWriteQueueNums();
            }
        }

        return -1;
    }

    @Override
    public String toString() {
        return "TopicPublishInfo [orderTopic=" + orderTopic + ", messageQueueList=" + messageQueueList
            + ", sendWhichQueue=" + sendWhichQueue + ", haveTopicRouterInfo=" + haveTopicRouterInfo + "]";
    }

    public TopicRouteData getTopicRouteData() {
        return topicRouteData;
    }

    public void setTopicRouteData(final TopicRouteData topicRouteData) {
        this.topicRouteData = topicRouteData;
    }
}
