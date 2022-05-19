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
package org.apache.rocketmq.client.trace.hook;

import java.util.ArrayList;
import org.apache.rocketmq.client.hook.SendMessageContext;
import org.apache.rocketmq.client.hook.SendMessageHook;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.client.trace.AsyncTraceDispatcher;
import org.apache.rocketmq.client.trace.TraceBean;
import org.apache.rocketmq.client.trace.TraceContext;
import org.apache.rocketmq.client.trace.TraceDispatcher;
import org.apache.rocketmq.client.trace.TraceType;
import org.apache.rocketmq.common.protocol.NamespaceUtil;

public class SendMessageTraceHookImpl implements SendMessageHook {

    private TraceDispatcher localDispatcher;

    public SendMessageTraceHookImpl(TraceDispatcher localDispatcher) {
        this.localDispatcher = localDispatcher;
    }

    @Override
    public String hookName() {
        return "SendMessageTraceHook";
    }

    /**
     * sendMessageBefore主要的用途就是在消息发送的时候，
     * 先准备一部分消息跟踪日志，存储在发送上下文环境中，
     * 此时并不会发送消息轨迹数据。
     */
    @Override
    public void sendMessageBefore(SendMessageContext context) {
        //if it is message trace data,then it doesn't recorded
        // 如果topic主题为消息轨迹的Topic，直接返回。轨迹消息自己不需要添加消息轨迹
        if (context == null || context.getMessage().getTopic().startsWith(((AsyncTraceDispatcher) localDispatcher).getTraceTopicName())) {
            return;
        }
        //build the context content of TuxeTraceContext
        // 发送前采集的轨迹数据如下
        // 在消息发送上下文中，设置用来跟踪消息轨迹的上下环境，
        // 里面主要包含一个TraceBean集合、追踪类型（TraceType.Pub）与生产者所属的组。
        TraceContext tuxeContext = new TraceContext();
        tuxeContext.setTraceBeans(new ArrayList<TraceBean>(1));
        context.setMqTraceContext(tuxeContext);
        tuxeContext.setTraceType(TraceType.Pub);
        tuxeContext.setGroupName(NamespaceUtil.withoutNamespace(context.getProducerGroup()));
        //build the data bean object of message trace
        // 构建一条跟踪消息，用TraceBean来表示，
        // 记录原消息的topic、tags、keys、发送到broker地址、消息体长度等消息。
        TraceBean traceBean = new TraceBean();
        traceBean.setTopic(NamespaceUtil.withoutNamespace(context.getMessage().getTopic()));
        traceBean.setTags(context.getMessage().getTags());
        traceBean.setKeys(context.getMessage().getKeys());
        traceBean.setStoreHost(context.getBrokerAddr());
        traceBean.setBodyLength(context.getMessage().getBody().length);
        traceBean.setMsgType(context.getMsgType());
        tuxeContext.getTraceBeans().add(traceBean);
    }

    @Override
    public void sendMessageAfter(SendMessageContext context) {
        //if it is message trace data,then it doesn't recorded
        // 如果topic主题为消息轨迹的Topic，直接返回。
        if (context == null || context.getMessage().getTopic().startsWith(((AsyncTraceDispatcher) localDispatcher).getTraceTopicName())
            || context.getMqTraceContext() == null) {
            return;
        }
        if (context.getSendResult() == null) {
            return;
        }

        if (context.getSendResult().getRegionId() == null
            || !context.getSendResult().isTraceOn()) {
            // if switch is false,skip it
            return;
        }
        // 从MqTraceContext中获取跟踪的TraceBean，虽然设计成List结构体，
        // 但在消息发送场景，这里的数据永远只有一条，批量发送也不例外。
        TraceContext tuxeContext = (TraceContext) context.getMqTraceContext();
        TraceBean traceBean = tuxeContext.getTraceBeans().get(0);
        // 获取消息发送到收到响应结果的耗时。
        int costTime = (int) ((System.currentTimeMillis() - tuxeContext.getTimeStamp()) / tuxeContext.getTraceBeans().size());
        // 设置costTime(耗时)
        // 、success(是否发送成功)
        // 、regionId(发送到broker所在的分区)
        // 、msgId(消息ID，全局唯一)
        // 、offsetMsgId(消息物理偏移量，如果是批量消息，则是最后一条消息的物理偏移量)
        // 、storeTime，这里使用的是(客户端发送时间 + 二分之一的耗时)来表示消息的存储时间，这里是一个估值。
        tuxeContext.setCostTime(costTime);
        if (context.getSendResult().getSendStatus().equals(SendStatus.SEND_OK)) {
            tuxeContext.setSuccess(true);
        } else {
            tuxeContext.setSuccess(false);
        }
        tuxeContext.setRegionId(context.getSendResult().getRegionId());
        traceBean.setMsgId(context.getSendResult().getMsgId());
        traceBean.setOffsetMsgId(context.getSendResult().getOffsetMsgId());
        traceBean.setStoreTime(tuxeContext.getTimeStamp() + costTime / 2);
        // 将需要跟踪的信息通过TraceDispatcher转发到Broker服务器。
        localDispatcher.append(tuxeContext);
    }
}
