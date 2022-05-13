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
package org.apache.rocketmq.common.filter;

import java.net.URL;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

public class FilterAPI {
    public static URL classFile(final String className) {
        final String javaSource = simpleClassName(className) + ".java";
        URL url = FilterAPI.class.getClassLoader().getResource(javaSource);
        return url;
    }

    public static String simpleClassName(final String className) {
        String simple = className;
        int index = className.lastIndexOf(".");
        if (index >= 0) {
            simple = className.substring(index + 1);
        }

        return simple;
    }
    // 构造一个SubscriptionData实体
    public static SubscriptionData buildSubscriptionData(final String consumerGroup, String topic,
        String subString) throws Exception {
        // 构造一个SubscriptionData实体，设置topic、表达式（tag）
        SubscriptionData subscriptionData = new SubscriptionData();
        subscriptionData.setTopic(topic);
        subscriptionData.setSubString(subString);
        // 如果tag为空或者为"*"，统一设置为"*"，即订阅所有消息
        if (null == subString || subString.equals(SubscriptionData.SUB_ALL) || subString.length() == 0) {
            subscriptionData.setSubString(SubscriptionData.SUB_ALL);
        } else {
            // tag不为空，则先按照‘|’进行分割
            String[] tags = subString.split("\\|\\|");
            if (tags.length > 0) {
                // 遍历tag表达式数组
                for (String tag : tags) {
                    if (tag.length() > 0) {
                        String trimString = tag.trim();
                        if (trimString.length() > 0) {
                            // 将每个tag的值设置到tagSet中
                            subscriptionData.getTagsSet().add(trimString);
                            subscriptionData.getCodeSet().add(trimString.hashCode());
                        }
                    }
                }
            } else {
                // tag解析异常
                throw new Exception("subString split error");
            }
        }

        return subscriptionData;
    }
    // 构建订阅实体数据
    public static SubscriptionData build(final String topic, final String subString,
        final String type) throws Exception {
        // 如果是Tag模式
        if (ExpressionType.TAG.equals(type) || type == null) {
            return buildSubscriptionData(null, topic, subString);
        }

        if (subString == null || subString.length() < 1) {
            throw new IllegalArgumentException("Expression can't be null! " + type);
        }

        SubscriptionData subscriptionData = new SubscriptionData();
        subscriptionData.setTopic(topic);
        subscriptionData.setSubString(subString);
        subscriptionData.setExpressionType(type);

        return subscriptionData;
    }
}
