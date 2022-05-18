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
package org.apache.rocketmq.acl.plain;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.rocketmq.acl.AccessResource;
import org.apache.rocketmq.common.MixAll;

public class PlainAccessResource implements AccessResource {

    // Identify the user
    // 访问Key，用户名。
    private String accessKey;
    // 用户密码。
    private String secretKey;
    // 远程IP地址白名单。
    private String whiteRemoteAddress;
    // 是否是管理员角色。
    private boolean admin;
    // 默认topic访问权限，即如果没有配置topic的权限，则Topic默认的访问权限为1，表示为DENY。
    private byte defaultTopicPerm = 1;
    // 默认的消费组访问权限，默认为DENY。
    private byte defaultGroupPerm = 1;
    // 资源需要的访问权限映射表。
    private Map<String, Byte> resourcePermMap;
    // 远程IP地址验证策略。
    private RemoteAddressStrategy remoteAddressStrategy;
    // 当前请求的requestCode。
    private int requestCode;

    //the content to calculate the content
    // 请求头与请求体的内容。
    private byte[] content;
    // 签名字符串，这是通常的套路，在客户端时，
    // 首先将请求参数排序，然后使用secretKey生成签名字符串，
    // 服务端重复这个步骤，然后对比签名字符串，如果相同，则认为登录成功，否则失败。
    private String signature;
    // 密钥token。
    private String secretToken;
    // 目前作用未知，代码中目前未被使用。
    private String recognition;

    public PlainAccessResource() {
    }

    public static boolean isRetryTopic(String topic) {
        return null != topic && topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX);
    }

    public static String printStr(String resource, boolean isGroup) {
        if (resource == null) {
            return null;
        }
        if (isGroup) {
            return String.format("%s:%s", "group", getGroupFromRetryTopic(resource));
        } else {
            return String.format("%s:%s", "topic", resource);
        }
    }

    public static String getGroupFromRetryTopic(String retryTopic) {
        if (retryTopic == null) {
            return null;
        }
        return retryTopic.substring(MixAll.RETRY_GROUP_TOPIC_PREFIX.length());
    }

    public static String getRetryTopic(String group) {
        if (group == null) {
            return null;
        }
        return MixAll.getRetryTopic(group);
    }

    public void addResourceAndPerm(String resource, byte perm) {
        if (resource == null) {
            return;
        }
        if (resourcePermMap == null) {
            resourcePermMap = new HashMap<>();
        }
        resourcePermMap.put(resource, perm);
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public String getWhiteRemoteAddress() {
        return whiteRemoteAddress;
    }

    public void setWhiteRemoteAddress(String whiteRemoteAddress) {
        this.whiteRemoteAddress = whiteRemoteAddress;
    }

    public boolean isAdmin() {
        return admin;
    }

    public void setAdmin(boolean admin) {
        this.admin = admin;
    }

    public byte getDefaultTopicPerm() {
        return defaultTopicPerm;
    }

    public void setDefaultTopicPerm(byte defaultTopicPerm) {
        this.defaultTopicPerm = defaultTopicPerm;
    }

    public byte getDefaultGroupPerm() {
        return defaultGroupPerm;
    }

    public void setDefaultGroupPerm(byte defaultGroupPerm) {
        this.defaultGroupPerm = defaultGroupPerm;
    }

    public Map<String, Byte> getResourcePermMap() {
        return resourcePermMap;
    }

    public String getRecognition() {
        return recognition;
    }

    public void setRecognition(String recognition) {
        this.recognition = recognition;
    }

    public int getRequestCode() {
        return requestCode;
    }

    public void setRequestCode(int requestCode) {
        this.requestCode = requestCode;
    }

    public String getSecretToken() {
        return secretToken;
    }

    public void setSecretToken(String secretToken) {
        this.secretToken = secretToken;
    }

    public RemoteAddressStrategy getRemoteAddressStrategy() {
        return remoteAddressStrategy;
    }

    public void setRemoteAddressStrategy(RemoteAddressStrategy remoteAddressStrategy) {
        this.remoteAddressStrategy = remoteAddressStrategy;
    }

    public String getSignature() {
        return signature;
    }

    public void setSignature(String signature) {
        this.signature = signature;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

    public byte[] getContent() {
        return content;
    }

    public void setContent(byte[] content) {
        this.content = content;
    }
}
