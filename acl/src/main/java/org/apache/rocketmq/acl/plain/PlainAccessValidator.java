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

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.rocketmq.acl.AccessResource;
import org.apache.rocketmq.acl.AccessValidator;
import org.apache.rocketmq.acl.common.AclException;
import org.apache.rocketmq.acl.common.AclUtils;
import org.apache.rocketmq.acl.common.Permission;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.header.GetConsumerListByGroupRequestHeader;
import org.apache.rocketmq.common.protocol.header.UnregisterClientRequestHeader;
import org.apache.rocketmq.common.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.common.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import static org.apache.rocketmq.acl.plain.PlainAccessResource.getRetryTopic;
// RocketMQ默认提供的基于yml配置格式的访问验证器。
public class PlainAccessValidator implements AccessValidator {

    private PlainPermissionLoader aclPlugEngine;
    // 构造函数，直接创建PlainPermissionLoader对象，
    // 从命名上来看，应该是触发acl规则的加载，即解析plain_acl.yml
    public PlainAccessValidator() {
        aclPlugEngine = new PlainPermissionLoader();
    }
    // 该方法的作用就是从请求命令中解析出本次访问所需要的访问权限，
    // 最终构建AccessResource对象，为后续的校验权限做准备。
    @Override
    public AccessResource parse(RemotingCommand request, String remoteAddr) {
        // 首先创建PlainAccessResource，从远程地址中提取出远程访问IP地址。
        PlainAccessResource accessResource = new PlainAccessResource();
        if (remoteAddr != null && remoteAddr.contains(":")) {
            accessResource.setWhiteRemoteAddress(remoteAddr.split(":")[0]);
        } else {
            accessResource.setWhiteRemoteAddress(remoteAddr);
        }
//        如果请求头中的扩展字段为空，则抛出异常，
//        如果不为空，则从请求头中读取requestCode、
//        accessKey(请求用户名)、
//        签名字符串(signature)、
//        secretToken。
        accessResource.setRequestCode(request.getCode());

        if (request.getExtFields() == null) {
            //If request's extFields is null,then return accessResource directly(users can use whiteAddress pattern)
            //The following logic codes depend on the request's extFields not to be null.
            return accessResource;
        }
        accessResource.setAccessKey(request.getExtFields().get(SessionCredentials.ACCESS_KEY));
        accessResource.setSignature(request.getExtFields().get(SessionCredentials.SIGNATURE));
        accessResource.setSecretToken(request.getExtFields().get(SessionCredentials.SECURITY_TOKEN));
        // 就是从请求中得出本次操作的Topic、消息组名称
        // 为了方便区分topic与消费组，消费组使用消费者对应的重试主题，当成资源的Key
        // 当前版本需要进行ACL权限验证的请求命令如下
        try {
            switch (request.getCode()) {
                case RequestCode.SEND_MESSAGE:
                    accessResource.addResourceAndPerm(request.getExtFields().get("topic"), Permission.PUB);
                    break;
                case RequestCode.SEND_MESSAGE_V2:
                    accessResource.addResourceAndPerm(request.getExtFields().get("b"), Permission.PUB);
                    break;
                case RequestCode.CONSUMER_SEND_MSG_BACK:
                    accessResource.addResourceAndPerm(request.getExtFields().get("originTopic"), Permission.PUB);
                    accessResource.addResourceAndPerm(getRetryTopic(request.getExtFields().get("group")), Permission.SUB);
                    break;
                case RequestCode.PULL_MESSAGE:
                    accessResource.addResourceAndPerm(request.getExtFields().get("topic"), Permission.SUB);
                    accessResource.addResourceAndPerm(getRetryTopic(request.getExtFields().get("consumerGroup")), Permission.SUB);
                    break;
                case RequestCode.QUERY_MESSAGE:
                    accessResource.addResourceAndPerm(request.getExtFields().get("topic"), Permission.SUB);
                    break;
                case RequestCode.HEART_BEAT:
                    HeartbeatData heartbeatData = HeartbeatData.decode(request.getBody(), HeartbeatData.class);
                    for (ConsumerData data : heartbeatData.getConsumerDataSet()) {
                        accessResource.addResourceAndPerm(getRetryTopic(data.getGroupName()), Permission.SUB);
                        for (SubscriptionData subscriptionData : data.getSubscriptionDataSet()) {
                            accessResource.addResourceAndPerm(subscriptionData.getTopic(), Permission.SUB);
                        }
                    }
                    break;
                case RequestCode.UNREGISTER_CLIENT:
                    final UnregisterClientRequestHeader unregisterClientRequestHeader =
                        (UnregisterClientRequestHeader) request
                            .decodeCommandCustomHeader(UnregisterClientRequestHeader.class);
                    accessResource.addResourceAndPerm(getRetryTopic(unregisterClientRequestHeader.getConsumerGroup()), Permission.SUB);
                    break;
                case RequestCode.GET_CONSUMER_LIST_BY_GROUP:
                    final GetConsumerListByGroupRequestHeader getConsumerListByGroupRequestHeader =
                        (GetConsumerListByGroupRequestHeader) request
                            .decodeCommandCustomHeader(GetConsumerListByGroupRequestHeader.class);
                    accessResource.addResourceAndPerm(getRetryTopic(getConsumerListByGroupRequestHeader.getConsumerGroup()), Permission.SUB);
                    break;
                case RequestCode.UPDATE_CONSUMER_OFFSET:
                    final UpdateConsumerOffsetRequestHeader updateConsumerOffsetRequestHeader =
                        (UpdateConsumerOffsetRequestHeader) request
                            .decodeCommandCustomHeader(UpdateConsumerOffsetRequestHeader.class);
                    accessResource.addResourceAndPerm(getRetryTopic(updateConsumerOffsetRequestHeader.getConsumerGroup()), Permission.SUB);
                    accessResource.addResourceAndPerm(updateConsumerOffsetRequestHeader.getTopic(), Permission.SUB);
                    break;
                default:
                    break;

            }
        } catch (Throwable t) {
            throw new AclException(t.getMessage(), t);
        }

        // Content
        // 对扩展字段进行排序，便于生成签名字符串，
        // 然后将扩展字段与请求体(body)写入content字段。
        // 完成从请求头中解析出本次请求需要验证的权限。
        SortedMap<String, String> map = new TreeMap<String, String>();
        for (Map.Entry<String, String> entry : request.getExtFields().entrySet()) {
            if (!SessionCredentials.SIGNATURE.equals(entry.getKey())) {
                map.put(entry.getKey(), entry.getValue());
            }
        }
        accessResource.setContent(AclUtils.combineRequestContent(request, map));
        return accessResource;
    }
    // 验证权限，即根据本次请求需要的权限与当前用户所拥有的权限进行对比，
    // 如果符合，则正常执行；否则抛出AclException。
    @Override
    public void validate(AccessResource accessResource) {
        aclPlugEngine.validate((PlainAccessResource) accessResource);
    }

}
