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
package org.apache.rocketmq.store.config;

/**
 * Broker角色
 *
 * 如果一个 Broker组有 Master和 Slave, 消息需要从 Master复制到 Slave上。
 *
 * 同步复制方式：
 * Master 和 Slave 均写成功 后才反馈给客户端写成功状态
 *
 * 异步复制方式：
 * 只要 Master 写成功即可反馈给客户端写成功状态 。
 *
 * 复制方式是通过 Broker 配置文件里的 brokerRole 参数进行设置
 */
public enum BrokerRole {
    // 异步复制
    ASYNC_MASTER,
    // 同步复制
    SYNC_MASTER,
    // 无影响
    SLAVE;
}
