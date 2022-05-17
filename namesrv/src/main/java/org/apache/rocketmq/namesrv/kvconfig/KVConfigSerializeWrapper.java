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
package org.apache.rocketmq.namesrv.kvconfig;

import java.util.HashMap;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
// kvConfig序列化包装器，内部只有一个配置表
public class KVConfigSerializeWrapper extends RemotingSerializable {
    // 可以看到这个KVConfig其实是基于Namesapce的，配置项的分类是基于namespace的。
    private HashMap<String/* Namespace */, HashMap<String/* Key */, String/* Value */>> configTable;

    public HashMap<String, HashMap<String, String>> getConfigTable() {
        return configTable;
    }

    public void setConfigTable(HashMap<String, HashMap<String, String>> configTable) {
        this.configTable = configTable;
    }
}
