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

package org.apache.rocketmq.acl;

import org.apache.rocketmq.remoting.protocol.RemotingCommand;
// 访问验证器接口，主要定义两个接口。
public interface AccessValidator {
    /**
     * Parse to get the AccessResource(user, resource, needed permission)
     *
     * @param request
     * @param remoteAddr
     * @return Plain access resource result,include access key,signature and some other access attributes.
     */
    // 从请求头中解析本次请求对应的访问资源，即本次请求需要的访问权限。
    AccessResource parse(RemotingCommand request, String remoteAddr);

    /**
     * Validate the access resource.
     *
     * @param accessResource
     */
    // 根据本次需要访问的权限，与请求用户拥有的权限进行对比验证，
    // 判断是拥有权限，如果没有访问该操作的权限，则抛出异常，否则放行。
    void validate(AccessResource accessResource);
}
