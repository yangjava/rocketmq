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
 * 刷盘方式
 *
 * flushDiskType也分两种：
 *
 * SYNC_FLUSH：
 * 同步刷盘模式，当消息来了之后，尽可能快地从内存持久化到磁盘上，保证尽量不丢消息，性能会有损耗
 *
 * ASYNC_FLUSH：
 * 异步刷盘模式，消息到了内存之后，不急于马上落盘，极端情况可能会丢消息，但是性能较好。
 *
 */
public enum FlushDiskType {
    // 同步刷盘
    SYNC_FLUSH,
    // 异步刷盘
    ASYNC_FLUSH
}
