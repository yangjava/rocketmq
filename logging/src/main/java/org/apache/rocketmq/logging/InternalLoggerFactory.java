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

package org.apache.rocketmq.logging;

import java.util.concurrent.ConcurrentHashMap;

/**
 * InternalLoggerFactory是一个抽象类，
 * 有两个实现类：InnerLoggerFactory和Slf4jLoggerFactory
 */
public abstract class InternalLoggerFactory {

    public static final String LOGGER_SLF4J = "slf4j";

    public static final String LOGGER_INNER = "inner";

    public static final String DEFAULT_LOGGER = LOGGER_SLF4J;

    private static String loggerType = null;
    // ConcurrentHashMap类型的缓存：loggerFactoryCache
    // key是唯一标识，value是InternalLoggerFactory，也就是这个类本身。
    private static ConcurrentHashMap<String, InternalLoggerFactory> loggerFactoryCache = new ConcurrentHashMap<String, InternalLoggerFactory>();

    public static InternalLogger getLogger(Class clazz) {
        return getLogger(clazz.getName());
    }

    public static InternalLogger getLogger(String name) {
        return getLoggerFactory().getLoggerInstance(name);
    }
    // LoggerFactory的获取，
    // 优先返回用户设置的loggerType，
    // 其次是默认的（slf4j），
    // 最后是（inner），
    // 如果全部初始化失败则抛出异常。
    private static InternalLoggerFactory getLoggerFactory() {
        InternalLoggerFactory internalLoggerFactory = null;
        if (loggerType != null) {
            internalLoggerFactory = loggerFactoryCache.get(loggerType);
        }
        if (internalLoggerFactory == null) {
            internalLoggerFactory = loggerFactoryCache.get(DEFAULT_LOGGER);
        }
        if (internalLoggerFactory == null) {
            internalLoggerFactory = loggerFactoryCache.get(LOGGER_INNER);
        }
        if (internalLoggerFactory == null) {
            throw new RuntimeException("[RocketMQ] Logger init failed, please check logger");
        }
        return internalLoggerFactory;
    }

    public static void setCurrentLoggerType(String type) {
        loggerType = type;
    }

    static {
        try {
            new Slf4jLoggerFactory();
        } catch (Throwable e) {
            //ignore
        }
        try {
            new InnerLoggerFactory();
        } catch (Throwable e) {
            //ignore
        }
    }
    // 把当前日志工厂塞入缓存
    protected void doRegister() {
        String loggerType = getLoggerType();
        if (loggerFactoryCache.get(loggerType) != null) {
            return;
        }
        loggerFactoryCache.put(loggerType, this);
    }

    protected abstract void shutdown();

    protected abstract InternalLogger getLoggerInstance(String name);
    // 返回当前日志工厂的类型（slf4j或者inner）
    protected abstract String getLoggerType();
}
