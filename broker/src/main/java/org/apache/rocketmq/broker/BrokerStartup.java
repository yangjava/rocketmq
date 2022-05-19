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
package org.apache.rocketmq.broker;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.common.TlsMode;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.netty.NettySystemConfig;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_ENABLE;
/**
 * Broker 主要负责消息的存储，投递和查询以及保证服务的高可用。
 * Broker负责接收生产者发送的消息并存储、同时为消费者消费消息提供支持。
 * 为了实现这些功能，Broker包含几个重要的子模块：
 *
 * 通信模块：负责处理来自客户端（生产者、消费者）的请求。
 * 客户端管理模块:负责管理客户端（生产者、消费者）和维护消费者的Topic订阅信息。
 * 存储模块：提供存储消息和查询消息的能力，方便Broker将消息存储到硬盘。
 * 高可用服务（HA Service）：提供数据冗余的能力，保证数据存储到多个服务器上，将Master Broker的数据同步到Slavew Broker上。
 * 索引服务（Index service）：对投递到Broker的消息建立索引，提供快速查询消息的能力。
 */

// BrokerStartup 负责了Broker的启动，其中的主方法main（）即为整个消息服务器的入口函数。
public class BrokerStartup {
    public static Properties properties = null;
    public static CommandLine commandLine = null;
    public static String configFile = null;
    public static InternalLogger log;

    // Broker在启动的过程中，将进行初始化的工作，初始化上述模块所需要的配置和资源等。
    // 在Name Server启动以后，Broker就可以开始启动了，
    // 启动过程将所有路由信息都注册到Name server服务器上，
    // 生产者就可以发送消息到Broker，
    // 消费者也可以从Broker消费消息。
    // BrokerStartup类是Broker的启动类，在BrokerStartup类的main方法中，
    // 首先创建用createBrokerController方法创建Broker控制器（BrokerController类），
    // Broker控制器主要负责Broker启动过程的具体的相关逻辑实现。
    // 创建好Broker 控制器以后，就可以启动Broker 控制器
    public static void main(String[] args) {
        start(createBrokerController(args));
    }



    /**
     * start方法的逻辑比较简单，首先启动Broker控制器，
     * 然后打印成功启动Broker控制器的日志。
     * Broker控制器启动的逻辑主要在controller.start()
     */
    public static BrokerController start(BrokerController controller) {
        try {
            //Broker控制器启动
            controller.start();

            String tip = "The broker[" + controller.getBrokerConfig().getBrokerName() + ", "
                + controller.getBrokerAddr() + "] boot success. serializeType=" + RemotingCommand.getSerializeTypeConfigInThisServer();

            if (null != controller.getBrokerConfig().getNamesrvAddr()) {
                tip += " and name server is " + controller.getBrokerConfig().getNamesrvAddr();
            }
            // 打印Broker成功的消息
            log.info(tip);
            System.out.printf("%s%n", tip);
            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    public static void shutdown(final BrokerController controller) {
        if (null != controller) {
            controller.shutdown();
        }
    }

    /**
     * BrokerController的创建过程主要是分析配置信息并进行初始化。
     * start()方法中Broker的启动过程，顾名思义，就是用来进行启动各种服务的。
     * 创建Broker控制器
     * 初始化配置信息
     * 创建并初始化Broker控制
     * 注册Broker关闭的钩子
     * 启动Broker控制器
     */
    public static BrokerController createBrokerController(String[] args) {
        // 设置RocketMQ的版本
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));
        // 设置netty接收和发送请求的buffer大小
        if (null == System.getProperty(NettySystemConfig.COM_ROCKETMQ_REMOTING_SOCKET_SNDBUF_SIZE)) {
            NettySystemConfig.socketSndbufSize = 131072;
        }

        if (null == System.getProperty(NettySystemConfig.COM_ROCKETMQ_REMOTING_SOCKET_RCVBUF_SIZE)) {
            NettySystemConfig.socketRcvbufSize = 131072;
        }

        try {
            // 构建命令行：将命令行进行解析封装
            //PackageConflictDetect.detectFastjson();
            Options options = ServerUtil.buildCommandlineOptions(new Options());
            commandLine = ServerUtil.parseCmdLine("mqbroker", args, buildCommandlineOptions(options),
                new PosixParser());
            if (null == commandLine) {
                System.exit(-1);
            }
            // Broker在启动的时候，会初始化一些配置，
            // 如Broker配置、netty服务端配置、netty客户端配置、消息存储配置，
            // 为Broker启动提供配置准备。

            // BrokerConfig：属性主要包括Broker相关的配置属性，
            // 如Broker名字、Broker Id、Broker连接的Name server地址、集群名字等。
            final BrokerConfig brokerConfig = new BrokerConfig();
            // NettyServerConfig：Broker netty服务端配置类，Broker netty服务端主要用来接收客户端的请求，
            // NettyServerConfig类主要属性包括监听接口、服务工作线程数、接收和发送请求的buffer大小等。
            final NettyServerConfig nettyServerConfig = new NettyServerConfig();
            // NettyClientConfig：netty客户端配置类，用于生产者、消费者这些客户端与Broker进行通信相关配置，
            // 配置属性主要包括客户端工作线程数、客户端回调线程数、连接超时时间、连接不活跃时间间隔、连接最大闲置时间等。
            final NettyClientConfig nettyClientConfig = new NettyClientConfig();

            nettyClientConfig.setUseTLS(Boolean.parseBoolean(System.getProperty(TLS_ENABLE,
                String.valueOf(TlsSystemConfig.tlsMode == TlsMode.ENFORCING))));
            // 设置netty监听接口
            nettyServerConfig.setListenPort(10911);

            // 消息存储配置类，
            // 配置属性包括存储路径、commitlog文件存储目录、CommitLog文件的大小、CommitLog刷盘的时间间隔等。
            final MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
            // 如果broker的角色是slave，设置命中消息在内存的最大比例
            if (BrokerRole.SLAVE == messageStoreConfig.getBrokerRole()) {
                int ratio = messageStoreConfig.getAccessMessageInMemoryMaxRatio() - 10;
                messageStoreConfig.setAccessMessageInMemoryMaxRatio(ratio);
            }
            // 创建BrokerConfig
            // 如果命令中包含字母c，则读取配置文件，将配置文件的内容设置到配置类中
            //  如果命令行中存在命令参数为‘c’（c是configFile的缩写），那么就读取configFile文件的内容，
            //  将configFile配置文件的配置项映射到BrokerConfig、NettyServerConfig、NettyClientConfig、MessageStoreConfig配置类中。
            if (commandLine.hasOption('c')) {
                String file = commandLine.getOptionValue('c');
                if (file != null) {
                    configFile = file;
                    InputStream in = new BufferedInputStream(new FileInputStream(file));
                    properties = new Properties();
                    properties.load(in);
                    // 读取配置文件的中namesrv地址
                    properties2SystemEnv(properties);
                    // 将配置文件中的配置项映射到配置类中去
                    MixAll.properties2Object(properties, brokerConfig);
                    MixAll.properties2Object(properties, nettyServerConfig);
                    MixAll.properties2Object(properties, nettyClientConfig);
                    MixAll.properties2Object(properties, messageStoreConfig);
                    // 设置配置broker配置文件
                    BrokerPathConfigHelper.setBrokerConfigPath(file);
                    in.close();
                }
            }
            // 设置broker配置类
            MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), brokerConfig);
            // 如果broker配置文件的rocketmqHome属性值为null，直接结束程序
            // RocketmqHome是Borker相关配置保存的文件目录，如果为空则直接退出程序，启动Broker失败
            if (null == brokerConfig.getRocketmqHome()) {
                System.out.printf("Please set the %s variable in your environment to match the location of the RocketMQ installation", MixAll.ROCKETMQ_HOME_ENV);
                System.exit(-2);
            }
            // 如果name server服务器的地址不为null
            String namesrvAddr = brokerConfig.getNamesrvAddr();
            if (null != namesrvAddr) {
                try {
                    // namesrvAddr是以";"分割的多个地址
                    String[] addrArray = namesrvAddr.split(";");
                    for (String addr : addrArray) {
                        //每个地址是ip:port的形式，检测下是否形如ip:port的形式
                        RemotingUtil.string2SocketAddress(addr);
                    }
                } catch (Exception e) {
                    System.out.printf(
                        "The Name Server Address[%s] illegal, please set it as follows, \"127.0.0.1:9876;192.168.0.1:9876\"%n",
                        namesrvAddr);
                    System.exit(-3);
                }
            }
            // 设置BrokerId，broker master 的BrokerId设置为0,broker slave 设置为大于0的值
            switch (messageStoreConfig.getBrokerRole()) {
                case ASYNC_MASTER:
                case SYNC_MASTER:
                    brokerConfig.setBrokerId(MixAll.MASTER_ID);
                    break;
                case SLAVE:
                    //如果小于等于0，退出程序
                    if (brokerConfig.getBrokerId() <= 0) {
                        System.out.printf("Slave's brokerId must be > 0");
                        System.exit(-3);
                    }

                    break;
                default:
                    break;
            }

            messageStoreConfig.setHaListenPort(nettyServerConfig.getListenPort() + 1);
            LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
            JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(lc);
            lc.reset();
            configurator.doConfigure(brokerConfig.getRocketmqHome() + "/conf/logback_broker.xml");
            //printConfigItem 打印配置信息
            // 先判断命令行参数是否包含字母‘p’（printConfigItem是我缩写），
            // 如果包含字母‘p’，则打印配置信息，否则判断下命令行是否包含字母‘m’，
            // 则打印被@ImportantField注解的配置属性，也就是重要的配置属性。
            // 最后，不管命令行中是否存在字母‘p’或者字母‘m’，都打印配置信息。
            if (commandLine.hasOption('p')) {
                InternalLogger console = InternalLoggerFactory.getLogger(LoggerName.BROKER_CONSOLE_NAME);
                MixAll.printObjectProperties(console, brokerConfig);
                MixAll.printObjectProperties(console, nettyServerConfig);
                MixAll.printObjectProperties(console, nettyClientConfig);
                MixAll.printObjectProperties(console, messageStoreConfig);
                System.exit(0);
                //printImportantConfig 打印重要配置信息
            } else if (commandLine.hasOption('m')) {
                InternalLogger console = InternalLoggerFactory.getLogger(LoggerName.BROKER_CONSOLE_NAME);
                MixAll.printObjectProperties(console, brokerConfig, true);
                MixAll.printObjectProperties(console, nettyServerConfig, true);
                MixAll.printObjectProperties(console, nettyClientConfig, true);
                MixAll.printObjectProperties(console, messageStoreConfig, true);
                System.exit(0);
            }
            // 打印配置信息
            log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
            MixAll.printObjectProperties(log, brokerConfig);
            MixAll.printObjectProperties(log, nettyServerConfig);
            MixAll.printObjectProperties(log, nettyClientConfig);
            MixAll.printObjectProperties(log, messageStoreConfig);

            // 创建并初始化Broker控制器
            // 创建BrokerController（broker 控制器）
            final BrokerController controller = new BrokerController(
                brokerConfig,
                nettyServerConfig,
                nettyClientConfig,
                messageStoreConfig);
            // remember all configs to prevent discard
            // 将所有的配置信息保存在内存
            controller.getConfiguration().registerConfig(properties);
            // 用其initialize方法对BrokerController进行初始化
            boolean initResult = controller.initialize();
            // 如果初始化失败，则退出
            if (!initResult) {
                controller.shutdown();
                System.exit(-3);
            }

            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                private volatile boolean hasShutdown = false;
                private AtomicInteger shutdownTimes = new AtomicInteger(0);

                @Override
                public void run() {
                    synchronized (this) {
                        log.info("Shutdown hook was invoked, {}", this.shutdownTimes.incrementAndGet());
                        if (!this.hasShutdown) {
                            this.hasShutdown = true;
                            long beginTime = System.currentTimeMillis();
                            controller.shutdown();
                            long consumingTimeTotal = System.currentTimeMillis() - beginTime;
                            log.info("Shutdown hook over, consuming total time(ms): {}", consumingTimeTotal);
                        }
                    }
                }
            }, "ShutdownHook"));

            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    private static void properties2SystemEnv(Properties properties) {
        if (properties == null) {
            return;
        }
        String rmqAddressServerDomain = properties.getProperty("rmqAddressServerDomain", MixAll.WS_DOMAIN_NAME);
        String rmqAddressServerSubGroup = properties.getProperty("rmqAddressServerSubGroup", MixAll.WS_DOMAIN_SUBGROUP);
        System.setProperty("rocketmq.namesrv.domain", rmqAddressServerDomain);
        System.setProperty("rocketmq.namesrv.domain.subgroup", rmqAddressServerSubGroup);
    }

    private static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("c", "configFile", true, "Broker config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "printConfigItem", false, "Print all config item");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("m", "printImportantConfig", false, "Print important config item");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }
}
