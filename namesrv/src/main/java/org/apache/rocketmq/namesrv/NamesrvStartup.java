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
package org.apache.rocketmq.namesrv;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.Callable;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.srvutil.ShutdownHookThread;
import org.slf4j.LoggerFactory;

/**
 * NameServer充当路由信息的提供者。
 * 生产者或消费者能够通过NameServer查找各Topic相应的Broker IP列表。
 * 多个Namesrver实例组成集群，但相互独立，没有信息交换。
 */

/**
 * 主要流程
 * Step1：通过命令行中获取配置。赋值给NamesrvConfig和NettyServerConfig类。
 * Step2：根据配置类NamesrvConfig和NettyServerConfig构造一个NamesrvController实例。
 */
public class NamesrvStartup {

    private static InternalLogger log;
    private static Properties properties = null;
    private static CommandLine commandLine = null;

    // NamesrvStartup类就是NameServer服务器启动的启动类，
    // NamesrvStartup类中有一个main启动类，main方法调用main0，main0主要流程代码
    public static void main(String[] args) {
        main0(args);
    }
    // main0 方法的主要作用就是创建Name Server服务器的控制器，
    // 并且启动Name Server服务器的控制器。
    public static NamesrvController main0(String[] args) {

        try {
            //创建NameServer服务器的控制器
            NamesrvController controller = createNamesrvController(args);
            //启动NameServer服务器的控制器
            start(controller);
            String tip = "The Name Server boot success. serializeType=" + RemotingCommand.getSerializeTypeConfigInThisServer();
            //打印启动日志
            log.info(tip);
            System.out.printf("%s%n", tip);
            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    /**
     * createNamesrvController方法主要做了几件事，
     * 读取和解析配置信息，包括Name Server服务的配置信息、Netty 服务器的配置信息、
     * 打印读取或者解析的配置信息、保存配置信息到本地文件中，
     * 以及根据namesrvConfig配置和nettyServerConfig配置作为参数创建nameServer 服务器的控制器。
     */
    public static NamesrvController createNamesrvController(String[] args) throws IOException, JoranException {
        //设置rocketMQ的版本信息，REMOTING_VERSION_KEY的值为：rocketmq.remoting.version，CURRENT_VERSION的值为：V4_5_1
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));
        //PackageConflictDetect.detectFastjson();
        //构建命令行，添加帮助命令和Name Server的提示命令，将createNamesrvController方法的args参数进行解析
        //构造org.apache.commons.cli.Options,并添加-h -n参数，-h参数是打印帮助信息，-n参数是指定namesrvAddr
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        //初始化commandLine，并在options中添加-c -p参数，-c指定nameserver的配置文件路径，-p标识打印配置信息
        commandLine = ServerUtil.parseCmdLine("mqnamesrv", args, buildCommandlineOptions(options), new PosixParser());
        if (null == commandLine) {
            System.exit(-1);
            return null;
        }
        //nameserver配置类，业务参数
        final NamesrvConfig namesrvConfig = new NamesrvConfig();
        //netty服务器配置类，网络参数
        final NettyServerConfig nettyServerConfig = new NettyServerConfig();
        //设置nameserver的端口号
        nettyServerConfig.setListenPort(9876);
        /**
         * 参数来源有如下两种方式
         * 1. -c configFile 通过，c 命令指定配置文件的路径。
         * 2. 使用“--属性名 属性值”，例如--listenPort 9876 。
         *
         */
        //命令带有-c参数，说明指定配置文件，需要根据配置文件路径读取配置文件内容，
        // 并将文件中配置信息赋值给NamesrvConfig和NettyServerConfig
        if (commandLine.hasOption('c')) {
            String file = commandLine.getOptionValue('c');
            if (file != null) {
                InputStream in = new BufferedInputStream(new FileInputStream(file));
                properties = new Properties();
                properties.load(in);
                // 首先将构建的命令行转换为Properties，
                // 然后将通过反射的方式将Properties的属性转换为namesrvConfig的配置项和配置值。
                MixAll.properties2Object(properties, namesrvConfig);

                MixAll.properties2Object(properties, nettyServerConfig);
                //设置配置文件路径
                namesrvConfig.setConfigStorePath(file);

                System.out.printf("load config properties file OK, %s%n", file);
                in.close();
            }
        }
        //命令行带有-p，说明是打印参数的命令，
        // 那么就打印出NamesrvConfig和NettyServerConfig的属性。
        // 在启动NameServer时可以
        // 先使用./mqnameserver -c configFile -p打印当前加载的配置属性
        if (commandLine.hasOption('p')) {
            //打印nameServer 服务器配置类和 netty 服务器配置类的配置信息
            InternalLogger console = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_CONSOLE_NAME);
            MixAll.printObjectProperties(console, namesrvConfig);
            MixAll.printObjectProperties(console, nettyServerConfig);
            //打印参数命令不需要启动nameserver服务，只需要打印参数即可
            System.exit(0);
        }
        //解析命令行参数，并加载到namesrvConfig中
        MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), namesrvConfig);
        //检查ROCKETMQ_HOME，不能为空
        if (null == namesrvConfig.getRocketmqHome()) {
            System.out.printf("Please set the %s variable in your environment to match the location of the RocketMQ installation%n", MixAll.ROCKETMQ_HOME_ENV);
            System.exit(-2);
        }
        //初始化logback日志工厂，rocketmq默认使用logback作为日志输出
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(lc);
        lc.reset();
        configurator.doConfigure(namesrvConfig.getRocketmqHome() + "/conf/logback_namesrv.xml");

        log = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

        MixAll.printObjectProperties(log, namesrvConfig);
        MixAll.printObjectProperties(log, nettyServerConfig);
        // 将namesrvConfig和nettyServerConfig作为参数创建nameServer 服务器的控制器
        final NamesrvController controller = new NamesrvController(namesrvConfig, nettyServerConfig);
        //将全局Properties的内容复制到NamesrvController.Configuration.allConfigs中
        // remember all configs to prevent discard
        controller.getConfiguration().registerConfig(properties);

        return controller;
    }

    // start方法主要作用就是进行初始化工作，然后进行启动Name Server控制器
    public static NamesrvController start(final NamesrvController controller) throws Exception {

        if (null == controller) {
            throw new IllegalArgumentException("NamesrvController is null");
        }
        // 初始化nameserver 服务器，如果初始化失败则退出
        boolean initResult = controller.initialize();
        if (!initResult) {
            controller.shutdown();
            System.exit(-3);
        }
        // //添加关闭的钩子，进行内存清理、对象销毁等惭怍
        /**
         * 注册JVM 钩子函数并启动服务器，以便监听Broker、消息生产者的网络请求。
         * 如果代码中使用了线程池，一种优雅停机的方式就是注册一个JVM 钩子函数，
         * 在JVM进程关闭之前，先将线程池关闭，及时释放资源。
         */
        Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(log, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                /**
                 * 优雅停机实现方法，释放资源
                 */
                controller.shutdown();
                return null;
            }
        }));
        // 启动
        controller.start();

        return controller;
    }

    public static void shutdown(final NamesrvController controller) {
        controller.shutdown();
    }

    public static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("c", "configFile", true, "Name server config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "printConfigItem", false, "Print all config item");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    public static Properties getProperties() {
        return properties;
    }
}
