package org.apache.rocketmq.namesrv;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;

/**
 * 在启动NameServer 时，
 * 可以先使用.／mqnameserver -c configFile -p 打印当前配置属性。
 * telnet 127.0.0.1 9876
 */
public class NamesrvStartupTest {

    public static void main1(String[] args) {
        NamesrvStartup.main(args);
    }

    /**
     * 启动日志：
     * [NettyEventExecutor] INFO  RocketmqRemoting - NettyEventExecutor service started
     * [FileWatchService] INFO  RocketmqCommon - FileWatchService service started
     * 命令行中输入 telnet 127.0.0.1 9876 ，看看是否能连接上 RocketMQ Namesrv
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // NamesrvConfig 配置
        final NamesrvConfig namesrvConfig = new NamesrvConfig();
        // NettyServerConfig 配置
        final NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setListenPort(9876); // 设置端口
        // 创建 NamesrvController 对象，并启动
        NamesrvController namesrvController = new NamesrvController(namesrvConfig, nettyServerConfig);
        namesrvController.initialize();
        namesrvController.start();
        // 睡觉，就不起来
        Thread.sleep(DateUtils.MILLIS_PER_DAY);
    }
}
