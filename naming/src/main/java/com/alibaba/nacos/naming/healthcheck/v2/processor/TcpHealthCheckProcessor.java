/*
 * Copyright 1999-2020 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.naming.healthcheck.v2.processor;

import com.alibaba.nacos.api.naming.pojo.healthcheck.HealthCheckType;
import com.alibaba.nacos.naming.core.v2.metadata.ClusterMetadata;
import com.alibaba.nacos.naming.core.v2.pojo.HealthCheckInstancePublishInfo;
import com.alibaba.nacos.naming.core.v2.pojo.Service;
import com.alibaba.nacos.naming.healthcheck.v2.HealthCheckTaskV2;
import com.alibaba.nacos.naming.misc.GlobalExecutor;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import com.alibaba.nacos.naming.monitor.MetricsMonitor;
import com.alibaba.nacos.sys.env.EnvUtil;
import org.springframework.stereotype.Component;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.alibaba.nacos.naming.misc.Loggers.SRV_LOG;

/**
 * TCP health check processor for v2.x.
 *
 * <p>Current health check logic is same as v1.x. TODO refactor health check for v2.x.
 *
 * @author xiweng.yy
 */
@Component
public class TcpHealthCheckProcessor implements HealthCheckProcessorV2, Runnable {
    
    public static final String TYPE = HealthCheckType.TCP.name();
    
    public static final int CONNECT_TIMEOUT_MS = 500;
    
    /**
     * this value has been carefully tuned, do not modify unless you're confident.
     */
    private static final int NIO_THREAD_COUNT = EnvUtil.getAvailableProcessors(0.5);
    
    /**
     * because some hosts doesn't support keep-alive connections, disabled temporarily.
     */
    private static final long TCP_KEEP_ALIVE_MILLIS = 0;
    
    private final HealthCheckCommonV2 healthCheckCommon;
    
    private final SwitchDomain switchDomain;
    
    private final Map<String, BeatKey> keyMap = new ConcurrentHashMap<>();
    
    private final BlockingQueue<Beat> taskQueue = new LinkedBlockingQueue<>();
    
    private final Selector selector;
    
    public TcpHealthCheckProcessor(HealthCheckCommonV2 healthCheckCommon, SwitchDomain switchDomain) {
        this.healthCheckCommon = healthCheckCommon;
        this.switchDomain = switchDomain;
        try {
            selector = Selector.open();
            GlobalExecutor.submitTcpCheck(this);
        } catch (Exception e) {
            throw new IllegalStateException("Error while initializing SuperSense(TM).");
        }
    }
    
    @Override
    public void process(HealthCheckTaskV2 task, Service service, ClusterMetadata metadata) {
        HealthCheckInstancePublishInfo instance = (HealthCheckInstancePublishInfo) task.getClient()
                .getInstancePublishInfo(service);
        if (null == instance) {
            return;
        }
        // TODO handle marked(white list) logic like v1.x.
        if (!instance.tryStartCheck()) {
            SRV_LOG.warn("[HEALTH-CHECK-V2] tcp check started before last one finished, service: {} : {} : {}:{}",
                    service.getNameSpaceGroupedServiceName(), instance.getCluster(), instance.getIp(), instance.getPort());
            healthCheckCommon
                    .reEvaluateCheckRT(task.getCheckRtNormalized() * 2, task, switchDomain.getTcpHealthParams());
            return;
        }
        // 向taskQueue中添加一个心跳对象
        taskQueue.add(new Beat(task, service, metadata, instance));
        MetricsMonitor.getTcpHealthCheckMonitor().incrementAndGet();
    }
    
    @Override
    public String getType() {
        return TYPE;
    }
    
    private void processTask() throws Exception {
        Collection<Callable<Void>> tasks = new LinkedList<>();
        do {
            Beat beat = taskQueue.poll(CONNECT_TIMEOUT_MS / 2, TimeUnit.MILLISECONDS);
            if (beat == null) {
                return;
            }
            
            tasks.add(new TaskProcessor(beat));
        } while (taskQueue.size() > 0 && tasks.size() < NIO_THREAD_COUNT * 64);
        
        for (Future<?> f : GlobalExecutor.invokeAllTcpSuperSenseTask(tasks)) {
            f.get();
        }
    }
    
    @Override
    public void run() {
        while (true) {
            try {
                processTask();
                // 检查是否有任何通道（如 SocketChannel）准备好进行 I/O 操作
                int readyCount = selector.selectNow();
                if (readyCount <= 0) {
                    continue;
                }
                // 获取所有已准备好的 SelectionKey 对象，并创建一个迭代器
                Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
                while (iter.hasNext()) {
                    SelectionKey key = iter.next();
                    // 从 selectedKeys 集合中移除当前键。这是必要的，因为一旦处理完毕，就不再需要该键，以避免重复处理
                    iter.remove();
                    // 将处理逻辑提交给全局执行器，使用 PostProcessor 处理每个连接的状态变化
                    GlobalExecutor.executeTcpSuperSense(new PostProcessor(key));
                }
            } catch (Throwable e) {
                SRV_LOG.error("[HEALTH-CHECK-V2] error while processing NIO task", e);
            }
        }
    }
    
    public class PostProcessor implements Runnable {
        
        SelectionKey key;
        
        public PostProcessor(SelectionKey key) {
            this.key = key;
        }
        
        @Override
        public void run() {
            // 从选择键中获取与之关联的 Beat 对象，用于检查心跳状态
            Beat beat = (Beat) key.attachment();
            //  获取与选择键关联的 SocketChannel，用于进行网络操作。
            SocketChannel channel = (SocketChannel) key.channel();
            try {
                // 如果 beat 对象被标记为不健康，取消选择键并关闭通道，同时调用 finishCheck() 方法标记检查完成
                if (!beat.isHealthy()) {
                    // 无效beat表示该服务器不再负责当前的服务
                    key.cancel();
                    key.channel().close();
                    
                    beat.finishCheck();
                    return;
                }
                // 如果选择键有效且可以连接，调用 finishConnect() 方法完成连接，并记录健康检查结果
                if (key.isValid() && key.isConnectable()) {
                    //connected
                    channel.finishConnect();
                    beat.finishCheck(true, false, System.currentTimeMillis() - beat.getTask().getStartTime(),
                            "tcp:ok+");
                }
                
                if (key.isValid() && key.isReadable()) {
                    //disconnected
                    ByteBuffer buffer = ByteBuffer.allocate(128);
                    if (channel.read(buffer) == -1) {
                        key.cancel();
                        key.channel().close();
                    } else {
                        // not terminate request, ignore
                        SRV_LOG.warn(
                                "Tcp check ok, but the connected server responses some msg. Connection won't be closed.");
                    }
                }
            } catch (ConnectException e) {
                // unable to connect, possibly port not opened
                beat.finishCheck(false, true, switchDomain.getTcpHealthParams().getMax(),
                        "tcp:unable2connect:" + e.getMessage());
            } catch (Exception e) {
                beat.finishCheck(false, false, switchDomain.getTcpHealthParams().getMax(),
                        "tcp:error:" + e.getMessage());
                
                try {
                    key.cancel();
                    key.channel().close();
                } catch (Exception ignore) {
                }
            }
        }
    }
    
    private class Beat {
        
        private final HealthCheckTaskV2 task;
        
        private final Service service;
        
        private final ClusterMetadata metadata;
        
        private final HealthCheckInstancePublishInfo instance;
        
        long startTime = System.currentTimeMillis();
        
        public Beat(HealthCheckTaskV2 task, Service service, ClusterMetadata metadata,
                HealthCheckInstancePublishInfo instance) {
            this.task = task;
            this.service = service;
            this.metadata = metadata;
            this.instance = instance;
        }
        
        public void setStartTime(long time) {
            startTime = time;
        }
        
        public long getStartTime() {
            return startTime;
        }
        
        public HealthCheckTaskV2 getTask() {
            return task;
        }
        
        public Service getService() {
            return service;
        }
        
        public ClusterMetadata getMetadata() {
            return metadata;
        }
        
        public HealthCheckInstancePublishInfo getInstance() {
            return instance;
        }
        
        public boolean isHealthy() {
            return System.currentTimeMillis() - startTime < TimeUnit.SECONDS.toMillis(30L);
        }
        
        /**
         * finish check only, no ip state will be changed.
         */
        public void finishCheck() {
            instance.finishCheck();
        }
        
        public void finishCheck(boolean success, boolean now, long rt, String msg) {
            if (success) {
                healthCheckCommon.checkOk(task, service, msg);
            } else {
                if (now) {
                    healthCheckCommon.checkFailNow(task, service, msg);
                } else {
                    healthCheckCommon.checkFail(task, service, msg);
                }
                
                keyMap.remove(toString());
            }
            
            healthCheckCommon.reEvaluateCheckRT(rt, task, switchDomain.getTcpHealthParams());
        }
        
        @Override
        public String toString() {
            return service.getNameSpaceGroupedServiceName() + ":" + instance.getCluster() + ":" + instance.getIp() + ":"
                    + instance.getPort();
        }
        
        @Override
        public int hashCode() {
            return toString().hashCode();
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof Beat)) {
                return false;
            }
            
            return this.toString().equals(obj.toString());
        }
    }
    
    private static class BeatKey {
        
        public SelectionKey key;
        
        public long birthTime;
        
        public BeatKey(SelectionKey key) {
            this.key = key;
            this.birthTime = System.currentTimeMillis();
        }
    }
    
    // 连接超时任务
    private static class TimeOutTask implements Runnable {
        
        SelectionKey key;
        
        public TimeOutTask(SelectionKey key) {
            this.key = key;
        }
        
        @Override
        public void run() {
            if (key != null && key.isValid()) {
                SocketChannel channel = (SocketChannel) key.channel();
                Beat beat = (Beat) key.attachment();
                // 如果网络已连接，则直接返回
                if (channel.isConnected()) {
                    return;
                }
                
                try {
                    // 超时关闭连接
                    channel.finishConnect();
                } catch (Exception ignore) {
                }
                
                try {
                    // 完成健康检查，设置为失败
                    beat.finishCheck(false, false, beat.getTask().getCheckRtNormalized() * 2, "tcp:timeout");
                    key.cancel();
                    key.channel().close();
                } catch (Exception ignore) {
                }
            }
        }
    }
    
    private class TaskProcessor implements Callable<Void> {
        
        private static final int MAX_WAIT_TIME_MILLISECONDS = 500;
        
        Beat beat;
        
        public TaskProcessor(Beat beat) {
            this.beat = beat;
        }
        
        @Override
        public Void call() {
            long waited = System.currentTimeMillis() - beat.getStartTime();
            // 如果等待时间超过500ms，则打印警告日志
            if (waited > MAX_WAIT_TIME_MILLISECONDS) {
                Loggers.SRV_LOG.warn("beat task waited too long: " + waited + "ms");
            }
            
            SocketChannel channel = null;
            try {
                // 服务实例信息
                HealthCheckInstancePublishInfo instance = beat.getInstance();
                
                // 从 keyMap 中获取与 beat 关联的 BeatKey
                BeatKey beatKey = keyMap.get(beat.toString());
                // 如果 beatKey 不为空且 key 有效，则直接返回
                if (beatKey != null && beatKey.key.isValid()) {
                    // 如果键有效且最近的活动时间小于 TCP_KEEP_ALIVE_MILLIS，则表示连接仍然有效，调用 instance.finishCheck() 并返回
                    if (System.currentTimeMillis() - beatKey.birthTime < TCP_KEEP_ALIVE_MILLIS) {
                        instance.finishCheck();
                        return null;
                    }
                    // 如果连接不再有效，取消 SelectionKey 并关闭相关的 SocketChannel
                    beatKey.key.cancel();
                    beatKey.key.channel().close();
                }
                // 打开通道: 创建一个新的 SocketChannel 实例。
                // 非阻塞模式: 将通道设置为非阻塞模式，以允许异步 I/O 操作
                channel = SocketChannel.open();
                channel.configureBlocking(false);
                // only by setting this can we make the socket close event asynchronous
                channel.socket().setSoLinger(false, -1);
                channel.socket().setReuseAddress(true);
                channel.socket().setKeepAlive(true);
                channel.socket().setTcpNoDelay(true);
                
                // 获取元数据
                ClusterMetadata cluster = beat.getMetadata();
                int port = cluster.isUseInstancePortForCheck() ? instance.getPort() : cluster.getHealthyCheckPort();
                // 使用 connect 方法尝试连接到指定的 IP 和端口
                channel.connect(new InetSocketAddress(instance.getIp(), port));
                
                // 注册通道: 将 SocketChannel 注册到 Selector，并设置操作类型为连接和读取。
                SelectionKey key = channel.register(selector, SelectionKey.OP_CONNECT | SelectionKey.OP_READ);
                // 关联 beat: 将 beat 对象附加到选择键上，以便后续处理。
                key.attach(beat);
                // 更新键映射: 将新的 BeatKey 存储到 keyMap 中，以便后续查找。
                keyMap.put(beat.toString(), new BeatKey(key));
                
                // 设置开始时间: 设置 beat 的开始时间为当前时间戳。
                beat.setStartTime(System.currentTimeMillis());
                // 调度超时任务: 使用全局执行器调度一个超时任务，以处理连接超时的情况
                GlobalExecutor
                        .scheduleTcpSuperSenseTask(new TimeOutTask(key), CONNECT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                // 如果出现异常，设置健康检查失败，并关闭通道
                beat.finishCheck(false, false, switchDomain.getTcpHealthParams().getMax(),
                        "tcp:error:" + e.getMessage());
                
                if (channel != null) {
                    try {
                        channel.close();
                    } catch (Exception ignore) {
                    }
                }
            }
            
            return null;
        }
    }
}
