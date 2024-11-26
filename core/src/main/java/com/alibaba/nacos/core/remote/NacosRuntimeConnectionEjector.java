/*
 *
 * Copyright 1999-2021 Alibaba Group Holding Ltd.
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
 *
 */

package com.alibaba.nacos.core.remote;

import com.alibaba.nacos.api.remote.RequestCallBack;
import com.alibaba.nacos.api.remote.request.ClientDetectionRequest;
import com.alibaba.nacos.api.remote.response.Response;
import com.alibaba.nacos.common.remote.exception.ConnectionAlreadyClosedException;
import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.plugin.control.Loggers;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * nacos runtime connection ejector.
 *
 * @author shiyiyue
 */
public class NacosRuntimeConnectionEjector extends RuntimeConnectionEjector {
    
    public NacosRuntimeConnectionEjector() {
    
    }
    
    /**
     * eject connections on runtime.
     */
    public void doEject() {
        // 删除过时的连接
        ejectOutdatedConnection();
        // 删除过载连接
        ejectOverLimitConnection();
    }
    
    /**
     * eject the outdated connection.
     */
    private void ejectOutdatedConnection() {
        try {
            Map<String, Connection> connections = connectionManager.connections;
            // 记录过时连接
            Set<String> outDatedConnections = new HashSet<>();
            long now = System.currentTimeMillis();
            //outdated connections collect.
            for (Map.Entry<String, Connection> entry : connections.entrySet()) {
                Connection client = entry.getValue();
              
                if (now - client.getMetaInfo().getLastActiveTime() >= KEEP_ALIVE_TIME) {
                    // 最近活跃时间超过20s
                    outDatedConnections.add(client.getMetaInfo().getConnectionId());
                } else if (client.getMetaInfo().pushQueueBlockTimesLastOver(300 * 1000)) {
                    // 如果连接的推送队列阻塞时间超过 300 秒，也将其标记为过时
                    outDatedConnections.add(client.getMetaInfo().getConnectionId());
                }
            }
            // 检查过时连接集合
            if (CollectionUtils.isNotEmpty(outDatedConnections)) {
                // 存储成功的连接
                Set<String> successConnections = new HashSet<>();
                // 使用 CountDownLatch 来等待所有连接检测完成
                final CountDownLatch latch = new CountDownLatch(outDatedConnections.size());
                for (String outDateConnectionId : outDatedConnections) {
                    try {
                        Connection connection = connectionManager.getConnection(outDateConnectionId);
                        if (connection != null) {
                            // 发送客户端检测请求
                            ClientDetectionRequest clientDetectionRequest = new ClientDetectionRequest();
                            connection.asyncRequest(clientDetectionRequest, new RequestCallBack() {
                                @Override
                                public Executor getExecutor() {
                                    return null;
                                }
                                
                                @Override
                                public long getTimeout() {
                                    return 5000L;
                                }
                                
                                @Override
                                public void onResponse(Response response) {
                                    latch.countDown();
                                    // 如果检测成功，更新连接的最近活跃时间，并将其从过时连接集合中移除
                                    if (response != null && response.isSuccess()) {
                                        connection.freshActiveTime();
                                        successConnections.add(outDateConnectionId);
                                    }
                                }
                                
                                @Override
                                public void onException(Throwable e) {
                                    latch.countDown();
                                }
                            });
                            
                        } else {
                            latch.countDown();
                        }
                        
                    } catch (ConnectionAlreadyClosedException e) {
                        latch.countDown();
                    } catch (Exception e) {
                        latch.countDown();
                    }
                }
                // 等待所有连接检测完成
                latch.await(5000L, TimeUnit.MILLISECONDS);
                
                for (String outDateConnectionId : outDatedConnections) {
                    // 如果连接仍然检测失败，将其注销
                    if (!successConnections.contains(outDateConnectionId)) {
                        connectionManager.unregister(outDateConnectionId);
                    }
                }
            }
        } catch (Throwable e) {
            Loggers.CONNECTION.error("Error occurs during connection check... ", e);
        }
    }
    
    /**
     * 删除过载的连接
     */
    private void ejectOverLimitConnection() {
        // 如果没有设置负载限制，则直接返回
        if (getLoadClient() > 0) {
            try {
                // 当前连接
                int currentConnectionCount = connectionManager.getCurrentConnectionCount();
                int ejectingCount = currentConnectionCount - getLoadClient();
                // if overload
                if (ejectingCount > 0) {
                    // 当连接重置时我们可以修改连接映射
                    // 避免并发修改异常，为 ids 快照创建新集
                    Set<String> ids = new HashSet<>(connectionManager.connections.keySet());
                    for (String id : ids) {
                        if (ejectingCount > 0) {
                            // check sdk
                            Connection connection = connectionManager.getConnection(id);
                            if (connection != null && connection.getMetaInfo().isSdkSource()) {
                                if (connectionManager.loadSingle(id, redirectAddress)) {
                                    ejectingCount--;
                                }
                            }
                        } else {
                            // reach the count
                            break;
                        }
                    }
                }
            } catch (Throwable e) {
                Loggers.CONNECTION.error("Error occurs during connection overLimit... ", e);
            }
            // reset
            setRedirectAddress(null);
            setLoadClient(-1);
        }
    }
    
    @Override
    public String getName() {
        return "nacos";
    }
}
