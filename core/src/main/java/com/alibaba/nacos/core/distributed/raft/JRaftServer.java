/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
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

package com.alibaba.nacos.core.distributed.raft;

import com.alibaba.nacos.common.JustForTest;
import com.alibaba.nacos.common.model.RestResult;
import com.alibaba.nacos.common.utils.ConvertUtils;
import com.alibaba.nacos.common.utils.InternetAddressUtil;
import com.alibaba.nacos.common.utils.LoggerUtils;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.common.utils.ThreadUtils;
import com.alibaba.nacos.consistency.ProtoMessageUtil;
import com.alibaba.nacos.consistency.RequestProcessor;
import com.alibaba.nacos.consistency.SerializeFactory;
import com.alibaba.nacos.consistency.Serializer;
import com.alibaba.nacos.consistency.cp.RequestProcessor4CP;
import com.alibaba.nacos.consistency.entity.ReadRequest;
import com.alibaba.nacos.consistency.entity.Response;
import com.alibaba.nacos.consistency.exception.ConsistencyException;
import com.alibaba.nacos.core.distributed.raft.exception.DuplicateRaftGroupException;
import com.alibaba.nacos.core.distributed.raft.exception.JRaftException;
import com.alibaba.nacos.core.distributed.raft.exception.NoLeaderException;
import com.alibaba.nacos.core.distributed.raft.exception.NoSuchRaftGroupException;
import com.alibaba.nacos.core.distributed.raft.utils.FailoverClosure;
import com.alibaba.nacos.core.distributed.raft.utils.FailoverClosureImpl;
import com.alibaba.nacos.core.distributed.raft.utils.JRaftConstants;
import com.alibaba.nacos.core.distributed.raft.utils.JRaftUtils;
import com.alibaba.nacos.core.distributed.raft.utils.RaftExecutor;
import com.alibaba.nacos.core.distributed.raft.utils.RaftOptionsBuilder;
import com.alibaba.nacos.core.monitor.MetricsMonitor;
import com.alibaba.nacos.core.utils.Loggers;
import com.alibaba.nacos.sys.env.EnvUtil;
import com.alipay.sofa.jraft.CliService;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.RaftServiceFactory;
import com.alipay.sofa.jraft.RouteTable;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.core.CliServiceImpl;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.rpc.InvokeCallback;
import com.alipay.sofa.jraft.rpc.RpcProcessor;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.alipay.sofa.jraft.util.Endpoint;
import com.google.protobuf.Message;
import org.springframework.util.CollectionUtils;

import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

/**
 * JRaft server instance, away from Spring IOC management.
 *
 * <p>
 * Why do we need to create a raft group based on the value of LogProcessor group (), that is, each function module has
 * its own state machine. Because each LogProcessor corresponds to a different functional module, such as Nacos's naming
 * module and config module, these two modules are independent of each other and do not affect each other. If we have
 * only one state machine, it is equal to the log of all functional modules The processing is loaded together. Any
 * module that has an exception during the log processing and a long block operation will affect the normal operation of
 * other functional modules.
 * </p>
 *
 * @author <a href="mailto:liaochuntao@live.com">liaochuntao</a>
 */
@SuppressWarnings("all")
public class JRaftServer {
    
    // Existential life cycle
    
    private RpcServer rpcServer;
    
    private CliClientServiceImpl cliClientService;
    
    private CliService cliService;
    
    // Ordinary member variable
    
    private Map<String, RaftGroupTuple> multiRaftGroup = new ConcurrentHashMap<>();
    
    private volatile boolean isStarted = false;
    
    private volatile boolean isShutdown = false;
    
    private Configuration conf;
    
    private RpcProcessor userProcessor;
    
    private NodeOptions nodeOptions;
    
    private Serializer serializer;
    
    private Collection<RequestProcessor4CP> processors = Collections.synchronizedSet(new HashSet<>());
    
    private String selfIp;
    
    private int selfPort;
    
    private RaftConfig raftConfig;
    
    private PeerId localPeerId;
    
    private int failoverRetries;
    
    private int rpcRequestTimeoutMs;
    
    public JRaftServer() {
        this.conf = new Configuration();
    }
    
    public void setFailoverRetries(int failoverRetries) {
        this.failoverRetries = failoverRetries;
    }
    
    void init(RaftConfig config) {
        this.raftConfig = config;
        this.serializer = SerializeFactory.getDefault();
        Loggers.RAFT.info("Initializes the Raft protocol, raft-config info : {}", config);
        // 初始化 RaftExecutor
        RaftExecutor.init(config);
        // 从 config 中获取自身成员地址，并解析 IP 和端口,如：192.168.20.57:7852
        final String self = config.getSelfMember();
        String[] info = InternetAddressUtil.splitIPPortStr(self);
        selfIp = info[0];
        selfPort = Integer.parseInt(info[1]);
        localPeerId = PeerId.parsePeer(self);
        nodeOptions = new NodeOptions();
        
        // Set the election timeout time. The default is 5 seconds. 设置选举超时时间
        int electionTimeout = Math.max(ConvertUtils.toInt(config.getVal(RaftSysConstants.RAFT_ELECTION_TIMEOUT_MS),
                RaftSysConstants.DEFAULT_ELECTION_TIMEOUT), RaftSysConstants.DEFAULT_ELECTION_TIMEOUT);
        
        rpcRequestTimeoutMs = ConvertUtils.toInt(raftConfig.getVal(RaftSysConstants.RAFT_RPC_REQUEST_TIMEOUT_MS),
                RaftSysConstants.DEFAULT_RAFT_RPC_REQUEST_TIMEOUT_MS);
        
        nodeOptions.setSharedElectionTimer(true);
        nodeOptions.setSharedVoteTimer(true);
        nodeOptions.setSharedStepDownTimer(true);
        nodeOptions.setSharedSnapshotTimer(true);
        
        nodeOptions.setElectionTimeoutMs(electionTimeout);
        RaftOptions raftOptions = RaftOptionsBuilder.initRaftOptions(raftConfig);
        nodeOptions.setRaftOptions(raftOptions);
        // open jraft node metrics record function
        nodeOptions.setEnableMetrics(true);
        
        CliOptions cliOptions = new CliOptions();
        
        this.cliService = RaftServiceFactory.createAndInitCliService(cliOptions);
        this.cliClientService = (CliClientServiceImpl) ((CliServiceImpl) this.cliService).getCliClientService();
    }
    
    // 线程安全启动Server
    synchronized void start() {
        if (!isStarted) {
            Loggers.RAFT.info("========= The raft protocol is starting... =========");
            try {
                // init raft group node
                com.alipay.sofa.jraft.NodeManager raftNodeManager = com.alipay.sofa.jraft.NodeManager.getInstance();
                // 初始化 Raft 组成员
                for (String address : raftConfig.getMembers()) {
                    // 获取配置中的所有成员地址，并为每个地址创建一个 PeerId 对象。
                    // PeerId 表示一个 raft 协议的参与者（leader/follower/candidate etc.)， 它由三元素组成： ip:port:index
                    PeerId peerId = PeerId.parsePeer(address);
                    // 将这些 PeerId 对象添加到配置中，并通知 NodeManager 添加相应的地址
                    conf.addPeer(peerId);
                    raftNodeManager.addAddress(peerId.getEndpoint());
                }
                // 使用 nodeOptions 设置 Raft 组的初始配置
                nodeOptions.setInitialConf(conf);
                // 初始化 RPC 服务器，并传入当前实例和本地 PeerId
                rpcServer = JRaftUtils.initRpcServer(this, localPeerId);
                
                if (!this.rpcServer.init(null)) {
                    Loggers.RAFT.error("Fail to init [BaseRpcServer].");
                    throw new RuntimeException("Fail to init [BaseRpcServer].");
                }
                
                // Initialize multi raft group service framework
                isStarted = true;
                // 创建多 Raft 组服务框架
                createMultiRaftGroup(processors);
                Loggers.RAFT.info("========= The raft protocol start finished... =========");
            } catch (Exception e) {
                Loggers.RAFT.error("raft protocol start failure, cause: ", e);
                throw new JRaftException(e);
            }
        }
    }
    
    synchronized void createMultiRaftGroup(Collection<RequestProcessor4CP> processors) {
        // There is no reason why the LogProcessor cannot be processed because of the synchronization
        if (!this.isStarted) {
            this.processors.addAll(processors);
            return;
        }
        // 定义 parentPath 为 nacos.home下的data/protocol/raft 目录
        final String parentPath = Paths.get(EnvUtil.getNacosHome(), "data/protocol/raft").toString();
        
        for (RequestProcessor4CP processor : processors) {
            // 获取处理器的 groupName, 如：naming_persistent_service_v2
            final String groupName = processor.group();
            // 检查 multiRaftGroup 中是否已存在该 groupName，如果存在则抛出 DuplicateRaftGroupException。
            if (multiRaftGroup.containsKey(groupName)) {
                throw new DuplicateRaftGroupException(groupName);
            }
            
            // 复制当前的 Configuration 和 NodeOptions
            // Ensure that each Raft Group has its own configuration and NodeOptions
            Configuration configuration = conf.copy();
            NodeOptions copy = nodeOptions.copy();
            // 初始化目录
            JRaftUtils.initDirectory(parentPath, groupName, copy);
            
            // 创建 NacosStateMachine 并设置到 NodeOptions 中
            // 在这里，LogProcessor被传递给StateMachine，当StateMachine触发onApply时，实际调用LogProcessor的onApply
            // 比如调用PersistentClientOperationServiceImpl的onApply去注册实例
            NacosStateMachine machine = new NacosStateMachine(this, processor);
            
            copy.setFsm(machine);
            copy.setInitialConf(configuration);
            
            // Set snapshot interval, default 1800 seconds 设置快照间隔，默认1800秒
            int doSnapshotInterval = ConvertUtils.toInt(raftConfig.getVal(RaftSysConstants.RAFT_SNAPSHOT_INTERVAL_SECS),
                    RaftSysConstants.DEFAULT_RAFT_SNAPSHOT_INTERVAL_SECS);
            
            // If the business module does not implement a snapshot processor, cancel the snapshot
            doSnapshotInterval = CollectionUtils.isEmpty(processor.loadSnapshotOperate()) ? 0 : doSnapshotInterval;
            
            copy.setSnapshotIntervalSecs(doSnapshotInterval);
            Loggers.RAFT.info("create raft group : {}", groupName);
            // 创建 RaftGroupService 并启动节点
            RaftGroupService raftGroupService = new RaftGroupService(groupName, localPeerId, copy, rpcServer, true);
    
            // Because BaseRpcServer has been started before, it is not allowed to start again here
            Node node = raftGroupService.start(false);
            machine.setNode(node);
            // 更新 RouteTable 配置
            RouteTable.getInstance().updateConfiguration(groupName, configuration);
            
            // 注册自己到集群中
            RaftExecutor.executeByCommon(() -> registerSelfToCluster(groupName, localPeerId, configuration));
            
            // Turn on the leader auto refresh for this group
            Random random = new Random();
            long period = nodeOptions.getElectionTimeoutMs() + random.nextInt(5 * 1000);
            // 设置定时任务定期刷新路由表
            RaftExecutor.scheduleRaftMemberRefreshJob(() -> refreshRouteTable(groupName),
                    nodeOptions.getElectionTimeoutMs(), period, TimeUnit.MILLISECONDS);
            // 将 Raft 组信息存储到 multiRaftGroup 中
            multiRaftGroup.put(groupName, new RaftGroupTuple(node, processor, raftGroupService, machine));
        }
    }
    
    CompletableFuture<Response> get(final ReadRequest request) {
        final String group = request.getGroup();
        CompletableFuture<Response> future = new CompletableFuture<>();
        final RaftGroupTuple tuple = findTupleByGroup(group);
        if (Objects.isNull(tuple)) {
            future.completeExceptionally(new NoSuchRaftGroupException(group));
            return future;
        }
        final Node node = tuple.node;
        final RequestProcessor processor = tuple.processor;
        try {
            node.readIndex(BytesUtil.EMPTY_BYTES, new ReadIndexClosure() {
                @Override
                public void run(Status status, long index, byte[] reqCtx) {
                    if (status.isOk()) {
                        try {
                            Response response = processor.onRequest(request);
                            future.complete(response);
                        } catch (Throwable t) {
                            MetricsMonitor.raftReadIndexFailed();
                            future.completeExceptionally(new ConsistencyException(
                                    "The conformance protocol is temporarily unavailable for reading", t));
                        }
                        return;
                    }
                    MetricsMonitor.raftReadIndexFailed();
                    Loggers.RAFT.error("ReadIndex has error : {}, go to Leader read.", status.getErrorMsg());
                    MetricsMonitor.raftReadFromLeader();
                    readFromLeader(request, future);
                }
            });
            return future;
        } catch (Throwable e) {
            MetricsMonitor.raftReadFromLeader();
            Loggers.RAFT.warn("Raft linear read failed, go to Leader read logic : {}", e.toString());
            // run raft read
            readFromLeader(request, future);
            return future;
        }
    }
    
    public void readFromLeader(final ReadRequest request, final CompletableFuture<Response> future) {
        commit(request.getGroup(), request, future);
    }
    
    public CompletableFuture<Response> commit(final String group, final Message data,
            final CompletableFuture<Response> future) {
        LoggerUtils.printIfDebugEnabled(Loggers.RAFT, "data requested this time : {}", data);
        final RaftGroupTuple tuple = findTupleByGroup(group);
        if (tuple == null) {
            future.completeExceptionally(new IllegalArgumentException("No corresponding Raft Group found : " + group));
            return future;
        }
        
        // 它是一个回调实现，用于处理请求的失败和成功情况。
        FailoverClosureImpl closure = new FailoverClosureImpl(future);
        
        final Node node = tuple.node;
        // 如果当前节点是领导者，直接应用这个请求，调用applyOperation方法处理数据，并使用closure作为回调
        if (node.isLeader()) {
            // The leader node directly applies this request
            applyOperation(node, data, closure);
        } else {
            // Forward to Leader for request processing
            // 如果当前节点不是领导者，将请求转发给领导者处理, 因为只有领导者才能处理写请求
            invokeToLeader(group, data, rpcRequestTimeoutMs, closure);
        }
        return future;
    }
    
    /**
     * Add yourself to the Raft cluster
     *
     * @param groupId raft group
     * @param selfIp  local raft node address
     * @param conf    {@link Configuration} without self info
     * @return join success
     */
    void registerSelfToCluster(String groupId, PeerId selfIp, Configuration conf) {
        while (!isShutdown) {
            try {
                List<PeerId> peerIds = cliService.getPeers(groupId, conf);
                if (peerIds.contains(selfIp)) {
                    return;
                }
                Status status = cliService.addPeer(groupId, conf, selfIp);
                if (status.isOk()) {
                    return;
                }
                Loggers.RAFT.warn("Failed to join the cluster, retry...");
            } catch (Exception e) {
                Loggers.RAFT.error("Failed to join the cluster, retry...", e);
            }
            ThreadUtils.sleep(1_000L);
        }
    }
    
    protected PeerId getLeader(final String raftGroupId) {
        return RouteTable.getInstance().selectLeader(raftGroupId);
    }
    
    synchronized void shutdown() {
        if (isShutdown) {
            return;
        }
        isShutdown = true;
        try {
            Loggers.RAFT.info("========= The raft protocol is starting to close =========");
            
            for (Map.Entry<String, RaftGroupTuple> entry : multiRaftGroup.entrySet()) {
                final RaftGroupTuple tuple = entry.getValue();
                final Node node = tuple.getNode();
                tuple.node.shutdown();
                tuple.raftGroupService.shutdown();
            }
            
            cliService.shutdown();
            cliClientService.shutdown();
            
            Loggers.RAFT.info("========= The raft protocol has been closed =========");
        } catch (Throwable t) {
            Loggers.RAFT.error("There was an error in the raft protocol shutdown, cause: ", t);
        }
    }
    
    public void applyOperation(Node node, Message data, FailoverClosure closure) {
        final Task task = new Task();
        task.setDone(new NacosClosure(data, status -> {
            NacosClosure.NacosStatus nacosStatus = (NacosClosure.NacosStatus) status;
            closure.setThrowable(nacosStatus.getThrowable());
            closure.setResponse(nacosStatus.getResponse());
            closure.run(nacosStatus);
        }));
        
        // add request type field at the head of task data.
        byte[] requestTypeFieldBytes = new byte[2];
        requestTypeFieldBytes[0] = ProtoMessageUtil.REQUEST_TYPE_FIELD_TAG;
        if (data instanceof ReadRequest) {
            requestTypeFieldBytes[1] = ProtoMessageUtil.REQUEST_TYPE_READ;
        } else {
            requestTypeFieldBytes[1] = ProtoMessageUtil.REQUEST_TYPE_WRITE;
        }
        
        byte[] dataBytes = data.toByteArray();
        task.setData((ByteBuffer) ByteBuffer.allocate(requestTypeFieldBytes.length + dataBytes.length)
                .put(requestTypeFieldBytes).put(dataBytes).position(0));
        node.apply(task);
    }
    
    private void invokeToLeader(final String group, final Message request, final int timeoutMillis,
            FailoverClosure closure) {
        try {
            final Endpoint leaderIp = Optional.ofNullable(getLeader(group))
                    .orElseThrow(() -> new NoLeaderException(group)).getEndpoint();
            cliClientService.getRpcClient().invokeAsync(leaderIp, request, new InvokeCallback() {
                @Override
                public void complete(Object o, Throwable ex) {
                    if (Objects.nonNull(ex)) {
                        closure.setThrowable(ex);
                        closure.run(new Status(RaftError.UNKNOWN, ex.getMessage()));
                        return;
                    }
                    if (!((Response)o).getSuccess()) {
                        closure.setThrowable(new IllegalStateException(((Response) o).getErrMsg()));
                        closure.run(new Status(RaftError.UNKNOWN, ((Response) o).getErrMsg()));
                        return;
                    }
                    closure.setResponse((Response) o);
                    closure.run(Status.OK());
                }
                
                @Override
                public Executor executor() {
                    return RaftExecutor.getRaftCliServiceExecutor();
                }
            }, timeoutMillis);
        } catch (Exception e) {
            closure.setThrowable(e);
            closure.run(new Status(RaftError.UNKNOWN, e.toString()));
        }
    }
    
    boolean peerChange(JRaftMaintainService maintainService, Set<String> newPeers) {
        // This is only dealing with node deletion, the Raft protocol, where the node adds itself to the cluster when it starts up
        Set<String> oldPeers = new HashSet<>(this.raftConfig.getMembers());
        this.raftConfig.setMembers(localPeerId.toString(), newPeers);
        oldPeers.removeAll(newPeers);
        if (oldPeers.isEmpty()) {
            return true;
        }
        
        Set<String> waitRemove = oldPeers;
        AtomicInteger successCnt = new AtomicInteger(0);
        multiRaftGroup.forEach(new BiConsumer<String, RaftGroupTuple>() {
            @Override
            public void accept(String group, RaftGroupTuple tuple) {
                Map<String, String> params = new HashMap<>();
                params.put(JRaftConstants.GROUP_ID, group);
                params.put(JRaftConstants.COMMAND_NAME, JRaftConstants.REMOVE_PEERS);
                params.put(JRaftConstants.COMMAND_VALUE, StringUtils.join(waitRemove, StringUtils.COMMA));
                RestResult<String> result = maintainService.execute(params);
                if (result.ok()) {
                    successCnt.incrementAndGet();
                } else {
                    Loggers.RAFT.error("Node removal failed : {}", result);
                }
            }
        });
        return successCnt.get() == multiRaftGroup.size();
    }
    
    void refreshRouteTable(String group) {
        if (isShutdown) {
            return;
        }
        
        final String groupName = group;
        Status status = null;
        try {
            RouteTable instance = RouteTable.getInstance();
            Configuration oldConf = instance.getConfiguration(groupName);
            String oldLeader = Optional.ofNullable(instance.selectLeader(groupName)).orElse(PeerId.emptyPeer())
                    .getEndpoint().toString();
            // fix issue #3661  https://github.com/alibaba/nacos/issues/3661
            status = instance.refreshLeader(this.cliClientService, groupName, rpcRequestTimeoutMs);
            if (!status.isOk()) {
                Loggers.RAFT.error("Fail to refresh leader for group : {}, status is : {}", groupName, status);
            }
            status = instance.refreshConfiguration(this.cliClientService, groupName, rpcRequestTimeoutMs);
            if (!status.isOk()) {
                Loggers.RAFT
                        .error("Fail to refresh route configuration for group : {}, status is : {}", groupName, status);
            }
        } catch (Exception e) {
            Loggers.RAFT.error("Fail to refresh raft metadata info for group : {}, error is : {}", groupName, e);
        }
    }
    
    public RaftGroupTuple findTupleByGroup(final String group) {
        RaftGroupTuple tuple = multiRaftGroup.get(group);
        return tuple;
    }
    
    public Node findNodeByGroup(final String group) {
        final RaftGroupTuple tuple = multiRaftGroup.get(group);
        if (Objects.nonNull(tuple)) {
            return tuple.node;
        }
        return null;
    }
    
    public boolean isReady() {
        if (raftConfig.isStrictMode()) {
            for (RequestProcessor4CP each : processors) {
                if (null == getLeader(each.group())) {
                    return false;
                }
            }
        }
        return isStarted;
    }
    
    Map<String, RaftGroupTuple> getMultiRaftGroup() {
        return multiRaftGroup;
    }
    
    @JustForTest
    void mockMultiRaftGroup(Map<String, RaftGroupTuple> map) {
        this.multiRaftGroup = map;
    }
    
    CliService getCliService() {
        return cliService;
    }
    
    public static class RaftGroupTuple {
        
        private RequestProcessor processor;
        
        private Node node;
        
        private RaftGroupService raftGroupService;
        
        private NacosStateMachine machine;
        
        @JustForTest
        public RaftGroupTuple() {
        }
        
        public RaftGroupTuple(Node node, RequestProcessor processor, RaftGroupService raftGroupService,
                NacosStateMachine machine) {
            this.node = node;
            this.processor = processor;
            this.raftGroupService = raftGroupService;
            this.machine = machine;
        }
        
        public Node getNode() {
            return node;
        }
        
        public RequestProcessor getProcessor() {
            return processor;
        }
        
        public RaftGroupService getRaftGroupService() {
            return raftGroupService;
        }
    }
    
}
