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

package com.alibaba.nacos.naming.remote.rpc.handler;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.remote.NamingRemoteConstants;
import com.alibaba.nacos.api.naming.remote.request.InstanceRequest;
import com.alibaba.nacos.api.naming.remote.response.InstanceResponse;
import com.alibaba.nacos.api.remote.request.RequestMeta;
import com.alibaba.nacos.auth.annotation.Secured;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.trace.DeregisterInstanceReason;
import com.alibaba.nacos.common.trace.event.naming.DeregisterInstanceTraceEvent;
import com.alibaba.nacos.common.trace.event.naming.RegisterInstanceTraceEvent;
import com.alibaba.nacos.core.control.TpsControl;
import com.alibaba.nacos.core.paramcheck.ExtractorManager;
import com.alibaba.nacos.core.paramcheck.impl.InstanceRequestParamExtractor;
import com.alibaba.nacos.core.remote.RequestHandler;
import com.alibaba.nacos.naming.core.v2.pojo.Service;
import com.alibaba.nacos.naming.core.v2.service.impl.EphemeralClientOperationServiceImpl;
import com.alibaba.nacos.naming.utils.InstanceUtil;
import com.alibaba.nacos.naming.utils.NamingRequestUtil;
import com.alibaba.nacos.plugin.auth.constant.ActionTypes;
import org.springframework.stereotype.Component;

/**
 * Instance request handler.
 * 实例请求处理器
 *
 * @author xiweng.yy
 */
@Component
public class InstanceRequestHandler extends RequestHandler<InstanceRequest, InstanceResponse> {

    // 临时客户端操作服务
    private final EphemeralClientOperationServiceImpl clientOperationService;
    
    public InstanceRequestHandler(EphemeralClientOperationServiceImpl clientOperationService) {
        this.clientOperationService = clientOperationService;
    }
    
    @Override
    @TpsControl(pointName = "RemoteNamingInstanceRegisterDeregister", name = "RemoteNamingInstanceRegisterDeregister")
    @Secured(action = ActionTypes.WRITE)
    @ExtractorManager.Extractor(rpcExtractor = InstanceRequestParamExtractor.class)
    public InstanceResponse handle(InstanceRequest request, RequestMeta meta) throws NacosException {
        // request 就是客户端封装的实例信息，包括服务名、端口等
        // 转换为 Service 对象
        Service service = Service.newService(request.getNamespace(), request.getGroupName(), request.getServiceName(),
                true);
        // 如果实例id和服务名称为空，重新赋值
        InstanceUtil.setInstanceIdIfEmpty(request.getInstance(), service.getGroupedServiceName());
        switch (request.getType()) {
            // 注册实例
            case NamingRemoteConstants.REGISTER_INSTANCE:
                return registerInstance(service, request, meta);
                // 注销实例
            case NamingRemoteConstants.DE_REGISTER_INSTANCE:
                return deregisterInstance(service, request, meta);
            default:
                throw new NacosException(NacosException.INVALID_PARAM,
                        String.format("Unsupported request type %s", request.getType()));
        }
    }
    
    private InstanceResponse registerInstance(Service service, InstanceRequest request, RequestMeta meta)
            throws NacosException {
        // 注册临时实例
        clientOperationService.registerInstance(service, request.getInstance(), meta.getConnectionId());
        // 发布注册实例追踪事件
        NotifyCenter.publishEvent(new RegisterInstanceTraceEvent(System.currentTimeMillis(),
                NamingRequestUtil.getSourceIpForGrpcRequest(meta), true, service.getNamespace(), service.getGroup(),
                service.getName(), request.getInstance().getIp(), request.getInstance().getPort()));
        return new InstanceResponse(NamingRemoteConstants.REGISTER_INSTANCE);
    }
    
    private InstanceResponse deregisterInstance(Service service, InstanceRequest request, RequestMeta meta) {
        clientOperationService.deregisterInstance(service, request.getInstance(), meta.getConnectionId());
        NotifyCenter.publishEvent(new DeregisterInstanceTraceEvent(System.currentTimeMillis(),
                NamingRequestUtil.getSourceIpForGrpcRequest(meta), true, DeregisterInstanceReason.REQUEST,
                service.getNamespace(), service.getGroup(), service.getName(), request.getInstance().getIp(),
                request.getInstance().getPort()));
        return new InstanceResponse(NamingRemoteConstants.DE_REGISTER_INSTANCE);
    }
    
}
