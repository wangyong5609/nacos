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

package com.alibaba.nacos.naming.push.v2;

import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.alibaba.nacos.common.notify.Event;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.notify.listener.SmartSubscriber;
import com.alibaba.nacos.naming.core.v2.client.manager.ClientManager;
import com.alibaba.nacos.naming.core.v2.client.manager.ClientManagerDelegate;
import com.alibaba.nacos.naming.core.v2.event.publisher.NamingEventPublisherFactory;
import com.alibaba.nacos.naming.core.v2.event.service.ServiceEvent;
import com.alibaba.nacos.naming.core.v2.index.ClientServiceIndexesManager;
import com.alibaba.nacos.naming.core.v2.index.ServiceStorage;
import com.alibaba.nacos.naming.core.v2.metadata.NamingMetadataManager;
import com.alibaba.nacos.naming.core.v2.pojo.Service;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import com.alibaba.nacos.naming.monitor.MetricsMonitor;
import com.alibaba.nacos.naming.pojo.Subscriber;
import com.alibaba.nacos.naming.push.NamingSubscriberService;
import com.alibaba.nacos.naming.push.v2.executor.PushExecutorDelegate;
import com.alibaba.nacos.naming.push.v2.task.PushDelayTask;
import com.alibaba.nacos.naming.push.v2.task.PushDelayTaskExecuteEngine;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Naming subscriber service for v2.x.
 *
 * @author xiweng.yy
 */
@org.springframework.stereotype.Service
public class NamingSubscriberServiceV2Impl extends SmartSubscriber implements NamingSubscriberService {
    
    private static final int PARALLEL_SIZE = 100;
    
    private final ClientManager clientManager;
    
    private final ClientServiceIndexesManager indexesManager;
    
    private final PushDelayTaskExecuteEngine delayTaskEngine;
    
    public NamingSubscriberServiceV2Impl(ClientManagerDelegate clientManager,
            ClientServiceIndexesManager indexesManager, ServiceStorage serviceStorage,
            NamingMetadataManager metadataManager, PushExecutorDelegate pushExecutor, SwitchDomain switchDomain) {
        this.clientManager = clientManager;
        this.indexesManager = indexesManager;
        this.delayTaskEngine = new PushDelayTaskExecuteEngine(clientManager, indexesManager, serviceStorage,
                metadataManager, pushExecutor, switchDomain);
        NotifyCenter.registerSubscriber(this, NamingEventPublisherFactory.getInstance());
        
    }
    
    @Override
    public Collection<Subscriber> getSubscribers(String namespaceId, String serviceName) {
        String serviceNameWithoutGroup = NamingUtils.getServiceName(serviceName);
        String groupName = NamingUtils.getGroupName(serviceName);
        Service service = Service.newService(namespaceId, groupName, serviceNameWithoutGroup);
        return getSubscribers(service);
    }
    
    @Override
    public Collection<Subscriber> getSubscribers(Service service) {
        Collection<Subscriber> result = new HashSet<>();
        for (String each : indexesManager.getAllClientsSubscribeService(service)) {
            result.add(clientManager.getClient(each).getSubscriber(service));
        }
        return result;
    }
    
    @Override
    public Collection<Subscriber> getFuzzySubscribers(String namespaceId, String serviceName) {
        Collection<Subscriber> result = new HashSet<>();
        Stream<Service> serviceStream = getServiceStream();
        String serviceNamePattern = NamingUtils.getServiceName(serviceName);
        String groupNamePattern = NamingUtils.getGroupName(serviceName);
        serviceStream.filter(service -> service.getNamespace().equals(namespaceId) && service.getName()
                .contains(serviceNamePattern) && service.getGroup().contains(groupNamePattern))
                .forEach(service -> result.addAll(getSubscribers(service)));
        return result;
    }
    
    @Override
    public Collection<Subscriber> getFuzzySubscribers(Service service) {
        return getFuzzySubscribers(service.getNamespace(), service.getGroupedServiceName());
    }
    
    @Override
    public List<Class<? extends Event>> subscribeTypes() {
        List<Class<? extends Event>> result = new LinkedList<>();
        // 订阅服务变更事件
        result.add(ServiceEvent.ServiceChangedEvent.class);
        result.add(ServiceEvent.ServiceSubscribedEvent.class);
        return result;
    }
    
    @Override
    public void onEvent(Event event) {
        if (event instanceof ServiceEvent.ServiceChangedEvent) {
            // If service changed, push to all subscribers.
            ServiceEvent.ServiceChangedEvent serviceChangedEvent = (ServiceEvent.ServiceChangedEvent) event;
            Service service = serviceChangedEvent.getService();
            // 添加服务推送延迟任务，将最新的服务实例列表推送给所有订阅了该服务的客户端
            delayTaskEngine.addTask(service, new PushDelayTask(service, PushConfig.getInstance().getPushTaskDelay()));
            // 增加服务变更计数
            MetricsMonitor.incrementServiceChangeCount(service);
        } else if (event instanceof ServiceEvent.ServiceSubscribedEvent) {
            // If service is subscribed by one client, only push this client.
            // 当 ServiceSubscribedEvent 事件发生时，意味着有新的客户端订阅了某个服务，
            // NamingSubscriberServiceV2Impl 同样会创建一个 PushDelayTask，但这次会指定推送的目标客户端ID，确保只有订阅该服务的客户端会收到推送
            ServiceEvent.ServiceSubscribedEvent subscribedEvent = (ServiceEvent.ServiceSubscribedEvent) event;
            Service service = subscribedEvent.getService();
            delayTaskEngine.addTask(service, new PushDelayTask(service, PushConfig.getInstance().getPushTaskDelay(),
                    subscribedEvent.getClientId()));
        }
    }
    
    private Stream<Service> getServiceStream() {
        Collection<Service> services = indexesManager.getSubscribedService();
        return services.size() > PARALLEL_SIZE ? services.parallelStream() : services.stream();
    }
    
    public int getPushPendingTaskCount() {
        return delayTaskEngine.size();
    }
}
