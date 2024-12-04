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

package com.alibaba.nacos.common.notify;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 事件的抽象类。
 *
 * @author <a href="mailto:liaochuntao@live.com">liaochuntao</a>
 * @author zongtanghu
 */
@SuppressWarnings({"PMD.AbstractClassShouldStartWithAbstractNamingRule"})
public abstract class Event implements Serializable {
    
    private static final long serialVersionUID = -3731383194964997493L;
    
    private static final AtomicLong SEQUENCE = new AtomicLong(0);
    
    private final long sequence = SEQUENCE.getAndIncrement();
    
    /**
     * 事件序列号，可用于处理事件的顺序。
     *
     * @return 序列号，最好确保它是单调的。
     */
    public long sequence() {
        return sequence;
    }
    
    /**
     * 活动范围。
     *
     * @return 事件范围，如果适用于所有范围则返回 null
     */
    public String scope() {
        return null;
    }
    
    /**
     * 是否是插件事件。如果是这样，当没有发布和订阅时，事件可以被删除，没有任何提示。默认
     * false
     *
     * @return {@code true} 如果是插件事件，否则 {@code false}
     */
    public boolean isPluginEvent() {
        return false;
    }
}

