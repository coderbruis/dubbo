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

package org.apache.dubbo.registry.retry;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.timer.Timeout;
import org.apache.dubbo.registry.support.FailbackRegistry;

/**
 * FailedRegisteredTask
 */
public final class FailedRegisteredTask extends AbstractRetryTask {

    private static final String NAME = "retry register";

    public FailedRegisteredTask(URL url, FailbackRegistry registry) {
        super(url, registry, NAME);
    }

    /**
     * 重试核心逻辑，重新去注册中心注册。
     *
     * 实现了了AbstractRetryTask的doRetry抽象方法
     *
     * 设计模式：模板模式
     *
     * @param url
     * @param registry
     * @param timeout
     */
    @Override
    protected void doRetry(URL url, FailbackRegistry registry, Timeout timeout) {
        // 模板方法由子类提供实现, 即重试核心逻辑由子类提供
        registry.doRegister(url);
        // 从failedRegisteredTask的map中移除该URL，表示已经重试了
        registry.removeFailedRegisteredTask(url);
    }
}
