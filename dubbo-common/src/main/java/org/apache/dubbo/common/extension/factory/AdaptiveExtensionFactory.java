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
package org.apache.dubbo.common.extension.factory;

import org.apache.dubbo.common.extension.Adaptive;
import org.apache.dubbo.common.extension.ExtensionFactory;
import org.apache.dubbo.common.extension.ExtensionLoader;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 *
 * AdaptiveExtensionFactorey不作任何功能，而是用来适配SpiExtensionFactory和SpringExtensionFactory这两种实现。
 * 即AdaptiveExtensionFactory会根据运行时的一些状态来选择具体使用哪个ExtensionFactory实现类。
 *
 * AdaptiveExtensionFactory
 */
@Adaptive
public class AdaptiveExtensionFactory implements ExtensionFactory {

    private final List<ExtensionFactory> factories;

    public AdaptiveExtensionFactory() {
        // 获取ExtensionFactory这个扩展点的扩展加载器
        ExtensionLoader<ExtensionFactory> loader = ExtensionLoader.getExtensionLoader(ExtensionFactory.class);
        List<ExtensionFactory> list = new ArrayList<ExtensionFactory>();
        // 因为loader是ExtensionFactory类型的，所以loader.getSupportedExtension()的值为SPI
        for (String name : loader.getSupportedExtensions()) {
            // 去获取ExtensionFactory的SPI扩展点实现类, 所以这里一般都是获取的是SpiExtensionFactory
            list.add(loader.getExtension(name));
        }
        // 因而AdaptiveExtensionFactory的factories属性值为SpiExtensionFactory。当然如果是Spring环境的话，则会适配到SpringExtensionFactory
        factories = Collections.unmodifiableList(list);
        System.err.println("AdaptiveExtensionFactory....");
    }

    @Override
    public <T> T getExtension(Class<T> type, String name) {
        for (ExtensionFactory factory : factories) {
            T extension = factory.getExtension(type, name);
            if (extension != null) {
                return extension;
            }
        }
        return null;
    }

}
