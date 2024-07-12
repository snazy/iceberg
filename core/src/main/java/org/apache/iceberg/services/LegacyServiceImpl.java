/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.services;

import static java.lang.String.format;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.util.Map;

/**
 * {@link ServiceProvider} internal legacy service implementation, NOT a public class.
 *
 * <p>Used to construct Java proxies for the service interface for legacy services.
 */
final class LegacyServiceImpl {

  private LegacyServiceImpl() {}

  @SuppressWarnings("ProxyNonConstantType")
  static <I, S extends Service<I>> S build(
      String kebabName, Class<I> implementorType, Class<I> interfaceType, Class<S> serviceType) {
    Object p =
        Proxy.newProxyInstance(
            Thread.currentThread().getContextClassLoader(),
            new Class[] {serviceType},
            (proxy, method, args) -> {
              switch (method.getName()) {
                case "serviceName":
                  return kebabName;
                case "serviceType":
                  return implementorType;
                case "priority":
                  return 100;
                case "buildUninitialized":
                case "build":
                  I instance;
                  try {
                    Constructor<I> implementorCtor = implementorType.getDeclaredConstructor();
                    instance = implementorCtor.newInstance();
                  } catch (InstantiationException
                      | IllegalAccessException
                      | NoSuchMethodException
                      | InvocationTargetException e) {
                    throw new IllegalArgumentException(
                        format(
                            "Cannot initialize %s implementation %s: Cannot find constructor",
                            interfaceType.getSimpleName(), implementorType.getName()));
                  }
                  if ("build".equals(method.getName())) {
                    @SuppressWarnings("unchecked")
                    Map<String, String> properties = (Map<String, String>) args[0];
                    if (instance instanceof WithProperties) {
                      ((WithProperties) instance).initialize(properties);
                    } else {
                      throw new UnsupportedOperationException(
                          format(
                              "Service implementation %s does not implement WithProperties",
                              implementorType.getName()));
                    }
                  }
                  return instance;
                case "toString":
                  return format(
                      "Legacy service builder for '%s' (class '%s')",
                      kebabName, implementorType.getName());
                default:
                  for (Method m : serviceType.getDeclaredMethods()) {
                    if (Modifier.isStatic(m.getModifiers())
                        && m.getName().equals(method.getName())) {
                      return m.invoke(null, args);
                    }
                  }
                  throw new UnsupportedOperationException(
                      "Could not find static pendant of method \""
                          + method
                          + "\" on \""
                          + serviceType.getName()
                          + "\"");
              }
            });
    @SuppressWarnings({"UnnecessaryLocalVariable", "unchecked"})
    S casted = (S) p;
    return casted;
  }
}
