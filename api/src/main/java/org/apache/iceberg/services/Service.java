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

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.iceberg.io.FileIO;

/**
 * Base interface for services, which can be loaded via Java's {@link ServiceLoader} mechanism.
 *
 * @param <I> the Iceberg interface provided by the implementation, for example {@link FileIO}.
 */
public interface Service<I> {

  /** Name of the service. */
  String serviceName();

  /**
   * Type of the <em>implementation</em>, for example {@code org.apache.iceberg.aws.s3.S3FileIO}.
   */
  Class<? extends I> serviceType();

  default int priority() {
    return 100;
  }

  default I build(Map<String, String> properties) {
    throw new UnsupportedOperationException(
        format(
            "Service implementation %s does not support direct initialization",
            serviceType().getSimpleName()));
  }

  default I buildUninitialized() {
    try {
      return serviceType().getDeclaredConstructor().newInstance();
    } catch (InstantiationException
        | IllegalAccessException
        | InvocationTargetException
        | NoSuchMethodException e) {
      throw new IllegalArgumentException(
          "Cannot initialize FileIO implementation "
              + serviceType().getName()
              + ": Cannot find constructor",
          e);
    }
  }
}
