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

import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import org.apache.iceberg.io.DelegateFileIO;
import org.apache.iceberg.io.FileIO;

/**
 * Provides services based on certain criteria like the service type, for example {@link FileIO} and
 * name.
 *
 * <p>The implementation targets services to eventually only use Java's {@link ServiceLoader},
 * providing a {@link Service Service<I>}.
 *
 * <p>This implementation currently also supports implementations that do not yet provide a {@link
 * Service<I>} for Java's {@link ServiceLoader} by leveraging <a
 * href="https://smallrye.io/jandex/">Jandex indexes</a>. The names of the services are derived from
 * the implementations simple class names minus the service interface name, and converting it to
 * kebab-case. For example, the simple class name of {@code org.apache.iceberg.aws.s3.S3FileIO}
 * ({@code S3FileIO}) becomes {@code s3} and a {@code internal.mystuff.MyCustomFileIO} would be
 * addressable using the name {@code my-custom}.
 *
 * @param <I> base interface of the service, for example {@link FileIO}
 */
public interface ServiceProvider<I, S extends Service<I>> extends Serializable {

  static <I, S extends Service<I>> ServiceProvider<I, S> newServiceProvider(
      Class<I> interfaceType, Class<S> serviceType) {
    return ServiceProviderImpl.newServiceProvider(interfaceType, serviceType);
  }

  /**
   * If multiple services are expected to match the provided criteria, using {@code anyMatch ==
   * true} instructs {@link #load()} and {@link #loadOptional()} to return the first matching
   * service.
   */
  ServiceProvider<I, S> withAnyMatch(boolean anyMatch);

  /** Filter criteria on {@link Service#serviceName()}. */
  ServiceProvider<I, S> withName(String name);

  /** Filter criteria on {@link Service#serviceName()} or {@link Service#serviceType()}. */
  ServiceProvider<I, S> withType(Class<? extends I> type, String name);

  /**
   * Filter criteria matching {@code impl} as a fully qualified class name against {@link
   * Service#serviceType()} or as the {@link Service#serviceName() service name}.
   *
   * <p>This function is used to maintain behavioral compatibility and subject to removal.
   */
  ServiceProvider<I, S> withLegacyImpl(String impl);

  /**
   * Filter criteria to return only services that have implementations that implement the given
   * types. Can be for example used to return only {@link DelegateFileIO} implementations using
   * {@code newServiceProvider(FileIO.class,
   * FileIOService.class}.withRequiredImplements(DelegateFileIO.class).loadAll()}.
   */
  ServiceProvider<I, S> withRequiredImplements(Class<?>... requiredImplements);

  S load();

  Optional<S> loadOptional();

  List<S> loadAll();
}
