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

import java.util.List;
import javax.annotation.Nullable;
import org.immutables.value.Value;

/** {@link ServiceProvider} internal filter specification, NOT a public class. */
@Value.Immutable(lazyhash = true)
abstract class ServiceFilterSpec<I, S extends Service<I>> {

  static <I, S extends Service<I>> ServiceFilterSpec<I, S> initial(
      Class<I> interfaceType, Class<S> serviceType) {
    return ImmutableServiceFilterSpec.of(interfaceType, serviceType);
  }

  @Value.Parameter(order = 1)
  abstract Class<I> interfaceType();

  @Value.Parameter(order = 2)
  abstract Class<S> serviceType();

  @Nullable
  abstract String selectorTypeName();

  abstract ServiceFilterSpec<I, S> withSelectorTypeName(String value);

  @Nullable
  abstract Class<? extends I> selectorType();

  abstract ServiceFilterSpec<I, S> withSelectorType(Class<? extends I> value);

  @Nullable
  abstract String selectorName();

  abstract ServiceFilterSpec<I, S> withSelectorName(String value);

  @Nullable
  abstract List<Class<?>> requiredImplements();

  abstract ServiceFilterSpec<I, S> withRequiredImplements(Class<?>... elements);

  @Value.Default
  boolean anyMatch() {
    return false;
  }

  abstract ServiceFilterSpec<I, S> withAnyMatch(boolean value);
}
