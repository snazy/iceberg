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
import static java.util.Comparator.comparingInt;
import static org.apache.iceberg.relocated.com.google.common.base.Preconditions.checkArgument;
import static org.apache.iceberg.relocated.com.google.common.base.Preconditions.checkState;

import com.fasterxml.jackson.databind.PropertyNamingStrategies.KebabCaseStrategy;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.iceberg.io.DelegateFileIO;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.CompositeIndex;
import org.jboss.jandex.DotName;
import org.jboss.jandex.Index;
import org.jboss.jandex.IndexReader;
import org.jboss.jandex.IndexView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ServiceProviderImpl<I, S extends Service<I>> implements ServiceProvider<I, S> {

  private static final Logger LOG = LoggerFactory.getLogger(ServiceProviderImpl.class);
  private static final KebabCaseStrategy KEBAB_CASE_STRATEGY = new KebabCaseStrategy();

  /** Provides a cache of service instances by service filter-spec. */
  private static final ConcurrentHashMap<ServiceFilterSpec<?, ?>, List<Service<?>>> SERVICES_CACHE =
      new ConcurrentHashMap<>();

  /**
   * Provides a cache of service instances by service-type. This one can go away when support for
   * legacy services is no longer needed.
   */
  @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
  private static final Map<Class<?>, List<Service<?>>> SERVICES_BY_TYPE = new ConcurrentHashMap<>();

  /** Provides a set of legacy service implementations that were already logged as a warning. */
  private static final Set<Class<?>> LEGACY_WARNED = ConcurrentHashMap.newKeySet();

  // *sigh* javac doesn't like those to be collapsed into a single assignment
  private static final Comparator<Service<?>> SERVICE_COMPARATOR_PRIORITY =
      comparingInt(Service::priority);
  private static final Comparator<Service<?>> SERVICE_COMPARATOR =
      SERVICE_COMPARATOR_PRIORITY.thenComparing(Service::serviceName);

  private final ServiceFilterSpec<I, S> filterSpec;

  private ServiceProviderImpl(ServiceFilterSpec<I, S> filterSpec) {
    this.filterSpec = filterSpec;
  }

  static <I, S extends Service<I>> ServiceProvider<I, S> newServiceProvider(
      Class<I> interfaceType, Class<S> serviceType) {
    checkArgument(interfaceType != null, "interfaceType must not be null");
    checkArgument(serviceType != null, "serviceType must not be null");

    return new ServiceProviderImpl<>(ServiceFilterSpec.initial(interfaceType, serviceType));
  }

  /**
   * If multiple services are expected to match the provided criteria, using {@code anyMatch ==
   * true} instructs {@link #load()} and {@link #loadOptional()} to return the first matching
   * service.
   */
  @Override
  public ServiceProvider<I, S> withAnyMatch(boolean anyMatch) {
    return new ServiceProviderImpl<>(filterSpec.withAnyMatch(anyMatch));
  }

  /** Filter criteria on {@link Service#serviceName()}. */
  @Override
  public ServiceProvider<I, S> withName(String name) {
    checkArgument(name != null, "service name must not be null");
    checkState(
        filterSpec.selectorType() == null && filterSpec.selectorName() == null,
        "name/type selector already specified");

    return new ServiceProviderImpl<>(filterSpec.withSelectorName(name));
  }

  /** Filter criteria on {@link Service#serviceName()} or {@link Service#serviceType()}. */
  @Override
  public ServiceProvider<I, S> withType(Class<? extends I> type, String name) {
    checkArgument(
        name != null || type != null, "at least one of service name or type must not be null");
    checkState(
        filterSpec.selectorType() == null && filterSpec.selectorName() == null,
        "name/type selector already specified");

    return new ServiceProviderImpl<>(filterSpec.withSelectorType(type).withSelectorName(name));
  }

  /**
   * Filter criteria matching {@code impl} as a fully qualified class name against {@link
   * Service#serviceType()} or as the {@link Service#serviceName() service name}.
   */
  @Override
  public ServiceProvider<I, S> withLegacyImpl(String impl) {
    checkArgument(impl != null, "at least one of service name or type name must not be null");
    checkState(
        filterSpec.selectorType() == null && filterSpec.selectorName() == null,
        "name/type selector already specified");

    return new ServiceProviderImpl<>(filterSpec.withSelectorName(impl).withSelectorTypeName(impl));
  }

  /**
   * Filter criteria to return only services that have implementations that implement the given
   * types. Can be for example used to return only {@link DelegateFileIO} implementations using
   * {@code newServiceProvider(FileIO.class,
   * FileIOService.class}.withRequiredImplements(DelegateFileIO.class).loadAll()}.
   */
  @Override
  public ServiceProvider<I, S> withRequiredImplements(Class<?>... requiredImplements) {
    return new ServiceProviderImpl<>(filterSpec.withRequiredImplements(requiredImplements));
  }

  @Override
  public S load() {
    List<S> candidates = loadAll();

    Iterator<S> iter = candidates.iterator();
    if (!iter.hasNext()) {
      throw new IllegalArgumentException(
          format(
              "No %s service matching the given criteria: type-name=%s, type=%s, name=%s, any-match=%s",
              filterSpec.interfaceType().getSimpleName(),
              filterSpec.selectorTypeName(),
              filterSpec.selectorType(),
              filterSpec.selectorName(),
              filterSpec.anyMatch()));
    }

    S service = iter.next();
    if (!filterSpec.anyMatch() && iter.hasNext()) {
      throw new IllegalArgumentException(
          format(
              "More than one %s service (%s and %s) matches the given criteria: type-name=%s, type=%s, name=%s, any-match=%s",
              filterSpec.interfaceType().getSimpleName(),
              service.serviceType().getName(),
              iter.next().serviceType().getName(),
              filterSpec.selectorTypeName(),
              filterSpec.selectorType(),
              filterSpec.selectorName(),
              filterSpec.anyMatch()));
    }

    return service;
  }

  @Override
  public Optional<S> loadOptional() {
    List<S> candidates = loadAll();

    Iterator<S> iter = candidates.iterator();
    if (!iter.hasNext()) {
      return Optional.empty();
    }

    S service = iter.next();
    if (!filterSpec.anyMatch() && iter.hasNext()) {
      return Optional.empty();
    }

    return Optional.of(service);
  }

  @Override
  public List<S> loadAll() {
    List<Service<?>> fromCache =
        SERVICES_CACHE.computeIfAbsent(filterSpec, f -> unsafeCast(internalLoadAll(unsafeCast(f))));
    return unsafeCast(fromCache);
  }

  private static <I, S extends Service<I>> List<S> internalLoadAll(
      ServiceFilterSpec<I, S> filterSpec) {
    BiPredicate<String, Class<?>> predicate;

    String selectorTypeName = filterSpec.selectorTypeName();
    String selectorName = filterSpec.selectorName();
    boolean isLegacy = selectorTypeName != null && selectorTypeName.equals(selectorName);
    if (isLegacy) {
      // via `withLegacyImpl()`
      predicate =
          (name, type) ->
              type.getName().equals(selectorTypeName) || selectorName.equalsIgnoreCase(name);
    } else {
      Class<? extends I> selectorType = selectorType(filterSpec);

      if (selectorType != null && selectorName != null) {
        predicate =
            (name, type) ->
                selectorType.isAssignableFrom(type) && selectorName.equalsIgnoreCase(name);
      } else if (selectorType != null) {
        predicate = (name, type) -> selectorType.isAssignableFrom(type);
      } else if (selectorName != null) {
        predicate = (name, type) -> selectorName.equalsIgnoreCase(name);
      } else {
        predicate = (name, type) -> true;
      }
    }

    List<Class<?>> requiredImplements = filterSpec.requiredImplements();
    if (requiredImplements != null) {
      predicate =
          predicate.and(
              (name, type) -> {
                for (Class<?> requiredImplement : requiredImplements) {
                  if (!requiredImplement.isAssignableFrom(type)) {
                    return false;
                  }
                }
                return true;
              });
    }

    BiPredicate<String, Class<?>> finalPredicate = predicate;

    List<S> services =
        getServices(filterSpec.interfaceType(), filterSpec.serviceType()).stream()
            .filter(s -> finalPredicate.test(s.serviceName(), s.serviceType()))
            .collect(Collectors.toList());

    if (isLegacy) {
      try {
        Class<I> impl = unsafeCast(Class.forName(selectorTypeName));
        if (services.stream().noneMatch(s -> s.serviceType().isAssignableFrom(impl))) {
          String simpleName = filterSpec.interfaceType().getSimpleName();
          String implementorSimpleName = impl.getSimpleName();
          String name =
              implementorSimpleName.endsWith(simpleName)
                  ? implementorSimpleName.substring(
                      0, implementorSimpleName.length() - simpleName.length())
                  : implementorSimpleName;
          String kebabName = KEBAB_CASE_STRATEGY.translate(name);

          maybeWarnLegacy(filterSpec.serviceType(), impl);
          services.add(
              LegacyServiceImpl.build(
                  kebabName, impl, filterSpec.interfaceType(), filterSpec.serviceType()));
        }
      } catch (ClassNotFoundException ignore) {
        // ignore
      } catch (LinkageError e) {
        LOG.warn("Failed to load legacy service {}", selectorTypeName, e);
      }
    }

    return services;
  }

  private static <I, S extends Service<I>> Class<? extends I> selectorType(
      ServiceFilterSpec<I, S> filterSpec) {
    Class<? extends I> selectorType;
    String selectorTypeName = filterSpec.selectorTypeName();
    if (selectorTypeName != null) {
      try {
        selectorType = unsafeCast(Class.forName(selectorTypeName));
      } catch (ClassNotFoundException e) {
        selectorType = null;
      }
    } else {
      selectorType = filterSpec.selectorType();
    }
    return selectorType;
  }

  @VisibleForTesting
  static <I, S extends Service<I>> List<S> getServices(
      Class<I> interfaceType, Class<S> serviceType) {
    Map<Class<I>, List<S>> servicesMap = unsafeCast(SERVICES_BY_TYPE);
    return servicesMap.computeIfAbsent(
        interfaceType, it -> loadServices(interfaceType, serviceType));
  }

  @VisibleForTesting
  static <I, S extends Service<I>> List<S> loadServices(
      Class<I> interfaceType, Class<S> serviceType) {

    List<S> services = new ArrayList<>();
    Set<Class<? extends I>> javaServiceImpls = new HashSet<>();

    ServiceLoader<S> loader = ServiceLoader.load(serviceType);
    for (S svc : loader) {
      javaServiceImpls.add(svc.serviceType());
      services.add(svc);
    }

    loadLegacyServices(interfaceType, serviceType, javaServiceImpls, services::add);

    services.sort(SERVICE_COMPARATOR);

    return services;
  }

  @VisibleForTesting
  static <I, S extends Service<I>> void loadLegacyServices(
      Class<I> interfaceType,
      Class<S> serviceType,
      Set<Class<? extends I>> javaServiceImpls,
      Consumer<S> candidateCollector) {

    DotName typeDotName = DotName.createSimple(interfaceType);
    String simpleName = interfaceType.getSimpleName();

    IndexView index = JandexIndexes.indexes();
    Collection<ClassInfo> implementors = index.getAllKnownImplementors(typeDotName);

    for (ClassInfo implementor : implementors) {

      String implementorClassName = implementor.name().toString();

      if (implementor.isAbstract()
          || implementor.isSynthetic()
          || implementor.isInterface()
          || implementor.enclosingClass() != null
          || !Modifier.isPublic(implementor.flags())
          || !implementor.hasNoArgsConstructor()) {
        LOG.debug(
            "Implementation {} of {} does not satisfy the requirements of a legacy service implementation (public, non-abstract, non-inner class w/ no-arg constructor)",
            implementorClassName,
            interfaceType.getName());
        continue;
      }

      String implementorSimpleName = implementor.simpleName();
      String name =
          implementorSimpleName.endsWith(simpleName)
              ? implementorSimpleName.substring(
                  0, implementorSimpleName.length() - simpleName.length())
              : implementorSimpleName;
      String kebabName = KEBAB_CASE_STRATEGY.translate(name);

      LOG.debug(
          "Found service {} implementation {}, using name {}",
          interfaceType.getName(),
          implementorClassName,
          kebabName);

      Class<I> implementorType;
      try {
        implementorType =
            unsafeCast(
                Class.forName(
                    implementorClassName, false, Thread.currentThread().getContextClassLoader()));
      } catch (ClassNotFoundException e) {
        if (LOG.isDebugEnabled()) {
          // work around for '[CatchBlockLogException] Catch block contains log statements but
          // thrown exception is never logged'
          LOG.warn(
              "Ignoring implementation {} of {}", implementorClassName, interfaceType.getName(), e);
        } else {
          LOG.warn(
              "Ignoring implementation {} of {} due to {}",
              implementorClassName,
              interfaceType.getName(),
              e.toString());
        }
        continue;
      }

      if (javaServiceImpls.contains(implementorType)) {
        // Exclude legacy implementations that have a corresponding Java service
        continue;
      }

      maybeWarnLegacy(serviceType, implementorType);
      candidateCollector.accept(
          LegacyServiceImpl.build(kebabName, implementorType, interfaceType, serviceType));
    }
  }

  private static <I, S extends Service<I>> void maybeWarnLegacy(
      Class<S> serviceType, Class<I> implementorType) {
    if (LEGACY_WARNED.add(implementorType)) {
      LOG.warn(
          "Found legacy {} service, which should to be migrated to the Java service based {}",
          implementorType.getName(),
          serviceType.getName());
    }
  }

  private static final class JandexIndexes {

    private static final IndexView INDEX = loadIndexes();

    static IndexView indexes() {
      return INDEX;
    }

    private static IndexView loadIndexes() {
      List<IndexView> indexes = new ArrayList<>();

      LOG.debug("Loading jandex indexes...");

      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      try {
        for (Enumeration<URL> indexResources = cl.getResources("META-INF/jandex.idx");
            indexResources.hasMoreElements(); ) {
          URL indexUrl = indexResources.nextElement();
          LOG.debug("Loading jandex index {} ...", indexUrl);
          try (InputStream in = indexUrl.openStream()) {
            IndexReader reader = new IndexReader(in);
            Index index = reader.read();
            indexes.add(index);
          } catch (IOException e) {
            LOG.warn("Failed to load Jandex index {}", indexUrl, e);
          }
        }
      } catch (IOException e) {
        throw new RuntimeException("Failed to locate Jandex indexes", e);
      }

      LOG.debug("Loaded {} jandex indexes", indexes.size());

      return CompositeIndex.create(indexes);
    }
  }

  @SuppressWarnings("unchecked")
  private static <R> R unsafeCast(Object o) {
    return (R) o;
  }
}
