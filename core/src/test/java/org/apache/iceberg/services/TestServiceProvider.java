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

import static java.util.Collections.emptySet;
import static org.apache.iceberg.services.ServiceProvider.newServiceProvider;
import static org.assertj.core.groups.Tuple.tuple;

import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.CatalogService;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.DelegateFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileIOService;
import org.apache.iceberg.services.fileios.DelegatingFileIO;
import org.apache.iceberg.services.fileios.JavaServiceFileIO;
import org.apache.iceberg.services.fileios.MyOwnFileIO;
import org.apache.iceberg.services.fileios.TestingFileIO;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
public class TestServiceProvider {

  @InjectSoftAssertions private SoftAssertions soft;

  @Test
  public void load() {
    soft.assertThat(newServiceProvider(FileIO.class, FileIOService.class).loadOptional()).isEmpty();
    soft.assertThat(
            newServiceProvider(FileIO.class, FileIOService.class)
                .withName("not-there")
                .loadOptional())
        .isEmpty();
    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                newServiceProvider(FileIO.class, FileIOService.class).withName("not-there").load())
        .withMessage(
            "No FileIO service matching the given criteria: type-name=null, type=null, name=not-there, any-match=false");

    soft.assertThat(
            newServiceProvider(FileIO.class, FileIOService.class).withAnyMatch(true).loadOptional())
        .isNotEmpty();

    soft.assertThat(
            newServiceProvider(FileIO.class, FileIOService.class)
                .withType(MyOwnFileIO.class, "my-own")
                .loadOptional())
        .isNotEmpty();

    soft.assertThat(
            newServiceProvider(FileIO.class, FileIOService.class)
                .withType(MyOwnFileIO.class, "not-my-own")
                .loadOptional())
        .isEmpty();
    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                newServiceProvider(FileIO.class, FileIOService.class)
                    .withType(MyOwnFileIO.class, "not-my-own")
                    .load())
        .withMessage(
            "No FileIO service matching the given criteria: type-name=null, type=class org.apache.iceberg.services.fileios.MyOwnFileIO, name=not-my-own, any-match=false");

    soft.assertThat(newServiceProvider(FileIO.class, FileIOService.class).withName("my-own").load())
        .extracting(Service::serviceName, Service::serviceType)
        .containsExactly("my-own", MyOwnFileIO.class);
    soft.assertThat(
            newServiceProvider(FileIO.class, FileIOService.class).withName("in-memory").load())
        .extracting(Service::serviceName, Service::serviceType)
        .containsExactly("in-memory", InMemoryFileIO.class);

    soft.assertThat(
            newServiceProvider(FileIO.class, FileIOService.class).withName("delegating").load())
        .extracting(Service::serviceName, Service::serviceType)
        .containsExactly("delegating", DelegatingFileIO.class);
    soft.assertThat(
            newServiceProvider(FileIO.class, FileIOService.class)
                .withRequiredImplements(DelegatingFileIO.class)
                .load())
        .extracting(Service::serviceName, Service::serviceType)
        .containsExactly("delegating", DelegatingFileIO.class);
    soft.assertThat(
            newServiceProvider(FileIO.class, FileIOService.class)
                .withName("delegating")
                .withRequiredImplements(DelegateFileIO.class)
                .load())
        .extracting(Service::serviceName, Service::serviceType)
        .containsExactly("delegating", DelegatingFileIO.class);
    soft.assertThat(
            newServiceProvider(FileIO.class, FileIOService.class)
                .withLegacyImpl("delegating")
                .withRequiredImplements(DelegateFileIO.class)
                .load())
        .extracting(Service::serviceName, Service::serviceType)
        .containsExactly("delegating", DelegatingFileIO.class);
    soft.assertThat(
            newServiceProvider(FileIO.class, FileIOService.class)
                .withLegacyImpl("org.apache.iceberg.services.fileios.DelegatingFileIO")
                .withRequiredImplements(DelegateFileIO.class)
                .load())
        .extracting(Service::serviceName, Service::serviceType)
        .containsExactly("delegating", DelegatingFileIO.class);

    for (CatalogService s : newServiceProvider(Catalog.class, CatalogService.class).loadAll()) {
      System.err.println(s.serviceType() + " " + s.serviceName());
    }
  }

  @Test
  public void loadAll() {
    soft.assertThat(newServiceProvider(FileIO.class, FileIOService.class).loadAll())
        .extracting(Service::serviceName, Service::serviceType)
        .contains(
            tuple("my-own", MyOwnFileIO.class),
            tuple("delegating", DelegatingFileIO.class),
            tuple("in-memory", InMemoryFileIO.class),
            tuple("hadoop", HadoopFileIO.class),
            tuple("java-svc", JavaServiceFileIO.class));

    soft.assertThat(
            newServiceProvider(FileIO.class, FileIOService.class)
                .withRequiredImplements(TestingFileIO.class)
                .loadAll())
        .extracting(Service::serviceName, Service::serviceType)
        .containsExactly(
            tuple("java-svc", JavaServiceFileIO.class),
            tuple("delegating", DelegatingFileIO.class),
            tuple("my-own", MyOwnFileIO.class));

    soft.assertThat(
            newServiceProvider(FileIO.class, FileIOService.class)
                .withRequiredImplements(DelegateFileIO.class)
                .loadAll())
        .extracting(Service::serviceName, Service::serviceType)
        .contains(tuple("delegating", DelegatingFileIO.class), tuple("hadoop", HadoopFileIO.class))
        .doesNotContain(
            tuple("my-own", MyOwnFileIO.class),
            tuple("in-memory", InMemoryFileIO.class),
            tuple("java-svc", JavaServiceFileIO.class));
  }

  @Test
  public void loadLegacyServices() {
    List<FileIOService> legacyServices = new ArrayList<>();
    ServiceProviderImpl.loadLegacyServices(
        FileIO.class, FileIOService.class, emptySet(), legacyServices::add);

    soft.assertThat(legacyServices)
        .anyMatch(svc -> svc.serviceName().equals("my-own"))
        .anyMatch(svc -> svc.serviceType().equals(MyOwnFileIO.class))
        .anyMatch(svc -> svc.serviceName().equals("delegating"))
        .anyMatch(svc -> svc.serviceType().equals(DelegatingFileIO.class))
        .anyMatch(svc -> svc.serviceName().equals("in-memory"))
        .anyMatch(svc -> svc.serviceType().equals(InMemoryFileIO.class))
        // defined as a Java service, only the 'java-service' name must match for legacy
        .noneMatch(svc -> svc.serviceName().equals("java-svc"))
        .anyMatch(svc -> svc.serviceName().equals("java-service"))
        .anyMatch(svc -> svc.serviceType().equals(JavaServiceFileIO.class));
  }

  @Test
  public void loadServices() {
    List<FileIOService> services =
        ServiceProviderImpl.loadServices(FileIO.class, FileIOService.class);

    soft.assertThat(services)
        .anyMatch(svc -> svc.serviceName().equals("my-own"))
        .anyMatch(svc -> svc.serviceType().equals(MyOwnFileIO.class))
        .anyMatch(svc -> svc.serviceName().equals("delegating"))
        .anyMatch(svc -> svc.serviceType().equals(DelegatingFileIO.class))
        .anyMatch(svc -> svc.serviceName().equals("in-memory"))
        .anyMatch(svc -> svc.serviceType().equals(InMemoryFileIO.class))
        // defined as a Java service, only the 'java-svc' name must match
        .anyMatch(svc -> svc.serviceName().equals("java-svc"))
        .noneMatch(svc -> svc.serviceName().equals("java-service"))
        .anyMatch(svc -> svc.serviceType().equals(JavaServiceFileIO.class));
  }
}
