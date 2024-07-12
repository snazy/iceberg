# Iceberg Services

Services in Apache Iceberg are technically implementations of a certain interface. The Java Service
loading mechanism, as defined via the Java class `java.util.ServiceLoader`, is used to find and
existing services.

Each individual type of service (for example the service for `FileIO`) has a Java interface that
extends the `org.apache.iceberg.services.Service` interface, (for example `FileIOService`). This
service interface name is used by the Java service loader.

The `Service` interface exposes three attributes:
1. the name of the service, which is used to distinguish different implementations of the service.
   For the `FileIOService` example, this is something like `s3` or `hadoop`.
2. the type of the service, which is the actual public implementation class, which _should_ have
   a public no-arg constructor and must be a non-abstract public Java class.
3. the priority of the service, which is an ordinal to define the order in which services are
   returned to callers.

The actual service implementation interface (for example `FileIO`) is not mandated by this service
framework.

Service implementation interfaces _may_ extend the `WithProperties` interface to allow calling
the default `Service.build(Map<String, String> properties)`.

## Service lookups

Service instances are looked up using the `ServiceProvider` class, which takes the service interface
type and the implementation interface type.

Several functions on `ServiceProvider` can be used to add filters by service name, service type,
additional implemented Java interfaces, and so on.

Example lookups:
* `ServiceProvider.withName("s3").load().buildUninitialized()` to get the `S3FileIO`
* `ServiceProvider.withLegacyImpl("org.apache.iceberg.gcp.gcs.GCSFileIO").load().buildUninitialized()` to get the `GCSFileIO`
* `ServiceProvider.withLegacyImpl("gcs").load().buildUninitialized()` to get the `GCSFileIO`

## Legacy services

The current `ServiceProvider` implementation allows using existing implementations even without
a specific service interface implementation (and the required entry in a `META-INF/services/*`
resource file). This has been implemented by using Jandex indexes, which are added to all Iceberg
jars.

3rd party implementations should implement the service interface and provide a corresponding
`META-INF/services/*` file. Although not recommended, 3rd party implementation _may_ opt to use
the Jandex based approach, in which case they have to provide a jar with a Jandex index.

The last fallback implemented in `ServiceProvider` is an attempt to load the class using
`Class.forName()`.
