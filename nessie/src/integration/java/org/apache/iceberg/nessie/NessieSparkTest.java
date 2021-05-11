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

package org.apache.iceberg.nessie;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.function.Function;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.projectnessie.client.NessieClient;
import org.projectnessie.model.Branch;

public class NessieSparkTest {

  @ClassRule
  public static TemporaryFolder temp = new TemporaryFolder();

  private static File warehouse;

  private static NessieClient client;

  private static SparkSession spark;
  private static SparkContext sc;

  private static SparkSession sparkDev;
  private static NessieCatalog catalogDev;

  @BeforeClass
  public static void initSparkAndCatalog() throws IOException {
    String port = System.getProperty("quarkus.http.test-port", "19120");
    String endpoint = String.format("http://localhost:%s/api/v1", port);

    warehouse = temp.newFile("warehouse");
    Assert.assertTrue(warehouse.delete());
    String warehouseUrl = "file://" + warehouse;

    String catalogPrefix = "spark.sql.catalog.int_test";

    spark = SparkSession.builder()
        .master("local[2]")
        .config("spark.testing", "true")
        .config("spark.sql.extensions", IcebergSparkSessionExtensions.class.getName())
        .config(catalogPrefix + ".warehouse", warehouseUrl)
        .config(catalogPrefix + ".url", endpoint)
        .config(catalogPrefix + ".ref", "main")
        .config(catalogPrefix + ".catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config(catalogPrefix + ".auth_type", "NONE")
        .config(catalogPrefix + ".cache-enabled", "false")
        .config(catalogPrefix, "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate();

    sparkDev = spark.newSession();
    sparkDev.conf().set(catalogPrefix + ".ref", "dev");

    sc = spark.sparkContext();

    final Function<String, String> fixPrefix = x -> x.replace("nessie.", catalogPrefix + ".");
    client = NessieClient.builder().fromConfig(x -> {
      String key = fixPrefix.apply(x);
      return sc.conf().contains(key) ? sc.conf().get(key) : null;
    }).build();

    createBranch("dev");

    catalogDev = (NessieCatalog)
        CatalogUtil.loadCatalog(NessieCatalog.class.getName(), "int_test", ImmutableMap.of(
                "ref", "dev",
                "url", endpoint,
                "warehouse", warehouseUrl
        ), sc.hadoopConfiguration());
  }

  private static void createBranch(String dev) throws IOException {
    client.getTreeApi().createReference(Branch.of("dev", null));
  }

  @Test
  public void testCreateTableInBranch() {
    TableIdentifier regionName = TableIdentifier.parse("testing.region");
    Schema regionSchema = new Schema(ImmutableList.of(
        NestedField.optional(1, "R_REGIONKEY", LongType.get()),
        NestedField.optional(2, "R_NAME", StringType.get()),
        NestedField.optional(3, "R_COMMENT", StringType.get())
    ));
    PartitionSpec regionSpec = PartitionSpec.unpartitioned();

    catalogDev.createTable(regionName, regionSchema, regionSpec);

    // Our dummy Parquet file to save
    URL regionUrl = NessieSparkTest.class.getResource("region.parquet");
    Assert.assertNotNull(regionUrl);
    Assert.assertEquals("file", regionUrl.getProtocol());

    Dataset<Row> regionDf = sparkDev.read().parquet(regionUrl.getPath());
    DataFrameWriter<Row> regionWriter = regionDf.write().format("iceberg").mode("overwrite");
    regionWriter.save("int_test.testing.region");
  }
}
