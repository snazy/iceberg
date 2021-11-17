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

import java.util.Collections;
import java.util.Map;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.assertj.core.api.AbstractStringAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Reference;

public class TestBranchVisibility extends BaseTestIceberg {

  private final TableIdentifier tableIdentifier1 = TableIdentifier.of("test-ns", "table1");
  private final TableIdentifier tableIdentifier2 = TableIdentifier.of("test-ns", "table2");
  private NessieCatalog testCatalog;
  private int schemaCounter = 1;

  public TestBranchVisibility() {
    super("main");
  }

  @BeforeEach
  public void before() throws NessieNotFoundException, NessieConflictException {
    createTable(tableIdentifier1, 1); // table 1
    createTable(tableIdentifier2, 1); // table 2
    catalog.refresh();
    createBranch("test", catalog.currentHash());
    testCatalog = initCatalog("test");
  }

  @AfterEach
  public void after() throws NessieNotFoundException, NessieConflictException {
    catalog.dropTable(tableIdentifier1);
    catalog.dropTable(tableIdentifier2);
    catalog.refresh();
    for (Reference reference : api.getAllReferences().get().getReferences()) {
      if (!reference.getName().equals("main")) {
        api.deleteBranch().branch((Branch) reference).delete();
      }
    }
    testCatalog = null;
  }

  @Test
  public void testBranchNoChange() {
    testCatalogEquality(catalog, testCatalog, true, true, () -> { });
  }

  /** Ensure catalogs can't see each others updates. */
  @Test
  public void testUpdateCatalogs() {
    testCatalogEquality(catalog, testCatalog, false, true,
        () -> updateSchema(catalog, tableIdentifier1));

    testCatalogEquality(catalog, testCatalog, true, false,
        () -> updateSchema(catalog, tableIdentifier2));
  }

  @Test
  public void testCatalogOnReference() {
    updateSchema(catalog, tableIdentifier1);
    updateSchema(testCatalog, tableIdentifier2);

    // catalog created with ref points to same catalog as above
    NessieCatalog refCatalog = initCatalog("test");
    testCatalogEquality(refCatalog, testCatalog, true, true, () -> { });

    // catalog created with hash points to same catalog as above
    NessieCatalog refHashCatalog = initCatalog("main");
    testCatalogEquality(refHashCatalog, catalog, true, true, () -> { });
  }

  @Test
  public void testCatalogWithTableNames() {
    updateSchema(testCatalog, tableIdentifier2);

    String mainName = "main";

    // asking for table@branch gives expected regardless of catalog
    Assertions.assertThat(metadataLocation(catalog, TableIdentifier.of("test-ns", "table1@test")))
        .isEqualTo(metadataLocation(testCatalog, tableIdentifier1));

    // Asking for table@branch gives expected regardless of catalog.
    // Earlier versions used "table1@" + tree.getReferenceByName("main").getHash() before, but since
    // Nessie 0.8.2 the branch name became mandatory and specifying a hash within a branch is not
    // possible.
    Assertions.assertThat(metadataLocation(catalog, TableIdentifier.of("test-ns", "table1@" + mainName)))
        .isEqualTo(metadataLocation(testCatalog, tableIdentifier1));
  }

  @Test
  public void testConcurrentChanges() {
    NessieCatalog emptyTestCatalog = initCatalog("test");
    updateSchema(testCatalog, tableIdentifier1);
    // Updating table with out of date hash. We expect this to succeed because of retry despite the conflict.
    updateSchema(emptyTestCatalog, tableIdentifier1);
  }

  /**
   * Complex-ish test case that verifies that both the snapshot-ID and schema-ID are properly set
   * and retained when working with a mixture of DDLs and DMLs across multiple branches.
   */
  @Test
  public void testStateTrackingOnMultipleBranches() throws Exception {

    String branchTest = "test";
    String branchA = "branch_a";
    String branchB = "branch_b";

    NessieCatalog catalog = initCatalog(branchTest);
    verifySchema(catalog, tableIdentifier1, Types.LongType.get());
    String initialLocation = metadataLocation(catalog, tableIdentifier1);

    // Verify last-column-id
    verifyRefState(catalog, tableIdentifier1, -1L, 0);

    // Add a row and verify that the
    String metadataOnTest = addRow(catalog, tableIdentifier1, "initial-data",
        Collections.singletonMap("id0", 1L));
    Assertions.assertThat(metadataOnTest).isNotEqualTo(initialLocation);
    long snapshotIdOnTest = snapshotIdFromMetadata(catalog, metadataOnTest);
    verifyRefState(catalog, tableIdentifier1, snapshotIdOnTest, 0);

    String hash = api.getReference().refName(branchTest).get().getHash();
    api.createReference().reference(Branch.of(branchA, hash)).sourceRefName(branchTest).create();
    api.createReference().reference(Branch.of(branchB, hash)).sourceRefName(branchTest).create();

    NessieCatalog catalogBranchA = initCatalog(branchA);
    // branchA hasn't been modified yet, so it must be "equal" to branch "test"
    verifyRefState(catalogBranchA, tableIdentifier1, snapshotIdOnTest, 0);
    // updateSchema updates the schema on branch "branch_a", but not on branch "test"
    updateSchema(catalogBranchA, tableIdentifier1, Types.StringType.get());
    verifyRefState(catalogBranchA, tableIdentifier1, snapshotIdOnTest, 1);
    verifySchema(catalogBranchA, tableIdentifier1, Types.LongType.get(), Types.StringType.get());
    verifyRefState(catalog, tableIdentifier1, snapshotIdOnTest, 0);

    String metadataOnA1 = addRow(catalogBranchA, tableIdentifier1, "branch-a-1",
        ImmutableMap.of("id0", 2L, "id1", "hello"));
    // addRow() must produce a new metadata
    Assertions.assertThat(metadataOnA1).isNotEqualTo(metadataOnTest);
    long snapshotIdOnA1 = snapshotIdFromMetadata(catalogBranchA, metadataOnA1);
    Assertions.assertThat(snapshotIdOnA1).isNotEqualTo(snapshotIdOnTest);
    verifyRefState(catalogBranchA, tableIdentifier1, snapshotIdOnA1, 1);
    verifyRefState(catalog, tableIdentifier1, snapshotIdOnTest, 0);

    NessieCatalog catalogBranchB = initCatalog(branchB);
    long snapshotIdOnB = snapshotIdFromNessie(catalogBranchB, tableIdentifier1);
    Assertions.assertThat(snapshotIdOnB).isEqualTo(snapshotIdOnTest);
    // branchB hasn't been modified yet, so it must be "equal" to branch "test"
    verifyRefState(catalogBranchB, tableIdentifier1, snapshotIdOnB, 0);
    // updateSchema should use schema-id 2 because schema-id 1 has already been used by the above
    // schema change in branch_a.
    updateSchema(catalogBranchB, tableIdentifier1, Types.LongType.get());
    verifyRefState(catalogBranchB, tableIdentifier1, snapshotIdOnB, 2);
    verifySchema(catalogBranchB, tableIdentifier1, Types.LongType.get(), Types.LongType.get());
    verifyRefState(catalog, tableIdentifier1, snapshotIdOnTest, 0);

    String metadataOnB1 = addRow(catalogBranchB, tableIdentifier1, "branch-b-1",
        ImmutableMap.of("id0", 3L, "id2", 42L));
    long snapshotIdOnB1 = snapshotIdFromMetadata(catalogBranchB, metadataOnB1);
    // addRow() must produce a new metadata
    Assertions.assertThat(metadataOnB1)
        .isNotEqualTo(metadataOnA1)
        .isNotEqualTo(metadataOnTest);
    verifyRefState(catalogBranchB, tableIdentifier1, snapshotIdOnB1, 2);
    verifyRefState(catalog, tableIdentifier1, snapshotIdOnTest, 0);

    // repeat addRow() against branchA
    catalogBranchA = initCatalog(branchA);
    verifySchema(catalogBranchA, tableIdentifier1, Types.LongType.get(), Types.StringType.get());
    String metadataOnA2 = addRow(catalogBranchA, tableIdentifier1, "branch-a-2",
        ImmutableMap.of("id0", 4L, "id1", "hello"));
    long snapshotIdOnA2 = snapshotIdFromMetadata(catalogBranchA, metadataOnA2);
    Assertions.assertThat(metadataOnA2)
        .isNotEqualTo(metadataOnA1)
        .isNotEqualTo(metadataOnB1)
        .isNotEqualTo(metadataOnTest);
    verifyRefState(catalogBranchA, tableIdentifier1, snapshotIdOnA2, 1);

    // repeat addRow() against branchB
    catalogBranchB = initCatalog(branchB);
    verifySchema(catalogBranchB, tableIdentifier1, Types.LongType.get(), Types.LongType.get());
    String metadataOnB2 = addRow(catalogBranchB, tableIdentifier1, "branch-b-2",
        ImmutableMap.of("id0", 5L, "id2", 666L));
    long snapshotIdOnB2 = snapshotIdFromMetadata(catalogBranchA, metadataOnB2);
    Assertions.assertThat(metadataOnB2)
        .isNotEqualTo(metadataOnA1).isNotEqualTo(metadataOnA2)
        .isNotEqualTo(metadataOnB1)
        .isNotEqualTo(metadataOnTest);
    verifyRefState(catalogBranchB, tableIdentifier1, snapshotIdOnB2, 2);

    // sanity check, branch "test" must not have changed
    verifyRefState(catalog, tableIdentifier1, snapshotIdOnTest, 0);
  }

  private void verifyRefState(NessieCatalog catalog, TableIdentifier identifier, long snapshotId, int schemaId)
      throws Exception {
    IcebergTable icebergTable = loadIcebergTable(catalog, identifier);
    Assertions.assertThat(icebergTable)
        .extracting(IcebergTable::getSnapshotId, IcebergTable::getSchemaId)
        .containsExactly(snapshotId, schemaId);
  }

  private long snapshotIdFromNessie(NessieCatalog catalog, TableIdentifier identifier) throws Exception {
    IcebergTable icebergTable = loadIcebergTable(catalog, identifier);
    return icebergTable.getSnapshotId();
  }

  private long snapshotIdFromMetadata(NessieCatalog catalog, String metadataLocation) {
    Snapshot snapshot = TableMetadataParser.read(catalog.fileIO(), metadataLocation).currentSnapshot();
    return snapshot != null ? snapshot.snapshotId() : -1;
  }

  private IcebergTable loadIcebergTable(NessieCatalog catalog, TableIdentifier identifier)
      throws NessieNotFoundException {
    ContentKey key = NessieUtil.toKey(identifier);
    return api.getContent().refName(catalog.currentRefName()).key(key)
        .get().get(key).unwrap(IcebergTable.class).get();
  }

  private String addRow(NessieCatalog catalog, TableIdentifier identifier, String fileName, Map<String, Object> data)
      throws Exception {
    Table table = catalog.loadTable(identifier);
    GenericRecordBuilder recordBuilder =
        new GenericRecordBuilder(AvroSchemaUtil.convert(table.schema(), table.name()));
    data.forEach(recordBuilder::set);

    String fileLocation = writeRecordsToFile(table, table.schema(), fileName,
        Collections.singletonList(recordBuilder.build()));
    DataFile dataFile = makeDataFile(table, fileLocation);
    table.newAppend().appendFile(dataFile).commit();

    return metadataLocation(catalog, identifier);
  }

  private void verifySchema(NessieCatalog catalog, TableIdentifier identifier, Type... types) {
    Assertions.assertThat(catalog.loadTable(identifier))
        .extracting(t -> t.schema().columns().stream().map(NestedField::type))
        .asInstanceOf(InstanceOfAssertFactories.stream(Type.class))
        .containsExactly(types);
  }

  private void updateSchema(NessieCatalog catalog, TableIdentifier identifier) {
    updateSchema(catalog, identifier, Types.LongType.get());
  }

  private void updateSchema(NessieCatalog catalog, TableIdentifier identifier, Type type) {
    catalog.loadTable(identifier).updateSchema().addColumn("id" + schemaCounter++, type).commit();
  }

  private void testCatalogEquality(
      NessieCatalog catalog, NessieCatalog compareCatalog, boolean table1Equal, boolean table2Equal,
      ThrowingCallable callable) {
    String testTable1 = metadataLocation(compareCatalog, tableIdentifier1);
    String testTable2 = metadataLocation(compareCatalog, tableIdentifier2);

    try {
      callable.call();
    } catch (RuntimeException e) {
      throw e;
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }

    String table1 = metadataLocation(catalog, tableIdentifier1);
    String table2 = metadataLocation(catalog, tableIdentifier2);

    AbstractStringAssert<?> assertion = Assertions.assertThat(table1)
        .describedAs("Table %s on ref %s should%s be equal to table %s on ref %s",
            tableIdentifier1.name(),
            catalog.currentRefName(),
            table1Equal ? "" : " not",
            tableIdentifier1.name(),
            compareCatalog.currentRefName());
    if (table1Equal) {
      assertion.isEqualTo(testTable1);
    } else  {
      assertion.isNotEqualTo(testTable1);
    }

    assertion = Assertions.assertThat(table2)
        .describedAs("Table %s on ref %s should%s be equal to table %s on ref %s",
            tableIdentifier2.name(),
            catalog.currentRefName(),
            table2Equal ? "" : " not",
            tableIdentifier2.name(),
            compareCatalog.currentRefName());
    if (table2Equal) {
      assertion.isEqualTo(testTable2);
    } else  {
      assertion.isNotEqualTo(testTable2);
    }
  }
}
