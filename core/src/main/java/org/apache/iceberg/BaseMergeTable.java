/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseMergeTable extends SnapshotProducer<BaseMergeTable> implements MergeTable {
  private static final Logger LOG = LoggerFactory.getLogger(BaseMergeTable.class);

  private final String name;
  private final TableOperations ops;
  private TableMetadata base;

  private final List<Long> snapshotsIdsToCherryPick = new ArrayList<>();

  BaseMergeTable(String name, TableOperations ops) {
    super(ops);
    this.name = name;
    this.ops = ops;
    this.base = ops.current();
  }

  @Override
  protected BaseMergeTable self() {
    return this;
  }

  @Override
  protected void cleanUncommitted(Set<ManifestFile> committed) {
  }

  @Override
  public MergeTable fromTable(Table fromTable) {
    Preconditions.checkArgument(fromTable instanceof HasTableOperations, "fromTable must implement HasTableOperations");
    TableOperations fromOps = ((HasTableOperations) fromTable).operations();
    TableMetadata fromMetadata = fromOps.current();

    List<Snapshot> snapshotsToCherryPick = findSnapshotsToCherryPick(fromMetadata);
    if (snapshotsToCherryPick.isEmpty()) {
      // Nothing to do
      return this;
    }

    TableMetadata newBase = base;

    // Note: only need the functionality in `unionByNameWith()`, but won't `commit()` via SchemaUpdate.
    SchemaUpdate schemaUpdate = new SchemaUpdate(null, newBase);

    for (int i = snapshotsToCherryPick.size() - 1; i >= 0; i++) {
      Snapshot snapshotToCherryPick = snapshotsToCherryPick.get(i);
      Schema newSchema = schemaForCherryPick(schemaUpdate, newBase, fromMetadata, snapshotToCherryPick);

      snapshotToCherryPick = ((BaseSnapshot) snapshotToCherryPick).withUpdatedSchemaId(newSchema.schemaId());

      // Update the table-metadata with the schema to use
      newBase = newBase.updateSchema(newSchema, Math.max(newBase.lastColumnId(), fromMetadata.lastColumnId()));

      newBase = newBase.addStagedSnapshot(snapshotToCherryPick);
    }

    this.base = newBase;

    return this;
  }

  @Override
  public Snapshot apply() {
    if (snapshotsIdsToCherryPick.isEmpty()) {
      return base.currentSnapshot();
    }

    SnapshotManager snapshotManager = new SnapshotManager(name, ops);
    snapshotsIdsToCherryPick.forEach(snapshotManager::cherrypick);
    snapshotManager.commit();
    return snapshotManager.apply();
  }

  @Override
  public Object updateEvent() {
    return MergeTable.super.updateEvent();
  }

  private List<Snapshot> findSnapshotsToCherryPick(TableMetadata fromMetadata) {
    Snapshot commonAncestor = null;
    List<Snapshot> snapshotsToCherryPick = new ArrayList<>();
    for (Snapshot fromSnapshot = fromMetadata.currentSnapshot();
        fromSnapshot != null;
        fromSnapshot = fromSnapshot.parentId() != null ? fromMetadata.snapshot(fromSnapshot.parentId()) : null) {
      Snapshot commonAncestorCandidate = base.snapshot(fromSnapshot.snapshotId());
      if (commonAncestorCandidate != null && commonAncestorCandidate.equals(fromSnapshot)) {
        commonAncestor = commonAncestorCandidate;
        break;
      }
      snapshotsToCherryPick.add(fromSnapshot);
    }
    if (commonAncestor == null) {
      throw new IllegalArgumentException("No common ancestor snapshot found");
    }
    return snapshotsToCherryPick;
  }

  private Schema schemaForCherryPick(SchemaUpdate schemaUpdate, TableMetadata newBase,
      TableMetadata fromMetadata,
      Snapshot snapshotToCherryPick) {
    Schema snapshotSchema = fromMetadata.schemasById().get(snapshotToCherryPick.schemaId());

    Schema existingSchema = newBase.schemasById().get(snapshotToCherryPick.schemaId());
    if (existingSchema != null && snapshotSchema.sameSchema(existingSchema)) {
      return existingSchema;
    }

    // Get a schema that is compatible with both the current one in the target table and the
    // current schema in the from-table.
    Schema mergedSchema = schemaUpdate.unionByNameWith(snapshotSchema).apply();

    // Try to find an existing schema that is the same as the expected schema. If none exists,
    // use the merged one.
    return Stream.concat(Stream.of(newBase.schema()),
            newBase.schemas().stream()).filter(baseSchema -> baseSchema.sameSchema(mergedSchema))
        .findAny()
        .orElse(mergedSchema);
  }
}
