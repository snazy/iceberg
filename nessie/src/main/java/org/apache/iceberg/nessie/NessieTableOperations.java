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
import java.util.UUID;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.BaseSnapshotBuilder;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.projectnessie.client.NessieClient;
import org.projectnessie.client.http.HttpClientException;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Contents;
import org.projectnessie.model.ContentsKey;
import org.projectnessie.model.GlobalContents;
import org.projectnessie.model.IcebergSnapshot;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableIcebergSnapshot;
import org.projectnessie.model.ImmutableOperations;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Operations;

/**
 * Nessie implementation of Iceberg TableOperations.
 */
public class NessieTableOperations extends BaseMetastoreTableOperations {

  private final NessieClient client;
  private final ContentsKey key;
  private final UpdateableReference reference;
  private IcebergSnapshot table;
  private final FileIO fileIO;
  private final Map<String, String> catalogOptions;

  /**
   * Create a nessie table operations given a table identifier.
   */
  NessieTableOperations(
      ContentsKey key,
      UpdateableReference reference,
      NessieClient client,
      FileIO fileIO,
      Map<String, String> catalogOptions) {
    this.key = key;
    this.reference = reference;
    this.client = client;
    this.fileIO = fileIO;
    this.catalogOptions = catalogOptions;
  }

  @Override
  protected String tableName() {
    return key.toString();
  }

  @Override
  protected TableMetadata validateRefreshedMetadata(TableMetadata metadata) {
    // Update the TableMetadata to use the snapshot referenced in IcebergSnapshot.

    Long snapshotId = table.getCurrentSnapshotId();
    if (snapshotId == null) {
      throw new IllegalStateException(String.format("Nessie-contents for table '%s' in reference '%s' at '%s' has no snapshot-ID",
          tableName(), reference.getName(), reference.getHash()));
    }
    Snapshot snapshot = metadata.snapshot(snapshotId);
    if (snapshot == null) {
      throw new IllegalStateException(String.format("Nessie-contents for table '%s' in reference '%s' at '%s' references snapshot-ID %d, which does not exist",
          tableName(), reference.getName(), reference.getHash(), snapshotId));
    }
    return metadata.replaceCurrentSnapshot(snapshot);
  }

  @Override
  protected void doRefresh() {
    try {
      reference.refresh();
    } catch (NessieNotFoundException e) {
      throw new RuntimeException("Failed to refresh as ref is no longer valid.", e);
    }
    String metadataLocation = null;
    try {
      Contents contents = client.getContentsApi().getContents(key, reference.getName(), reference.getHash());
      this.table = contents.unwrap(IcebergSnapshot.class)
          .orElseThrow(() ->
              new IllegalStateException("Cannot refresh iceberg table: " +
                  String.format("Nessie points to a non-Iceberg object for path: %s.", key)));
      metadataLocation = table.getMetadataLocation();
    } catch (NessieNotFoundException ex) {
      if (currentMetadataLocation() != null) {
        throw new NoSuchTableException(ex, "No such table %s", key);
      }
    }
    refreshFromMetadataLocation(metadataLocation, 2);
  }

  @Override
  protected void doCommit(TableMetadata base, TableMetadata metadata) {
    reference.checkMutable();

    metadata = ensureUpdatedSnapshot(base, metadata);

    String newMetadataLocation = writeNewMetadata(metadata, currentVersion() + 1);

    boolean delete = true;
    try {
      GlobalContents expectedGlobal = null;

      ImmutableIcebergSnapshot.Builder newTable = ImmutableIcebergSnapshot.builder();
      if (table != null) {
        newTable.id(table.getId());
        expectedGlobal = IcebergTable.of(table.getMetadataLocation());
      }

      newTable.metadataLocation(newMetadataLocation)
          .currentSnapshotId(metadata.currentSnapshot().snapshotId());

      Operations op = ImmutableOperations.builder().addOperations(Operation.Put.of(key, newTable.build(), expectedGlobal))
          .commitMeta(NessieUtil.buildCommitMetadata("iceberg commit", catalogOptions)).build();
      Branch branch = client.getTreeApi().commitMultipleOperations(reference.getAsBranch().getName(),
          reference.getHash(), op);
      reference.updateReference(branch);

      delete = false;
    } catch (NessieConflictException ex) {
      throw new CommitFailedException(ex, "Commit failed: Reference hash is out of date. " +
          "Update the reference %s and try again", reference.getName());
    } catch (HttpClientException ex) {
      // Intentionally catch all nessie-client-exceptions here and not just the "timeout" variant
      // to catch all kinds of network errors (e.g. connection reset). Network code implementation
      // details and all kinds of network devices can induce unexpected behavior. So better be
      // safe than sorry.
      delete = false;
      throw new CommitStateUnknownException(ex);
    } catch (NessieNotFoundException ex) {
      throw new RuntimeException(String.format("Commit failed: Reference %s no longer exist", reference.getName()), ex);
    } finally {
      if (delete) {
        io().deleteFile(newMetadataLocation);
        // TODO delete manifestListPath created via ensureUpdatedSnapshot -> withSnapshot as well
      }
    }
  }

  private TableMetadata ensureUpdatedSnapshot(TableMetadata base, TableMetadata metadata) {
    // Nessie-commits require a new snapshot for every Nessie-commit to be able to track changes.
    //
    // For example schema changes that happen on one Nessie-branch must not "leak" into other
    // Nessie-branches.
    //
    // The "per-Nessie-commit-contents" contain a pointer to the current TableMetadata and the
    // ID of the current snapshot.
    //
    // Need a (new) snapshot, if:
    // - 'metadata' has no snapshot
    // - the current snapshot in 'base' is the same as in 'metadata'

    Snapshot baseSnapshot = base != null ? base.currentSnapshot() : null;
    Snapshot snapshot = metadata.currentSnapshot();
    if (snapshot == null || (baseSnapshot != null && baseSnapshot.snapshotId() == snapshot.snapshotId())) {
      metadata = withSnapshot(metadata);
    }
    return metadata;
  }

  private TableMetadata withSnapshot(TableMetadata metadata) {
    Long parentSnapshotId = metadata.currentSnapshot() != null ?
        metadata.currentSnapshot().snapshotId() : null;
    long sequenceNumber = metadata.nextSequenceNumber();

    Snapshot snapshot = new BaseSnapshotBuilder(metadata, io(), metadata.formatVersion(),
        "Nessie-commit", newSnapshotId()) {
      final String commitUUID = UUID.randomUUID().toString();

      @Override
      protected ManifestFile manifest(ManifestFile existing) {
        return existing;
      }

      @Override
      protected void addManifestListLocation(String manifestListLocation) {
        // ignore
      }

      @Override
      protected OutputFile manifestListPath() {
        return io().newOutputFile(metadataFileLocation(metadata, FileFormat.AVRO.addExtension(
            String.format("snap-%d-%d-%s", snapshotId, 1, commitUUID))));
      }

      @Override
      protected Map<String, String> summary() {
        return ImmutableMap.of();
      }
    }.buildSnapshot(parentSnapshotId, sequenceNumber, Collections.emptyList());

    return metadata.replaceCurrentSnapshot(snapshot);
  }

  @Override
  public FileIO io() {
    return fileIO;
  }
}
