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

import static org.apache.iceberg.TableProperties.MANIFEST_LISTS_ENABLED;
import static org.apache.iceberg.TableProperties.MANIFEST_LISTS_ENABLED_DEFAULT;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;

public abstract class BaseSnapshotBuilder {

  private final TableMetadata base;
  private final FileIO opsIo;
  private final int currentFormatVersion;
  private final String operation;
  protected final long snapshotId;

  protected BaseSnapshotBuilder(TableMetadata base, FileIO opsIo,
      int currentFormatVersion, String operation, long snapshotId) {
    this.base = base;
    this.opsIo = opsIo;
    this.currentFormatVersion = currentFormatVersion;
    this.operation = operation;
    this.snapshotId = snapshotId;
  }

  protected abstract ManifestFile manifest(ManifestFile existing);

  protected abstract OutputFile manifestListPath();

  protected abstract void addManifestListLocation(String manifestListLocation);

  protected abstract Map<String, String> summary();

  public Snapshot buildSnapshot(Long parentSnapshotId, long sequenceNumber, List<ManifestFile> manifests) {
    if (base.formatVersion() > 1 || base.propertyAsBoolean(MANIFEST_LISTS_ENABLED, MANIFEST_LISTS_ENABLED_DEFAULT)) {
      OutputFile manifestList = manifestListPath();

      try (ManifestListWriter writer = ManifestLists.write(
          currentFormatVersion, manifestList, snapshotId, parentSnapshotId,
          sequenceNumber)) {

        // keep track of the manifest lists created
        addManifestListLocation(manifestList.location());

        ManifestFile[] manifestFiles = new ManifestFile[manifests.size()];

        Tasks.range(manifestFiles.length)
            .stopOnFailure().throwFailureWhenFinished()
            .executeWith(ThreadPools.getWorkerPool())
            .run(index ->
                manifestFiles[index] = manifest(manifests.get(index)));

        writer.addAll(Arrays.asList(manifestFiles));

      } catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to write manifest list file");
      }

      return new BaseSnapshot(opsIo,
          sequenceNumber, snapshotId, parentSnapshotId, System.currentTimeMillis(), operation, summary(base),
          base.currentSchemaId(), manifestList.location());

    } else {
      return new BaseSnapshot(opsIo,
          snapshotId, parentSnapshotId, System.currentTimeMillis(), operation, summary(base),
          base.currentSchemaId(), manifests);
    }
  }

  /**
   * Returns the snapshot summary from the implementation and updates totals.
   */
  private Map<String, String> summary(TableMetadata previous) {
    Map<String, String> summary = summary();

    if (summary == null) {
      return ImmutableMap.of();
    }

    Map<String, String> previousSummary;
    if (previous.currentSnapshot() != null) {
      if (previous.currentSnapshot().summary() != null) {
        previousSummary = previous.currentSnapshot().summary();
      } else {
        // previous snapshot had no summary, use an empty summary
        previousSummary = ImmutableMap.of();
      }
    } else {
      // if there was no previous snapshot, default the summary to start totals at 0
      ImmutableMap.Builder<String, String> summaryBuilder = ImmutableMap.builder();
      summaryBuilder
          .put(SnapshotSummary.TOTAL_RECORDS_PROP, "0")
          .put(SnapshotSummary.TOTAL_FILE_SIZE_PROP, "0")
          .put(SnapshotSummary.TOTAL_DATA_FILES_PROP, "0")
          .put(SnapshotSummary.TOTAL_DELETE_FILES_PROP, "0")
          .put(SnapshotSummary.TOTAL_POS_DELETES_PROP, "0")
          .put(SnapshotSummary.TOTAL_EQ_DELETES_PROP, "0");
      previousSummary = summaryBuilder.build();
    }

    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

    // copy all summary properties from the implementation
    builder.putAll(summary);

    updateTotal(
        builder, previousSummary, SnapshotSummary.TOTAL_RECORDS_PROP,
        summary, SnapshotSummary.ADDED_RECORDS_PROP, SnapshotSummary.DELETED_RECORDS_PROP);
    updateTotal(
        builder, previousSummary, SnapshotSummary.TOTAL_FILE_SIZE_PROP,
        summary, SnapshotSummary.ADDED_FILE_SIZE_PROP, SnapshotSummary.REMOVED_FILE_SIZE_PROP);
    updateTotal(
        builder, previousSummary, SnapshotSummary.TOTAL_DATA_FILES_PROP,
        summary, SnapshotSummary.ADDED_FILES_PROP, SnapshotSummary.DELETED_FILES_PROP);
    updateTotal(
        builder, previousSummary, SnapshotSummary.TOTAL_DELETE_FILES_PROP,
        summary, SnapshotSummary.ADDED_DELETE_FILES_PROP, SnapshotSummary.REMOVED_DELETE_FILES_PROP);
    updateTotal(
        builder, previousSummary, SnapshotSummary.TOTAL_POS_DELETES_PROP,
        summary, SnapshotSummary.ADDED_POS_DELETES_PROP, SnapshotSummary.REMOVED_POS_DELETES_PROP);
    updateTotal(
        builder, previousSummary, SnapshotSummary.TOTAL_EQ_DELETES_PROP,
        summary, SnapshotSummary.ADDED_EQ_DELETES_PROP, SnapshotSummary.REMOVED_EQ_DELETES_PROP);

    return builder.build();
  }


  private static void updateTotal(ImmutableMap.Builder<String, String> summaryBuilder,
      Map<String, String> previousSummary, String totalProperty,
      Map<String, String> currentSummary,
      String addedProperty, String deletedProperty) {
    String totalStr = previousSummary.get(totalProperty);
    if (totalStr != null) {
      try {
        long newTotal = Long.parseLong(totalStr);

        String addedStr = currentSummary.get(addedProperty);
        if (newTotal >= 0 && addedStr != null) {
          newTotal += Long.parseLong(addedStr);
        }

        String deletedStr = currentSummary.get(deletedProperty);
        if (newTotal >= 0 && deletedStr != null) {
          newTotal -= Long.parseLong(deletedStr);
        }

        if (newTotal >= 0) {
          summaryBuilder.put(totalProperty, String.valueOf(newTotal));
        }

      } catch (NumberFormatException e) {
        // ignore and do not add total
      }
    }
  }
}
