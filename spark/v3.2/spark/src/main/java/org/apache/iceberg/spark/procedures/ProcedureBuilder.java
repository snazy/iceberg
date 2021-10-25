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

package org.apache.iceberg.spark.procedures;

import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.Procedure;

/** Builder interface to create a new {@link Procedure} instance. */
public interface ProcedureBuilder {

  ProcedureBuilder withTableCatalog(TableCatalog tableCatalog);

  Procedure build();

  /**
   * Helper class to create a {@link ProcedureBuilder}.
   * @param <T> type of the procedure.
   */
  abstract class Builder<T extends Procedure> implements ProcedureBuilder {
    private TableCatalog tableCatalog;

    @Override
    public Builder<T> withTableCatalog(TableCatalog newTableCatalog) {
      this.tableCatalog = newTableCatalog;
      return this;
    }

    @Override
    public T build() {
      return doBuild();
    }

    protected abstract T doBuild();

    TableCatalog tableCatalog() {
      return tableCatalog;
    }
  }
}
