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
package com.facebook.presto.execution;

import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.RenameTable;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.metadata.MetadataUtil.createQualifiedObjectName;
import static com.facebook.presto.metadata.MetadataUtil.getConnectorIdOrThrow;
import static com.facebook.presto.metadata.MetadataUtil.targetTableCatalogError;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_TABLE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TABLE_ALREADY_EXISTS;
import static com.google.common.util.concurrent.Futures.immediateFuture;

public class RenameTableTask
        implements DDLDefinitionTask<RenameTable>
{
    @Override
    public String getName()
    {
        return "RENAME TABLE";
    }

    @Override
    public ListenableFuture<?> execute(RenameTable statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, Session session, List<Expression> parameters, WarningCollector warningCollector)
    {
        QualifiedObjectName tableName = createQualifiedObjectName(session, statement, statement.getSource());
        Optional<TableHandle> tableHandle = metadata.getMetadataResolver(session).getTableHandle(tableName);
        if (!tableHandle.isPresent()) {
            if (!statement.isExists()) {
                throw new SemanticException(MISSING_TABLE, statement, "Table '%s' does not exist", tableName);
            }
            return immediateFuture(null);
        }

        Optional<MaterializedViewDefinition> optionalMaterializedView = metadata.getMetadataResolver(session).getMaterializedView(tableName);
        if (optionalMaterializedView.isPresent()) {
            if (!statement.isExists()) {
                throw new SemanticException(NOT_SUPPORTED, statement, "'%s' is a materialized view, and rename is not supported", tableName);
            }
            return immediateFuture(null);
        }

        QualifiedObjectName target = createQualifiedObjectName(session, statement, statement.getTarget());
        getConnectorIdOrThrow(session, metadata, target.getLegacyCatalogName(), statement, targetTableCatalogError);
        if (metadata.getMetadataResolver(session).getTableHandle(target).isPresent()) {
            throw new SemanticException(TABLE_ALREADY_EXISTS, statement, "Target table '%s' already exists", target);
        }
        if (!tableName.getLegacyCatalogName().equals(target.getLegacyCatalogName())) {
            throw new SemanticException(NOT_SUPPORTED, statement, "Table rename across catalogs is not supported");
        }
        accessControl.checkCanRenameTable(session.getRequiredTransactionId(), session.getIdentity(), session.getAccessControlContext(), tableName, target);

        metadata.renameTable(session, tableHandle.get(), target);

        return immediateFuture(null);
    }
}
