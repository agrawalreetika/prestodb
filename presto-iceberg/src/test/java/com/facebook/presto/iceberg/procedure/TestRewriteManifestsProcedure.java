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
package com.facebook.presto.iceberg.procedure;

import com.facebook.presto.iceberg.IcebergQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import static com.facebook.presto.iceberg.CatalogType.HADOOP;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestRewriteManifestsProcedure
        extends AbstractTestQueryFramework
{
    public static final String TEST_SCHEMA = "tpch";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setCatalogType(HADOOP)
                .build()
                .getQueryRunner();
    }

    private void createTable(String tableName)
    {
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, value VARCHAR)");
    }

    private void dropTable(String tableName)
    {
        assertQuerySucceeds("DROP TABLE IF EXISTS " + TEST_SCHEMA + "." + tableName);
    }

    @Test
    public void testRewriteManifestsUsingPositionalArgs()
    {
        String tableName = "rewrite_manifests_positional";
        createTable(tableName);
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'b')", 1);

            long replaceBefore = (long) computeScalar(format("SELECT count(*) FROM %s.\"%s$snapshots\" WHERE operation = 'replace'", TEST_SCHEMA, tableName));
            assertUpdate(format("CALL system.rewrite_manifests('%s', '%s')", TEST_SCHEMA, tableName));
            long replaceAfter = (long) computeScalar(format("SELECT count(*) FROM %s.\"%s$snapshots\" WHERE operation = 'replace'", TEST_SCHEMA, tableName));

            assertQuery(format("SELECT * FROM %s.%s ORDER BY id", TEST_SCHEMA, tableName), "VALUES (1, 'a'), (2, 'b')");
            assertEquals(replaceAfter, replaceBefore + 1);
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testRewriteManifestsUsingNamedArgs()
    {
        String tableName = "rewrite_manifests_named";
        createTable(tableName);
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);
            assertUpdate(format("CALL system.rewrite_manifests(schema => '%s', table_name => '%s')", TEST_SCHEMA, tableName));
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testRewriteManifestsWithValidSpecId()
    {
        String tableName = "rewrite_manifests_spec";
        createTable(tableName);
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);
            // default tables have spec_id = 0
            assertUpdate(format("CALL system.rewrite_manifests('%s', '%s', 0)", TEST_SCHEMA, tableName));
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testRewriteManifestsWithInvalidSpecIdFails()
    {
        String tableName = "rewrite_manifests_invalid_spec";
        createTable(tableName);
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);
            assertQueryFails(format("CALL system.rewrite_manifests('%s', '%s', 999)", TEST_SCHEMA, tableName), "Given spec id does not exist: 999");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testRewriteManifestsIsIdempotent()
    {
        String tableName = "rewrite_manifests_idempotent";
        createTable(tableName);
        try {
            assertUpdate(format("INSERT INTO %s.%s VALUES (1, 'a')", TEST_SCHEMA, tableName), 1);
            assertUpdate(format("CALL system.rewrite_manifests('%s', '%s')", TEST_SCHEMA, tableName));
            assertUpdate(format("CALL system.rewrite_manifests('%s', '%s')", TEST_SCHEMA, tableName));
            assertQuery(format("SELECT * FROM %s.%s", TEST_SCHEMA, tableName), "VALUES (1, 'a')");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testInvalidRewriteManifestsCalls()
    {
        assertQueryFails("CALL system.rewrite_manifests('test_table', 1)", "line 1:45: Cannot cast type integer to varchar");
        assertQueryFails("CALL system.rewrite_manifests(table_name => 'test_table', spec_id=> 1)", "line 1:1: Required procedure argument 'schema' is missing");
        assertQueryFails("CALL system.rewrite_manifests(schema => 'tpch', table_name => 'test', 1)", "line 1:1: Named and positional arguments cannot be mixed");
        assertQueryFails("CALL custom.rewrite_manifests('tpch', 'test')", "Procedure not registered: custom.rewrite_manifests");
    }

    @Test
    public void testRewriteManifestsOnNonExistingTableFails()
    {
        assertQueryFails("CALL system.rewrite_manifests('tpch', 'non_existing_table')", "Table does not exist: tpch.non_existing_table");
    }
}
