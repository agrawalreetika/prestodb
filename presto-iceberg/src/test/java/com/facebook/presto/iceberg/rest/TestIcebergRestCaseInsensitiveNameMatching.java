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
package com.facebook.presto.iceberg.rest;

import com.facebook.presto.iceberg.IcebergConfig;
import com.facebook.presto.iceberg.IcebergNativeCatalogFactory;
import com.facebook.presto.iceberg.IcebergQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.iceberg.CatalogType.REST;
import static com.facebook.presto.iceberg.FileFormat.ORC;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.restConnectorProperties;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Verifies the default (case-insensitive) behaviour of the Iceberg REST connector, i.e.
 * {@code case-sensitive-name-matching} is NOT set (defaults to {@code false}).
 *
 * <p>In this mode {@code normalizeIdentifier} lowercases every identifier it receives, so:
 * <ul>
 *   <li>Mixed-case table/column names are stored and retrieved in lowercase.</li>
 *   <li>Double-quoted identifiers with mixed case are also lowercased — quoting only helps
 *       with special characters or reserved keywords, not with preserving case.</li>
 * </ul>
 */
@Test(singleThreaded = true)
public class TestIcebergRestCaseInsensitiveNameMatching
        extends IcebergRestNameMatchingTestBase
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        // No case-sensitive-name-matching property — defaults to false (case-insensitive).
        return IcebergQueryRunner.builder()
                .setCatalogType(REST)
                .setFormat(ORC)
                .setExtraConnectorProperties(ImmutableMap.copyOf(restConnectorProperties(serverUri)))
                .setDataDirectory(Optional.of(warehouseLocation.toPath()))
                .setCreateTpchTables(false)
                .build()
                .getQueryRunner();
    }

    @Override
    protected IcebergNativeCatalogFactory getCatalogFactory()
    {
        IcebergConfig icebergConfig = new IcebergConfig()
                .setCatalogType(REST)
                .setCatalogWarehouse(warehouseLocation.getAbsolutePath());
        return buildCatalogFactory(icebergConfig);
    }

    @Test
    public void testCreateTableMixedCase()
    {
        // normalizeIdentifier lowercases 'MixedCaseTable' → 'mixedcasetable'.
        // All case variants resolve to the same stored name.
        assertQuerySucceeds(testSession(), "CREATE TABLE MixedCaseTable (id bigint, val varchar)");

        assertQuerySucceeds(testSession(), "SELECT * FROM MixedCaseTable");
        assertQuerySucceeds(testSession(), "SELECT * FROM mixedcasetable");
        assertQuerySucceeds(testSession(), "SELECT * FROM MIXEDCASETABLE");

        // Creating with a different case input refers to the same table — already exists.
        assertQueryFails(testSession(), "CREATE TABLE mixedcasetable (id bigint, val varchar)", ".*Table.*already exists.*");
        assertQuerySucceeds(testSession(), "DROP TABLE MixedCaseTable");
    }

    @Test
    public void testCreateTableAsMixedCase()
    {
        // 'MyTable' is normalised to 'mytable'; all case variants resolve to the same table.
        assertQuerySucceeds(testSession(), "CREATE TABLE MyTable AS SELECT 1 AS id, 'hello' AS name");

        assertQuery(testSession(), "SELECT id, name FROM MyTable", "VALUES (1, 'hello')");
        assertQuery(testSession(), "SELECT id, name FROM mytable", "VALUES (1, 'hello')");
        assertQuery(testSession(), "SELECT id, name FROM MYTABLE", "VALUES (1, 'hello')");

        assertQuerySucceeds(testSession(), "DROP TABLE MyTable");
    }

    @Test
    public void testMixedCaseColumns()
    {
        // normalizeIdentifier lowercases every column name regardless of quoting.

        // Quoted: 'FirstName' → 'firstname'. Any case variant retrieves the same column.
        assertQuerySucceeds(testSession(), "CREATE TABLE coltestquoted (\"FirstName\" varchar, \"lastName\" varchar)");
        assertQuerySucceeds(testSession(), "INSERT INTO coltestquoted VALUES ('Alice', 'Smith')");
        assertQuery(testSession(), "SELECT firstname, lastname FROM coltestquoted", "VALUES ('Alice', 'Smith')");
        assertQuery(testSession(), "SELECT \"FirstName\", \"lastName\" FROM coltestquoted", "VALUES ('Alice', 'Smith')");
        assertQuerySucceeds(testSession(), "DROP TABLE coltestquoted");

        // Unquoted: 'FirstName' → 'firstname'.
        assertQuerySucceeds(testSession(), "CREATE TABLE coltestunquoted (FirstName varchar, lastName varchar)");
        assertQuerySucceeds(testSession(), "INSERT INTO coltestunquoted VALUES ('Bob', 'Jones')");
        assertQuery(testSession(), "SELECT firstname, lastname FROM coltestunquoted", "VALUES ('Bob', 'Jones')");
        assertQuerySucceeds(testSession(), "DROP TABLE coltestunquoted");
    }

    @Test
    public void testMixedCasePartitionColumn()
    {
        // normalizeIdentifier lowercases the resolved column name; column definition and
        // partition array reference both resolve to the same lowercase name.

        // Quoted in array: '"RegionId"' → 'RegionId' → 'regionid'. Column also → 'regionid'. ✓
        assertQuerySucceeds(testSession(), "CREATE TABLE partTestQuoted (\"RegionId\" bigint, name varchar) WITH (partitioning = ARRAY['\"RegionId\"'])");
        assertQuerySucceeds(testSession(), "INSERT INTO partTestQuoted VALUES (1, 'north'), (2, 'south')");
        assertQuery(testSession(), "SELECT regionid, name FROM partTestQuoted ORDER BY regionid", "VALUES (1, 'north'), (2, 'south')");
        assertQuerySucceeds(testSession(), "DROP TABLE partTestQuoted");

        // Unquoted in array: 'RegionId' → 'regionid'. ✓
        assertQuerySucceeds(testSession(), "CREATE TABLE partTestUnquoted (RegionId bigint, name varchar) WITH (partitioning = ARRAY['RegionId'])");
        assertQuerySucceeds(testSession(), "INSERT INTO partTestUnquoted VALUES (1, 'north'), (2, 'south')");
        assertQuery(testSession(), "SELECT regionid, name FROM partTestUnquoted ORDER BY regionid", "VALUES (1, 'north'), (2, 'south')");
        assertQuerySucceeds(testSession(), "DROP TABLE partTestUnquoted");
    }

    @Test
    public void testShowColumnsCaseInsensitive()
    {
        // All column names are lowercased regardless of how they were written.
        assertQuerySucceeds(testSession(), "CREATE TABLE showcols (\"MyCol\" bigint, \"anotherCol\" varchar)");
        String result = getQueryRunner().execute(testSession(), "SHOW COLUMNS FROM showcols").toString();
        assertTrue(result.contains("mycol"), "Expected lowercase 'mycol' in SHOW COLUMNS output");
        assertTrue(result.contains("anothercol"), "Expected lowercase 'anothercol' in SHOW COLUMNS output");
        assertQuerySucceeds(testSession(), "DROP TABLE showcols");
    }

    @Test
    public void testNormalizeIdentifierLowercases()
    {
        String normalized = normalizeIdentifier("MyTable", ICEBERG_CATALOG);
        assertTrue(normalized.equals("mytable"), "Expected normalizeIdentifier to lowercase 'MyTable', got: " + normalized);

        String normalizedLower = normalizeIdentifier("mytable", ICEBERG_CATALOG);
        assertTrue(normalizedLower.equals("mytable"), "Expected normalizeIdentifier to return 'mytable' unchanged, got: " + normalizedLower);

        String normalizedUpper = normalizeIdentifier("MYTABLE", ICEBERG_CATALOG);
        assertTrue(normalizedUpper.equals("mytable"), "Expected normalizeIdentifier to lowercase 'MYTABLE', got: " + normalizedUpper);

        assertFalse(normalized.equals("MyTable"), "normalizeIdentifier must not preserve mixed case in case-insensitive mode");
        assertFalse(normalizedUpper.equals("MYTABLE"), "normalizeIdentifier must not leave uppercase unchanged in case-insensitive mode");
    }

    /**
     * Verifies that ANALYZE succeeds when the table has mixed-case column names in case-insensitive mode.
     * All column names are lowercased by {@code normalizeIdentifier}, so SHOW STATS must report
     * the stored lowercase names.
     */
    @Test
    public void testAnalyzeMixedCaseColumns()
    {
        assertQuerySucceeds(testSession(), "CREATE TABLE analyze_mixed_ci (Id bigint, Name varchar, VAL integer)");
        assertQuerySucceeds(testSession(), "INSERT INTO analyze_mixed_ci VALUES (1, 'alice', 10), (2, 'bob', 20), (3, 'alice', 30)");

        assertQuerySucceeds(testSession(), "ANALYZE analyze_mixed_ci");

        // All names are lowercased — stored as 'id', 'name', 'val'.
        Set<String> colNames = showStatsColumnNames("analyze_mixed_ci");
        assertTrue(colNames.contains("id"), "SHOW STATS must report stored lowercase column 'id', got: " + colNames);
        assertTrue(colNames.contains("name"), "SHOW STATS must report stored lowercase column 'name', got: " + colNames);
        assertTrue(colNames.contains("val"), "SHOW STATS must report stored lowercase column 'val', got: " + colNames);
        assertFalse(colNames.contains("Id"), "SHOW STATS must not preserve mixed-case 'Id' in case-insensitive mode, got: " + colNames);
        assertFalse(colNames.contains("Name"), "SHOW STATS must not preserve mixed-case 'Name' in case-insensitive mode, got: " + colNames);
        assertFalse(colNames.contains("VAL"), "SHOW STATS must not preserve mixed-case 'VAL' in case-insensitive mode, got: " + colNames);

        assertQuerySucceeds(testSession(), "DROP TABLE analyze_mixed_ci");
    }

    /**
     * Verifies that ANALYZE and SHOW STATS work correctly with all-lowercase column names in
     * case-insensitive mode — acts as a regression guard that the fix does not break the normal path.
     */
    @Test
    public void testAnalyzeLowerCaseColumns()
    {
        assertQuerySucceeds(testSession(), "CREATE TABLE analyze_lower_ci (id bigint, name varchar, val integer)");
        assertQuerySucceeds(testSession(), "INSERT INTO analyze_lower_ci VALUES (1, 'alice', 10), (2, 'bob', 20), (3, 'alice', 30)");

        assertQuerySucceeds(testSession(), "ANALYZE analyze_lower_ci");

        Set<String> colNames = showStatsColumnNames("analyze_lower_ci");
        assertTrue(colNames.contains("id"), "SHOW STATS must report column 'id', got: " + colNames);
        assertTrue(colNames.contains("name"), "SHOW STATS must report column 'name', got: " + colNames);
        assertTrue(colNames.contains("val"), "SHOW STATS must report column 'val', got: " + colNames);

        assertQuerySucceeds(testSession(), "DROP TABLE analyze_lower_ci");
    }

    /**
     * Verifies that SHOW STATS FOR (subquery) with a partition column filter works in
     * case-insensitive mode. The column is stored as 'regionid' (lowercased), so any
     * case variant of the identifier resolves to the same partition column, ensuring
     * the predicate is pushed down and no FilterNode remains.
     */
    @Test
    public void testShowStatsForFilteredMixedCaseColumns()
    {
        // Column and partition stored as 'regionid' (normalizeIdentifier lowercases).
        assertQuerySucceeds(testSession(), "CREATE TABLE stats_filter_ci (RegionId bigint, Name varchar) WITH (partitioning = ARRAY['RegionId'])");
        assertQuerySucceeds(testSession(), "INSERT INTO stats_filter_ci VALUES (1, 'north'), (2, 'south'), (1, 'east')");
        assertQuerySucceeds(testSession(), "ANALYZE stats_filter_ci");

        // Lowercase filter — matches stored 'regionid' partition column → pushed down → succeeds.
        assertQuerySucceeds(testSession(), "SHOW STATS FOR (SELECT * FROM stats_filter_ci WHERE regionid = 1)");
        // Mixed-case filter — also normalised to 'regionid' → same partition column → succeeds.
        assertQuerySucceeds(testSession(), "SHOW STATS FOR (SELECT * FROM stats_filter_ci WHERE RegionId = 1)");

        assertQuerySucceeds(testSession(), "DROP TABLE stats_filter_ci");
    }

    @Test
    public void testRewriteDataFilesWithSortedByMixedCaseColumn()
    {
        assertQuerySucceeds(testSession(), "CREATE TABLE rewrite_sorted_ci (Id bigint, Name varchar, VAL integer)");
        assertQuerySucceeds(testSession(), "INSERT INTO rewrite_sorted_ci VALUES (3, 'c', 30), (1, 'a', 10)");
        assertQuerySucceeds(testSession(), "INSERT INTO rewrite_sorted_ci VALUES (2, 'b', 20), (4, 'd', 40)");

        // sorted_by 'Id' → normalizeIdentifier → 'id' → matches stored column 'id'
        assertQuerySucceeds(testSession(), format(
                "CALL iceberg.system.rewrite_data_files(schema => '%s', table_name => 'rewrite_sorted_ci', " +
                        "sorted_by => ARRAY['Id'])",
                SCHEMA));

        // Columns are stored and retrieved as lowercase
        assertQuery(testSession(), "SELECT id, name, val FROM rewrite_sorted_ci ORDER BY id", "VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30), (4, 'd', 40)");

        List<Types.NestedField> columns = getIcebergTable(SCHEMA, "rewrite_sorted_ci").schema().columns();
        assertEquals(columns.get(0).name(), "id", "Column 0 must be stored as lowercase 'id'");
        assertEquals(columns.get(1).name(), "name", "Column 1 must be stored as lowercase 'name'");
        assertEquals(columns.get(2).name(), "val", "Column 2 must be stored as lowercase 'val'");
        assertFalse(columns.get(0).name().equals("Id"), "Column 0 must NOT be stored as mixed-case 'Id'");
        assertFalse(columns.get(1).name().equals("Name"), "Column 1 must NOT be stored as mixed-case 'Name'");
        assertFalse(columns.get(2).name().equals("VAL"), "Column 2 must NOT be stored as mixed-case 'VAL'");

        assertQuerySucceeds(testSession(), "DROP TABLE rewrite_sorted_ci");
    }

    private Set<String> showStatsColumnNames(String tableName)
    {
        MaterializedResult result = computeActual(testSession(), "SHOW STATS FOR " + tableName);
        return result.getMaterializedRows().stream()
                .map(row -> (String) row.getField(0))
                .filter(name -> name != null)
                .collect(Collectors.toSet());
    }
}
