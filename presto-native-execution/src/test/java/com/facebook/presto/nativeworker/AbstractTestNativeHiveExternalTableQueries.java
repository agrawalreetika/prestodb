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
package com.facebook.presto.nativeworker;

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public abstract class AbstractTestNativeHiveExternalTableQueries
        extends AbstractTestQueryFramework
{
    @Test
    public void testHiveSymlinkTableRead()
    {
        QueryRunner nativeHiveQueryRunner = getQueryRunner();
        String tableQuery = "SELECT * FROM hive.default.hive_symlink_table";
        nativeHiveQueryRunner.execute(tableQuery);

        MaterializedResult result = computeActual(tableQuery);

        assertEquals(result.getRowCount(), 1);
        long col = (long) result.getMaterializedRows().get(0).getField(0);
        assertEquals(col, 42);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        assertUpdate("DROP TABLE IF EXISTS hive.default.hive_symlink_table");
        assertUpdate("DROP SCHEMA IF EXISTS hive.default");
    }
}
