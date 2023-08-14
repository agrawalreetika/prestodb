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
package com.facebook.presto.hive;

import com.amazonaws.util.StringUtils;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.hive.filesystem.ExtendedFileSystem;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.hive.util.HiveFileIterator;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.expressions.EqualTo;
import io.delta.standalone.expressions.Literal;
import io.delta.standalone.types.StructType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hudi.common.fs.FSUtils;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.Optional;

import static com.facebook.presto.hive.HiveFileInfo.createHiveFileInfo;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DEFAULT_PORT;

public class DeltaDirectoryLister
        implements DirectoryLister
{
    private static final Logger log = Logger.get(DeltaDirectoryLister.class);

    private final HdfsEnvironment hdfsEnvironment;
    private final ConnectorSession session;
    private final Configuration configuration;

    public DeltaDirectoryLister(HdfsEnvironment hdfsEnvironment, Configuration conf, ConnectorSession session)
    {
        log.info("Using Delta Directory Lister.");
        this.hdfsEnvironment = hdfsEnvironment;
        this.session = session;
        this.configuration = conf;
    }

    private Optional<DeltaLog> loadDeltaTableLog(ConnectorSession session, Path tableLocation, SchemaTableName schemaTableName)
    {
        try {
            HdfsContext hdfsContext = new HdfsContext(
                    session,
                    schemaTableName.getSchemaName(),
                    schemaTableName.getTableName(),
                    tableLocation.toString(),
                    false);
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(hdfsContext, tableLocation);
            if (!fileSystem.isDirectory(tableLocation)) {
                return Optional.empty();
            }
            return Optional.of(DeltaLog.forTable(
                    hdfsEnvironment.getConfiguration(hdfsContext, tableLocation),
                    tableLocation));
        }
        catch (IOException ioException) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to load Delta table: " + ioException.getMessage(), ioException);
        }
    }

    @Override
    public Iterator<HiveFileInfo> list(
            ExtendedFileSystem fileSystem,
            Table table,
            Path path,
            Optional<Partition> partition,
            NamenodeStats namenodeStats,
            HiveDirectoryContext hiveDirectoryContext)
    {
        log.info("Listing path using Delta directory lister: %s", path.toString());
        log.info("============= Thread : " + Thread.currentThread().getId() + ", partition Before in DeltaDirectoryLister : " + partition);
        Optional<DeltaLog> deltaLog = loadDeltaTableLog(
                session,
                new Path(table.getStorage().getLocation()),
                new SchemaTableName(table.getDatabaseName(), table.getTableName()));

        Snapshot snapshot = deltaLog.get().snapshot();
        log.info("============= Thread : " + Thread.currentThread().getId() + ", path Before in DeltaDirectoryLister : " + deltaLog.get().getPath() + ", snapshot is DeltaDirectoryLister : " + snapshot.getVersion());
        return new HiveFileIterator(
                path,
                p -> new DeltaFileInfoIterator(
                        deltaLog,
                        snapshot,
                        table.getStorage().getLocation(),
                        p,
                        hdfsEnvironment,
                        configuration),
                namenodeStats,
                hiveDirectoryContext.getNestedDirectoryPolicy());
    }

    public static class DeltaFileInfoIterator
            implements RemoteIterator<HiveFileInfo>
    {
        private final Iterator<AddFile> deltaBaseFileIterator;
        HdfsEnvironment hdfsEnvironment;
        Configuration configuration;
        Path tablePath;

        public DeltaFileInfoIterator(
                Optional<DeltaLog> deltaLog,
                Snapshot snapshot,
                String tablePath,
                Path directory,
                HdfsEnvironment hdfsEnvironment,
                Configuration configuration)
        {
            String partition = FSUtils.getRelativePartitionPath(new Path(tablePath), directory); //mktsegment=FURNITURE
            log.info("============= Thread : " + Thread.currentThread().getId() + ", tablePath : " + tablePath + ", directory : " + directory + ", partition After in DeltaDirectoryLister : " + partition);

            if (StringUtils.isNullOrEmpty(partition)) {
                this.deltaBaseFileIterator = deltaLog.get()
                        .getSnapshotForVersionAsOf(snapshot.getVersion())
                        .scan()
                        .getFiles();
            }
            else {
                StructType schema = deltaLog.get().snapshot().getMetadata().getSchema();
                String partitionColumnName = partition.split("=")[0];
                String partitionColumnValue = partition.split("=")[1];
                this.deltaBaseFileIterator = deltaLog.get()
                        .getSnapshotForVersionAsOf(snapshot.getVersion())
                        .scan(new EqualTo(schema.column(partitionColumnName), Literal.of(partitionColumnValue)))
                        .getFiles();
            }

            this.hdfsEnvironment = hdfsEnvironment;
            this.configuration = configuration;
            this.tablePath = new Path(tablePath);
        }

        @Override
        public boolean hasNext()
        {
            return deltaBaseFileIterator.hasNext();
        }

        @Override
        public HiveFileInfo next()
                throws IOException
        {
            AddFile file = deltaBaseFileIterator.next();
            Path filePath = new Path(tablePath, URI.create(file.getPath()).getPath());
            log.info("============= Thread : " + Thread.currentThread().getId() + ", path in DeltaFileInfoIterator : " + filePath);
            ExtendedFileSystem fileSystem = hdfsEnvironment.getFileSystem("user", filePath, configuration);
            FileStatus fileStatus = fileSystem.getFileStatus(filePath);
            String[] name = new String[] {"localhost:" + DFS_DATANODE_DEFAULT_PORT};
            String[] host = new String[] {"localhost"};
            LocatedFileStatus deltaFileStatus = new LocatedFileStatus(fileStatus,
                    new BlockLocation[] {new BlockLocation(name, host, 0L, fileStatus.getLen())});
            return createHiveFileInfo(deltaFileStatus, Optional.empty());
        }
    }
}
