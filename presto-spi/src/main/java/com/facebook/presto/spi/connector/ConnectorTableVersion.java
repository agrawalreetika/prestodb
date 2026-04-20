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
package com.facebook.presto.spi.connector;

import com.facebook.presto.common.type.Type;

import static java.util.Objects.requireNonNull;

public class ConnectorTableVersion
{
    public enum VersionType
    {
        TIMESTAMP,
        VERSION
    }
    public enum VersionOperator
    {
        EQUAL,
        LESS_THAN,
        BETWEEN,
        FROM_TO
    }
    private final VersionType versionType;
    private final VersionOperator versionOperator;
    private final Type versionExpressionType;
    private final Object tableVersion;
    private final Object endTableVersion;

    public ConnectorTableVersion(VersionType versionType, VersionOperator versionOperator, Type versionExpressionType, Object tableVersion)
    {
        this(versionType, versionOperator, versionExpressionType, tableVersion, null);
    }

    public ConnectorTableVersion(VersionType versionType, VersionOperator versionOperator, Type versionExpressionType, Object tableVersion, Object endTableVersion)
    {
        requireNonNull(versionType, "versionType is null");
        requireNonNull(versionOperator, "versionOperator is null");
        requireNonNull(versionExpressionType, "versionExpressionType is null");
        requireNonNull(tableVersion, "tableVersion is null");
        if ((versionOperator == VersionOperator.BETWEEN || versionOperator == VersionOperator.FROM_TO) && endTableVersion == null) {
            throw new IllegalArgumentException("endTableVersion is required for BETWEEN and FROM_TO operators");
        }
        this.versionType = versionType;
        this.versionOperator = versionOperator;
        this.versionExpressionType = versionExpressionType;
        this.tableVersion = tableVersion;
        this.endTableVersion = endTableVersion;
    }

    public VersionType getVersionType()
    {
        return versionType;
    }

    public VersionOperator getVersionOperator()
    {
        return versionOperator;
    }

    public Type getVersionExpressionType()
    {
        return versionExpressionType;
    }

    public Object getTableVersion()
    {
        return tableVersion;
    }

    public Object getEndTableVersion()
    {
        return endTableVersion;
    }

    public boolean isRangeQuery()
    {
        return versionOperator == VersionOperator.BETWEEN || versionOperator == VersionOperator.FROM_TO;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("ConnectorTableVersion{")
                .append("tableVersionType=").append(versionType)
                .append(", versionOperator=").append(versionOperator)
                .append(", versionExpressionType=").append(versionExpressionType)
                .append(", tableVersion=").append(tableVersion);
        if (endTableVersion != null) {
            sb.append(", endTableVersion=").append(endTableVersion);
        }
        return sb.append('}').toString();
    }
}
