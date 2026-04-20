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
package com.facebook.presto.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class TableVersionExpression
        extends Expression
{
    public enum TableVersionType
    {
        TIMESTAMP,
        VERSION
    }

    public enum TableVersionOperator
    {
        EQUAL,
        LESS_THAN,
        BETWEEN,
        FROM_TO
    }
    private final Expression stateExpression;
    private final Expression endStateExpression;  // For range queries (BETWEEN/FROM_TO)
    private final TableVersionType type;
    private final TableVersionOperator operator;

    public TableVersionExpression(TableVersionType type, TableVersionOperator operator, Expression value)
    {
        this(Optional.empty(), type, operator, value, null);
    }

    public TableVersionExpression(NodeLocation location, TableVersionType type, TableVersionOperator operator, Expression value)
    {
        this(Optional.of(location), type, operator, value, null);
    }

    public TableVersionExpression(NodeLocation location, TableVersionType type, TableVersionOperator operator, Expression startValue, Expression endValue)
    {
        this(Optional.of(location), type, operator, startValue, endValue);
    }

    private TableVersionExpression(Optional<NodeLocation> location, TableVersionType type, TableVersionOperator operator, Expression value, Expression endValue)
    {
        super(location);
        requireNonNull(value, "value is null");
        requireNonNull(operator, "operator is null");
        requireNonNull(type, "type is null");
        if ((operator == TableVersionOperator.BETWEEN || operator == TableVersionOperator.FROM_TO) && endValue == null) {
            throw new IllegalArgumentException("endValue is required for BETWEEN and FROM_TO operators");
        }

        this.stateExpression = value;
        this.endStateExpression = endValue;
        this.operator = operator;
        this.type = type;
    }

    public static TableVersionExpression timestampExpression(NodeLocation location, TableVersionOperator operator, Expression value)
    {
        return new TableVersionExpression(Optional.of(location), TableVersionType.TIMESTAMP, operator, value, null);
    }

    public static TableVersionExpression versionExpression(NodeLocation location, TableVersionOperator operator, Expression value)
    {
        return new TableVersionExpression(Optional.of(location), TableVersionType.VERSION, operator, value, null);
    }

    public static TableVersionExpression timestampExpression(TableVersionOperator operator, Expression value)
    {
        return new TableVersionExpression(Optional.empty(), TableVersionType.TIMESTAMP, operator, value, null);
    }

    public static TableVersionExpression versionExpression(TableVersionOperator operator, Expression value)
    {
        return new TableVersionExpression(Optional.empty(), TableVersionType.VERSION, operator, value, null);
    }

    public static TableVersionExpression timestampRangeExpression(NodeLocation location, Expression startValue, Expression endValue)
    {
        return new TableVersionExpression(location, TableVersionType.TIMESTAMP, TableVersionOperator.BETWEEN, startValue, endValue);
    }

    public static TableVersionExpression versionRangeExpression(NodeLocation location, Expression startValue, Expression endValue)
    {
        return new TableVersionExpression(location, TableVersionType.VERSION, TableVersionOperator.BETWEEN, startValue, endValue);
    }

    public Expression getStateExpression()
    {
        return stateExpression;
    }

    public Optional<Expression> getEndStateExpression()
    {
        return Optional.ofNullable(endStateExpression);
    }

    public TableVersionType getTableVersionType()
    {
        return type;
    }
    public TableVersionOperator getTableVersionOperator()
    {
        return operator;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitTableVersion(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of(stateExpression);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TableVersionExpression that = (TableVersionExpression) o;
        return Objects.equals(stateExpression, that.stateExpression) &&
                (type == that.type) && (operator == that.operator);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(stateExpression, type, operator);
    }
}
