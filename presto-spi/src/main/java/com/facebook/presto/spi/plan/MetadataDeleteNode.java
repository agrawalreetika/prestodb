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
package com.facebook.presto.spi.plan;

import com.facebook.presto.spi.SourceLocation;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Immutable
public class MetadataDeleteNode
        extends PlanNode
{
    private final TableHandle tableHandle;
    private final VariableReferenceExpression output;

    @JsonCreator
    public MetadataDeleteNode(
            Optional<SourceLocation> sourceLocation,
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("target") TableHandle tableHandle,
            @JsonProperty("output") VariableReferenceExpression output)
    {
        this(sourceLocation, id, Optional.empty(), tableHandle, output);
    }

    public MetadataDeleteNode(
            Optional<SourceLocation> sourceLocation,
            PlanNodeId id,
            Optional<PlanNode> statsEquivalentPlanNode,
            TableHandle tableHandle,
            VariableReferenceExpression output)
    {
        super(sourceLocation, id, statsEquivalentPlanNode);

        this.tableHandle = requireNonNull(tableHandle, "target is null");
        this.output = requireNonNull(output, "output is null");
    }

    @JsonProperty
    public TableHandle getTableHandle()
    {
        return tableHandle;
    }

    @JsonProperty
    public VariableReferenceExpression getOutput()
    {
        return output;
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return Collections.singletonList(output);
    }

    @Override
    public List<PlanNode> getSources()
    {
        return Collections.emptyList();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitMetadataDelete(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new MetadataDeleteNode(getSourceLocation(), getId(), getStatsEquivalentPlanNode(), tableHandle, output);
    }

    @Override
    public PlanNode assignStatsEquivalentPlanNode(Optional<PlanNode> statsEquivalentPlanNode)
    {
        return new MetadataDeleteNode(getSourceLocation(), getId(), statsEquivalentPlanNode, tableHandle, output);
    }
}
