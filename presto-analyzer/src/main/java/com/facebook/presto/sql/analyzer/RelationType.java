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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Immutable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.UnaryOperator;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkElementIndex;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * TODO: this needs to be merged with RowType at some point (when the type system is unified)
 */
@Immutable
public class RelationType
{
    private final List<Field> visibleFields;
    private final List<Field> allFields;

    private final Map<Field, Integer> fieldIndexes;

    // Index for O(1) field resolution by name.
    // Key is produced by nameKeyFunction (lowercase by default; identity for case-sensitive catalogs).
    private final Map<String, List<Field>> fieldsByName;

    private final UnaryOperator<String> nameKeyFunction;

    public RelationType(Field... fields)
    {
        this(ImmutableList.copyOf(fields));
    }

    /**
     * Backward-compatible constructor. Uses case-insensitive (toLowerCase) field resolution.
     */
    public RelationType(List<Field> fields)
    {
        this(fields, s -> s.toLowerCase(Locale.ENGLISH));
    }

    /**
     * Constructor with an explicit name-key function, used when the owning catalog has
     * specific identifier-normalization rules
     *
     * @param fields   the fields of this relation
     * @param nameKeyFunction function applied to a field name to produce the lookup key;
     *                  use {@code s -> s.toLowerCase(Locale.ENGLISH)} for case-insensitive
     *                  and {@code UnaryOperator.identity()} for case-sensitive behaviour
     */
    public RelationType(List<Field> fields, UnaryOperator<String> nameKeyFunction)
    {
        requireNonNull(fields, "fields is null");
        requireNonNull(nameKeyFunction, "nameKeyFunction is null");
        this.nameKeyFunction = nameKeyFunction;
        this.allFields = ImmutableList.copyOf(fields);
        this.visibleFields = ImmutableList.copyOf(fields.stream().filter(not(Field::isHidden)).iterator());

        int index = 0;
        ImmutableMap.Builder<Field, Integer> builder = ImmutableMap.builder();
        for (Field field : fields) {
            builder.put(field, index++);
        }
        fieldIndexes = builder.build();

        // Build name index applying nameKeyFunction so lookup keys match what resolveFields() will produce.
        HashMap<String, List<Field>> nameIndex = new HashMap<>();
        for (Field field : fields) {
            field.getName().ifPresent(name ->
                    nameIndex.computeIfAbsent(nameKeyFunction.apply(name), k -> new ArrayList<>()).add(field));
        }
        // Convert to immutable lists
        ImmutableMap.Builder<String, List<Field>> nameIndexBuilder = ImmutableMap.builder();
        nameIndex.forEach((name, fieldList) -> nameIndexBuilder.put(name, ImmutableList.copyOf(fieldList)));
        this.fieldsByName = nameIndexBuilder.build();
    }

    /**
     * Returns the name-key function used for field resolution in this relation.
     * Callers that derive new RelationTypes from this one (e.g. withAlias, joinWith) should
     * propagate this function so that consistent resolution semantics are maintained.
     */
    public UnaryOperator<String> getNameKeyFunction()
    {
        return nameKeyFunction;
    }

    /**
     * Gets the index of the specified field.
     *
     * @throws IllegalArgumentException when field is not found
     */
    public int indexOf(Field field)
    {
        requireNonNull(field, "field cannot be null");
        Integer index = fieldIndexes.get(field);
        checkArgument(index != null, "Field %s not found", field);
        return index;
    }

    /**
     * Gets the field at the specified index.
     */
    public Field getFieldByIndex(int fieldIndex)
    {
        checkElementIndex(fieldIndex, allFields.size(), "fieldIndex");
        return allFields.get(fieldIndex);
    }

    /**
     * Gets only the visible fields.
     * No assumptions should be made about the order of the fields returned from this method.
     * To obtain the index of a field, call indexOf.
     */
    public Collection<Field> getVisibleFields()
    {
        return visibleFields;
    }

    public int getVisibleFieldCount()
    {
        return visibleFields.size();
    }

    /**
     * Gets all fields including hidden fields.
     * No assumptions should be made about the order of the fields returned from this method.
     * To obtain the index of a field, call indexOf.
     */
    public Collection<Field> getAllFields()
    {
        return ImmutableSet.copyOf(allFields);
    }

    /**
     * Gets the count of all fields including hidden fields.
     */
    public int getAllFieldCount()
    {
        return allFields.size();
    }

    /**
     * This method is used for SELECT * or x.* queries
     */
    public List<Field> resolveFieldsWithPrefix(Optional<QualifiedName> prefix)
    {
        return visibleFields.stream()
                .filter(input -> input.matchesPrefix(prefix))
                .collect(toImmutableList());
    }

    /**
     * Gets the index of all columns matching the specified name.
     * The lookup key is produced by getNameKeyFunction, matching the same transformation applied at
     * index-build time — so case-sensitive catalogs require exact-case matches.
     */
    public List<Field> resolveFields(QualifiedName name)
    {
        List<Field> candidates = fieldsByName.getOrDefault(nameKeyFunction.apply(name.getOriginalSuffix().getValue()), ImmutableList.of());
        if (candidates.isEmpty()) {
            return ImmutableList.of();
        }
        return candidates.stream()
                .filter(field -> field.matchesPrefix(name.getPrefix()))
                .collect(toImmutableList());
    }

    public boolean canResolve(QualifiedName name)
    {
        return !resolveFields(name).isEmpty();
    }

    /**
     * Creates a new tuple descriptor containing all fields from this tuple descriptor
     * and all fields from the specified tuple descriptor.
     * <p>
     * For cross-catalog joins the two relations may carry different nameKeyFunction values.
     * The merged relation uses this relation's nameKeyFunction for fields coming from {@code this},
     * and the other relation's nameKeyFunction for fields coming from {@code other}, by delegating
     * field resolution back to each original relation via their stored functions.
     */
    public RelationType joinWith(RelationType other)
    {
        List<Field> fields = ImmutableList.<Field>builder()
                .addAll(this.allFields)
                .addAll(other.allFields)
                .build();

        return new RelationType(fields, this.nameKeyFunction);
    }

    /**
     * Creates a new tuple descriptor with the relation, and, optionally, the columns aliased.
     */
    public RelationType withAlias(String relationAlias, List<String> columnAliases)
    {
        if (columnAliases != null) {
            checkArgument(columnAliases.size() == visibleFields.size(),
                    "Column alias list has %s entries but '%s' has %s columns available",
                    columnAliases.size(),
                    relationAlias,
                    visibleFields.size());
        }

        ImmutableList.Builder<Field> fieldsBuilder = ImmutableList.builder();
        for (int i = 0; i < allFields.size(); i++) {
            Field field = allFields.get(i);
            Optional<String> columnAlias = field.getName();
            if (columnAliases == null) {
                fieldsBuilder.add(Field.newQualified(
                        field.getNodeLocation(),
                        QualifiedName.of(relationAlias),
                        columnAlias,
                        field.getType(),
                        field.isHidden(),
                        field.getOriginTable(),
                        field.getOriginColumnName(),
                        field.isAliased()));
            }
            else if (!field.isHidden()) {
                // hidden fields are not exposed when there are column aliases
                columnAlias = Optional.of(columnAliases.get(i));
                fieldsBuilder.add(Field.newQualified(
                        field.getNodeLocation(),
                        QualifiedName.of(relationAlias),
                        columnAlias,
                        field.getType(),
                        false,
                        field.getOriginTable(),
                        field.getOriginColumnName(),
                        field.isAliased()));
            }
        }

        return new RelationType(fieldsBuilder.build(), this.nameKeyFunction);
    }

    /**
     * Creates a new tuple descriptor containing only the visible fields.
     */
    public RelationType withOnlyVisibleFields()
    {
        return new RelationType(visibleFields, this.nameKeyFunction);
    }

    @Override
    public String toString()
    {
        return allFields.toString();
    }
}
