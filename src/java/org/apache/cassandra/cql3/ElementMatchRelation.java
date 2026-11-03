/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.cql3;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

import org.apache.cassandra.cql3.restrictions.ElementMatchRestriction;
import org.apache.cassandra.cql3.restrictions.Restriction;
import org.apache.cassandra.cql3.restrictions.SingleColumnRestriction;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.db.filter.IndexHints;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

/**
 * A {@code CONTAINS ELEMENT} group: {@code addrs CONTAINS ELEMENT (city = 'Rome' AND zip = 100)} selects the rows
 * where some element of the collection {@code addrs} satisfies all the conditions of the group, and
 * {@code addrs NOT CONTAINS ELEMENT (...)} those where no element does (including the rows without elements).
 * <p>
 * The collection is a column, or a field of a UDT or tuple column ({@code p.home.names CONTAINS ELEMENT (...)}), of
 * type list, set or map (its values) of UDTs or tuples, frozen or not. The conditions are a WHERE expression (AND,
 * OR, parentheses) of relations whose names are paths relative to the elements, at any depth ({@code loc.lat < 2}),
 * with the operators of the relations on fields (see {@link SingleColumnRelation}): they're the relations on the
 * absolute paths ({@code addrs.loc.lat < 2}, see {@link SingleColumnRelation#withColumnPrefix}), evaluated on each
 * element (see {@link ElementMatchRestriction}). Unlike the AND of relations outside a group, which are independent
 * (each may be satisfied by a different element), all the conditions of a group are on the same element.
 */
public final class ElementMatchRelation extends Relation
{
    public static final String NOT_A_COLLECTION_OF_UDTS_MESSAGE =
    "CONTAINS ELEMENT requires a collection of user-defined types or tuples, but %s is of type %s";

    public static final String UNSUPPORTED_RELATION_MESSAGE =
    "Only relations on the fields of the elements are supported in CONTAINS ELEMENT groups: %s";

    private final ColumnIdentifier entity;
    /** The names of the path from the column to the collection, {@code null} if the collection is the column. */
    @Nullable
    private final List<FieldIdentifier> fieldNames;
    private final boolean negated;
    private final WhereClause clause;

    private ElementMatchRelation(ColumnIdentifier entity, @Nullable List<FieldIdentifier> fieldNames, boolean negated, WhereClause clause)
    {
        this.entity = entity;
        this.fieldNames = fieldNames == null || fieldNames.isEmpty() ? null : List.copyOf(fieldNames);
        this.negated = negated;
        this.clause = clause;
        this.relationType = negated ? Operator.NOT_CONTAINS : Operator.CONTAINS;
    }

    /**
     * @param entity the column of the collection
     * @param fieldNames the names of the path from the column to the collection, {@code null} or empty if the
     * collection is the column
     * @param negated {@code true} for {@code NOT CONTAINS ELEMENT}
     * @param clause the conditions on each element, with names relative to the elements
     */
    public static ElementMatchRelation create(ColumnIdentifier entity, @Nullable List<FieldIdentifier> fieldNames, boolean negated, WhereClause clause)
    {
        return new ElementMatchRelation(entity, fieldNames, negated, clause);
    }

    public ColumnIdentifier getEntity()
    {
        return entity;
    }

    public boolean isNegated()
    {
        return negated;
    }

    public WhereClause getClause()
    {
        return clause;
    }

    @Override
    public Term.Raw getValue()
    {
        return null;
    }

    @Override
    public List<? extends Term.Raw> getInValues()
    {
        return null;
    }

    /**
     * @return the collection in CQL form: the column, followed by the path to the collection if it's a field
     */
    private String collectionAsString()
    {
        return fieldNames == null ? entity.toCQLString() : entity.toCQLString() + '.' + FieldPath.toCQLString(fieldNames);
    }

    @Override
    public Restriction toRestriction(TableMetadata table, VariableSpecifications boundNames, IndexHints indexHints)
    {
        ColumnMetadata column = table.getExistingColumn(entity);

        FieldPath collectionPath = null;
        if (fieldNames != null)
        {
            try
            {
                collectionPath = FieldPath.resolve(column.type, fieldNames);
            }
            catch (InvalidRequestException e)
            {
                throw invalidRequest("Invalid restriction on %s: %s", collectionAsString(), e.getMessage());
            }
        }

        AbstractType<?> collectionType = (collectionPath == null ? column.type : collectionPath.leafType(column.type)).unwrap();
        checkTrue(collectionType.isCollection() && isTupleOrUDT(FieldPath.Step.ELEMENTS.apply(collectionType).unwrap()),
                  NOT_A_COLLECTION_OF_UDTS_MESSAGE, collectionAsString(), collectionType.asCQL3Type());

        FieldPath prefix = FieldPath.elementsOf(collectionPath);
        List<FieldIdentifier> collectionNames = fieldNames == null ? Collections.emptyList() : fieldNames;
        ElementMatchRestriction.Group root = group(table, prefix, collectionNames, clause.root().conjunctiveForm(), boundNames, indexHints);

        // a NOT CONTAINS ELEMENT group with a single equality is the != on its field: no element is equal
        SingleColumnRestriction reduced = null;
        if (negated)
        {
            ElementMatchRestriction.Leaf leaf = root.singleLeaf();
            SingleColumnRelation relation = leaf == null ? null : singleRelation(clause.root());
            if (relation != null && relation.operator() == Operator.EQ)
                reduced = (SingleColumnRestriction) relation.withColumnPrefix(entity, collectionNames)
                                                            .withOperator(Operator.NEQ)
                                                            .toRestriction(table, boundNames, indexHints);
        }

        return new ElementMatchRestriction(column, prefix, negated, root, reduced, toCQLString());
    }

    /**
     * @return the single relation of the given expression, {@code null} if it doesn't have exactly one relation
     */
    @Nullable
    private static SingleColumnRelation singleRelation(WhereClause.ExpressionElement element)
    {
        List<Relation> relations = element.relations();
        List<WhereClause.ContainerElement> operations = element.operations();
        if (relations.size() == 1 && operations.isEmpty() && relations.get(0) instanceof SingleColumnRelation)
            return (SingleColumnRelation) relations.get(0);
        if (relations.isEmpty() && operations.size() == 1)
            return singleRelation(operations.get(0));
        return null;
    }

    private static boolean isTupleOrUDT(AbstractType<?> type)
    {
        return type.isTuple() || type.isUDT();
    }

    private ElementMatchRestriction.Group group(TableMetadata table,
                                                FieldPath prefix,
                                                List<FieldIdentifier> collectionNames,
                                                WhereClause.ExpressionElement element,
                                                VariableSpecifications boundNames,
                                                IndexHints indexHints)
    {
        if (!element.expressions().isEmpty())
            throw invalidRequest(UNSUPPORTED_RELATION_MESSAGE, element.expressions().get(0));

        List<ElementMatchRestriction.Leaf> leaves = new ArrayList<>();
        for (Relation relation : element.relations())
            leaves.add(leaf(table, prefix, collectionNames, relation, boundNames, indexHints));

        List<ElementMatchRestriction.Group> children = new ArrayList<>();
        for (WhereClause.ContainerElement child : element.operations())
            children.add(group(table, prefix, collectionNames, child, boundNames, indexHints));

        return new ElementMatchRestriction.Group(element.isDisjunction(), leaves, children);
    }

    private ElementMatchRestriction.Leaf leaf(TableMetadata table,
                                              FieldPath prefix,
                                              List<FieldIdentifier> collectionNames,
                                              Relation relation,
                                              VariableSpecifications boundNames,
                                              IndexHints indexHints)
    {
        if (relation instanceof ElementMatchRelation)
            throw invalidRequest("Nested CONTAINS ELEMENT groups are not supported: %s", relation);
        if (!(relation instanceof SingleColumnRelation) || ((SingleColumnRelation) relation).getMapKey() != null)
            throw invalidRequest(UNSUPPORTED_RELATION_MESSAGE, relation);

        SingleColumnRelation absolute = ((SingleColumnRelation) relation).withColumnPrefix(entity, collectionNames);
        Operator operator = relation.operator();
        if (operator == Operator.IS_NOT)
            throw invalidRequest("IS NOT NULL restrictions on fields are not yet supported: %s", absolute);

        Restriction restriction = absolute.toRestriction(table, boundNames, indexHints);
        assert restriction instanceof SingleColumnRestriction
               && ((SingleColumnRestriction) restriction).fieldPath() != null
               && ((SingleColumnRestriction) restriction).fieldPath().isStrictlyUnder(prefix) : absolute;

        // as outside the groups, LIKE and ':' need an index
        if ((relation.isLIKE() || operator == Operator.ANALYZER_MATCHES)
            && !restriction.hasSupportingIndex(IndexRegistry.obtain(table), indexHints))
            throw invalidRequest(StatementRestrictions.RESTRICTION_REQUIRES_INDEX_MESSAGE, operator, absolute);

        return new ElementMatchRestriction.Leaf((SingleColumnRestriction) restriction, isPositive(relation));
    }

    /**
     * @return whether the relation selects the rows where some element satisfies it, a superset of the rows where
     * an element satisfies the group; the negative relations ({@code !=}, {@code NOT IN}, {@code NOT CONTAINS})
     * select nothing on their own
     */
    private static boolean isPositive(Relation relation)
    {
        switch (relation.operator())
        {
            case EQ:
            case IN:
            case LT:
            case LTE:
            case GT:
            case GTE:
            case CONTAINS:
            case ANALYZER_MATCHES:
                return true;
            default:
                return relation.isLIKE();
        }
    }

    @Override
    protected Restriction newEQRestriction(TableMetadata table, VariableSpecifications boundNames, IndexHints indexHints)
    {
        throw new AssertionError();
    }

    @Override
    protected Restriction newNEQRestriction(TableMetadata table, VariableSpecifications boundNames)
    {
        throw new AssertionError();
    }

    @Override
    protected Restriction newINRestriction(TableMetadata table, VariableSpecifications boundNames)
    {
        throw new AssertionError();
    }

    @Override
    protected Restriction newNotINRestriction(TableMetadata table, VariableSpecifications boundNames)
    {
        throw new AssertionError();
    }

    @Override
    protected Restriction newSliceRestriction(TableMetadata table, VariableSpecifications boundNames, Bound bound, boolean inclusive)
    {
        throw new AssertionError();
    }

    @Override
    protected Restriction newContainsRestriction(TableMetadata table, VariableSpecifications boundNames, boolean isKey)
    {
        throw new AssertionError("Built by toRestriction");
    }

    @Override
    protected Restriction newNotContainsRestriction(TableMetadata table, VariableSpecifications boundNames, boolean isKey)
    {
        throw new AssertionError("Built by toRestriction");
    }

    @Override
    protected Restriction newIsNotRestriction(TableMetadata table, VariableSpecifications boundNames)
    {
        throw new AssertionError();
    }

    @Override
    protected Restriction newLikeRestriction(TableMetadata table, VariableSpecifications boundNames, Operator operator)
    {
        throw new AssertionError();
    }

    @Override
    protected Restriction newAnnRestriction(TableMetadata table, VariableSpecifications boundNames)
    {
        throw new AssertionError();
    }

    @Override
    protected Restriction newBm25Restriction(TableMetadata table, VariableSpecifications boundNames)
    {
        throw new AssertionError();
    }

    @Override
    protected Restriction newAnalyzerMatchesRestriction(TableMetadata table, VariableSpecifications boundNames)
    {
        throw new AssertionError();
    }

    @Override
    protected Term toTerm(List<? extends ColumnSpecification> receivers, Term.Raw raw, String keyspace, VariableSpecifications boundNames)
    {
        throw new AssertionError();
    }

    @Override
    public Relation renameIdentifier(ColumnIdentifier from, ColumnIdentifier to)
    {
        // the names of the conditions are relative to the elements, not columns
        return entity.equals(from) ? new ElementMatchRelation(to, fieldNames, negated, clause) : this;
    }

    @Override
    public String toCQLString()
    {
        return String.format("%s %s (%s)", collectionAsString(), negated ? "NOT CONTAINS ELEMENT" : "CONTAINS ELEMENT", clause.toCQLString());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(entity, fieldNames, negated, clause.toCQLString());
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (!(o instanceof ElementMatchRelation))
            return false;
        ElementMatchRelation other = (ElementMatchRelation) o;
        return entity.equals(other.entity)
               && Objects.equals(fieldNames, other.fieldNames)
               && negated == other.negated
               && clause.equals(other.clause);
    }
}
