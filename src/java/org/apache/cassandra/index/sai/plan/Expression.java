/*
 * All changes to the original code are Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

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

package org.apache.cassandra.index.sai.plan;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.utils.GeoUtil;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.lucene.util.SloppyMath;

public class Expression
{
    private static final Logger logger = LoggerFactory.getLogger(Expression.class);

    public enum Op
    {
        EQ, MATCH, PREFIX, NOT_EQ, RANGE,
        CONTAINS_KEY, CONTAINS_VALUE,
        NOT_CONTAINS_VALUE, NOT_CONTAINS_KEY,
        IN, ORDER_BY, BOUNDED_ANN;

        public static Op valueOf(Operator operator)
        {
            switch (operator)
            {
                case EQ:
                    return EQ;

                case NEQ:
                    return NOT_EQ;

                case CONTAINS:
                    return CONTAINS_VALUE; // non-frozen map: value contains term;

                case CONTAINS_KEY:
                    return CONTAINS_KEY; // non-frozen map: value contains key term;

                case NOT_CONTAINS:
                    return NOT_CONTAINS_VALUE;

                case NOT_CONTAINS_KEY:
                    return NOT_CONTAINS_KEY;

                case LT:
                case GT:
                case LTE:
                case GTE:
                    return RANGE;

                case LIKE_PREFIX:
                    return PREFIX;

                case LIKE_MATCHES:
                case ANALYZER_MATCHES:
                    return MATCH;

                case IN:
                    return IN;

                case ANN:
                case BM25:
                case ORDER_BY_ASC:
                case ORDER_BY_DESC:
                    return ORDER_BY;

                case BOUNDED_ANN:
                    return BOUNDED_ANN;

                default:
                    return null;
            }
        }

        public boolean isEquality()
        {
            return this == EQ || this == CONTAINS_KEY || this == CONTAINS_VALUE;
        }

        public boolean isEqualityOrRange()
        {
            return isEquality() || this == RANGE;
        }

        public boolean isNonEquality()
        {
            return this == NOT_EQ || this == NOT_CONTAINS_KEY || this == NOT_CONTAINS_VALUE;
        }

        public boolean isContains()
        {
            return this == CONTAINS_KEY
                   || this == CONTAINS_VALUE
                   || this == NOT_CONTAINS_KEY
                   || this == NOT_CONTAINS_VALUE;
        }
    }

    public final AbstractAnalyzer.AnalyzerFactory analyzerFactory;

    public final IndexContext context;
    public final AbstractType<?> validator;

    @VisibleForTesting
    protected Op operation;

    public Bound lower, upper;
    private float boundedAnnEuclideanDistanceThreshold = 0;
    private float searchRadiusMeters = 0;
    private float searchRadiusDegreesSquared = 0;
    public int topK;
    // These variables are only meant to be used for final validation of the range search. They are not
    // meant to be used when searching the index. See the 'add' method below for additional explanation.
    private boolean upperInclusive, lowerInclusive;

    final List<ByteBuffer> exclusions = new ArrayList<>();

    public Expression(IndexContext indexContext)
    {
        this.context = indexContext;
        this.analyzerFactory = indexContext.getAnalyzerFactory();
        this.validator = indexContext.getValidator();
    }

    public boolean isLiteral()
    {
        return context.isLiteral();
    }

    public Expression add(Operator op, ByteBuffer value)
    {
        boolean lowerInclusive, upperInclusive;
        // If the type supports rounding then we need to make sure that index
        // range search is always inclusive, otherwise we run the risk of
        // missing values that are within the exclusive range but are rejected
        // because their rounded value is the same as the value being queried.
        lowerInclusive = upperInclusive = TypeUtil.supportsRounding(validator);
        switch (op)
        {
            case LIKE_PREFIX:
            case LIKE_MATCHES:
            case ANALYZER_MATCHES:
            case EQ:
            case CONTAINS:
            case CONTAINS_KEY:
            case NOT_CONTAINS:
            case NOT_CONTAINS_KEY:
                lower = new Bound(value, validator, true);
                upper = lower;
                operation = Op.valueOf(op);
                break;

            case NEQ:
                // index expressions are priority sorted
                // and NOT_EQ is the lowest priority, which means that operation type
                // is always going to be set before reaching it in case of RANGE or EQ.
                if (operation == null)
                {
                    operation = Op.NOT_EQ;
                    lower = new Bound(value, validator, true);
                    upper = lower;
                }
                else
                    exclusions.add(value);
                break;

            case LTE:
                if (context.getDefinition().isReversedType())
                {
                    this.lowerInclusive = true;
                    lowerInclusive = true;
                }
                else
                {
                    this.upperInclusive = true;
                    upperInclusive = true;
                }
            case LT:
                operation = Op.RANGE;
                if (context.getDefinition().isReversedType())
                    lower = new Bound(value, validator, lowerInclusive);
                else
                    upper = new Bound(value, validator, upperInclusive);
                break;

            case GTE:
                if (context.getDefinition().isReversedType())
                {
                    this.upperInclusive = true;
                    upperInclusive = true;
                }
                else
                {
                    this.lowerInclusive = true;
                    lowerInclusive = true;
                }
            case GT:
                operation = Op.RANGE;
                if (context.getDefinition().isReversedType())
                    upper = new Bound(value, validator,  upperInclusive);
                else
                    lower = new Bound(value, validator, lowerInclusive);
                break;
            case BOUNDED_ANN:
                operation = Op.BOUNDED_ANN;
                lower = new Bound(value, validator, true);
                assert upper != null;
                searchRadiusMeters = FloatType.instance.compose(upper.value.raw);
                boundedAnnEuclideanDistanceThreshold = GeoUtil.amplifiedEuclideanSimilarityThreshold(lower.value.vector, searchRadiusMeters);
                break;
            case ANN:
            case BM25:
            case ORDER_BY_ASC:
            case ORDER_BY_DESC:
                // If we alread have an operation on the column, we don't need to set the ORDER_BY op because
                // it is only used to force validation on a column, and the presence of another operation will do that.
                if (operation == null)
                    operation = Op.ORDER_BY;
                break;
            default:
                throw new UnsupportedOperationException("Unsupported operator: " + op);
        }

        assert operation != null;

        return this;
    }

    // VSTODO seems like we could optimize for CompositeType here since we know we have a key match
    public boolean isSatisfiedBy(ByteBuffer columnValue)
    {
        if (columnValue == null)
            return false;

        // ORDER_BY is not indepently verifiable, so we always return true
        if (operation == Op.ORDER_BY)
            return true;

        if (!TypeUtil.isValid(columnValue, validator))
        {
            logger.error(context.logMessage("Value is not valid for indexed column {} with {}"), context.getColumnName(), validator);
            return false;
        }

        Value value = new Value(columnValue, validator);

        if (operation == Op.BOUNDED_ANN)
        {
            double haversineDistance = SloppyMath.haversinMeters(lower.value.vector[0], lower.value.vector[1], value.vector[0], value.vector[1]);
            return upperInclusive ? haversineDistance <= searchRadiusMeters : haversineDistance < searchRadiusMeters;
        }

        if (lower != null)
        {
            // suffix check
            if (TypeUtil.isLiteral(validator))
            {
                if (!validateStringValue(value.raw, lower.value.raw))
                    return false;
            }
            else
            {
                // range or (not-)equals - (mainly) for numeric values
                int cmp = TypeUtil.comparePostFilter(lower.value, value, validator);

                // in case of (NOT_)EQ lower == upper
                if (operation == Op.EQ || operation == Op.CONTAINS_KEY || operation == Op.CONTAINS_VALUE)
                    return cmp == 0;

                if (operation == Op.NOT_EQ || operation == Op.NOT_CONTAINS_KEY || operation == Op.NOT_CONTAINS_VALUE)
                    return cmp != 0;

                if (cmp > 0 || (cmp == 0 && !lowerInclusive))
                    return false;
            }
        }

        if (upper != null && lower != upper)
        {
            // string (prefix or suffix) check
            if (TypeUtil.isLiteral(validator))
            {
                if (!validateStringValue(value.raw, upper.value.raw))
                    return false;
            }
            else
            {
                // range - mainly for numeric values
                int cmp = TypeUtil.comparePostFilter(upper.value, value, validator);
                if (cmp < 0 || (cmp == 0 && !upperInclusive))
                    return false;
            }
        }

        // as a last step let's check exclusions for the given field,
        // this covers EQ/RANGE with exclusions.
        for (ByteBuffer term : exclusions)
        {
            if (TypeUtil.isLiteral(validator) && validateStringValue(value.raw, term) ||
                TypeUtil.comparePostFilter(new Value(term, validator), value, validator) == 0)
                return false;
        }

        return true;
    }

    /**
     * Returns the lower bound of the expression as a ByteComparable with an encoding based on the version and the
     * validator.
     * @param version the version of the index
     * @return
     */
    public ByteComparable getEncodedLowerBoundByteComparable(Version version)
    {
        // Note: this value was encoded using the TypeUtil.encode method, but it wasn't
        var bound = getPartiallyEncodedLowerBound(version);
        if (bound == null)
            return null;
        // If the lower bound is inclusive, we use the LT_NEXT_COMPONENT terminator to make sure the bound is not a
        // prefix of some other key. This ensures reverse iteration works correctly too.
        var terminator = lower.inclusive ? ByteSource.LT_NEXT_COMPONENT : ByteSource.GT_NEXT_COMPONENT;
        return getBoundByteComparable(bound, version, terminator);
    }

    /**
     * Returns the upper bound of the expression as a ByteComparable with an encoding based on the version and the
     * validator.
     * @param version the version of the index
     * @return
     */
    public ByteComparable getEncodedUpperBoundByteComparable(Version version)
    {
        var bound = getPartiallyEncodedUpperBound(version);
        if (bound == null)
            return null;
        // If the upper bound is inclusive, we use the LT_NEXT_COMPONENT terminator to make sure the bound is not a
        // prefix of some other key. This ensures reverse iteration works correctly too.
        var terminator = upper.inclusive ? ByteSource.GT_NEXT_COMPONENT : ByteSource.LT_NEXT_COMPONENT;
        return getBoundByteComparable(bound, version, terminator);
    }

    // This call encodes the byte buffer into a ByteComparable object based on the version of the index, the validator,
    // and whether the expression is in memory or on disk.
    private ByteComparable getBoundByteComparable(ByteBuffer unencodedBound, Version version, int terminator)
    {
        if (TypeUtil.isComposite(validator) && version.onOrAfter(Version.DB))
            // Note that for ranges that have one unrestricted bound, we technically do not need the terminator
            // because we use the 0 or the 1 at the end of the first component as the bound. However, it works
            // with the terminator, so we use it for simplicity.
            return TypeUtil.asComparableBytes(unencodedBound, terminator, (CompositeType) validator);
        else
            return version.onDiskFormat().encodeForTrie(unencodedBound, validator);
    }

    /**
     * This is partially encoded because it uses the {@link TypeUtil#encode(ByteBuffer, AbstractType)} method on the
     * {@link ByteBuffer}, but it does not apply the validator's encoding. We do this because we apply
     * {@link TypeUtil#encode(ByteBuffer, AbstractType)} before we find the min/max on an index and this method is
     * exposed publicly for determining if a bound is within an index's min/max.
     * @param version
     * @return
     */
    public ByteBuffer getPartiallyEncodedLowerBound(Version version)
    {
        return getBound(lower, true, version);
    }

    /**
     * This is partially encoded because it uses the {@link TypeUtil#encode(ByteBuffer, AbstractType)} method on the
     * {@link ByteBuffer}, but it does not apply the validator's encoding. We do this because we apply
     * {@link TypeUtil#encode(ByteBuffer, AbstractType)} before we find the min/max on an index and this method is
     * exposed publicly for determining if a bound is within an index's min/max.
     * @param version
     * @return
     */
    public ByteBuffer getPartiallyEncodedUpperBound(Version version)
    {
        return getBound(upper, false, version);
    }

    private ByteBuffer getBound(Bound bound, boolean isLowerBound, Version version)
    {
        if (bound == null)
            return null;
        // Composite types are currently only used in maps.
        // Before DB, we need to extract the first component of the composite type to use as the trie search prefix.
        // After DB, we can use the encoded value directly because the trie is encoded in order so the range
        // correctly gets all relevant values.
        if (!version.onOrAfter(Version.DB) && validator instanceof CompositeType)
            return CompositeType.extractFirstComponentAsTrieSearchPrefix(bound.value.encoded, isLowerBound);
        return bound.value.encoded;
    }

    public boolean isSatisfiedBy(Iterator<ByteBuffer> values)
    {
        if (values == null)
            values = Collections.emptyIterator();

        boolean success = operation.isNonEquality();
        while (values.hasNext())
        {
            ByteBuffer v = values.next();
            if (isSatisfiedBy(v) ^ success)
                return !success;
        }
        return success;
    }

    private boolean validateStringValue(ByteBuffer columnValue, ByteBuffer requestedValue)
    {
        AbstractAnalyzer analyzer = analyzerFactory.create();
        analyzer.reset(columnValue);
        try
        {
            while (analyzer.hasNext())
            {
                final ByteBuffer term = analyzer.next();

                boolean isMatch = false;
                switch (operation)
                {
                    case EQ:
                    case MATCH:
                        // Operation.isSatisfiedBy handles conclusion on !=,
                        // here we just need to make sure that term matched it
                    case CONTAINS_KEY:
                    case CONTAINS_VALUE:
                        isMatch = validator.compare(term, requestedValue) == 0;
                        break;
                    case NOT_EQ:
                    case NOT_CONTAINS_KEY:
                    case NOT_CONTAINS_VALUE:
                        isMatch = validator.compare(term, requestedValue) != 0;
                        break;
                    case RANGE:
                        isMatch = isLowerSatisfiedBy(term) && isUpperSatisfiedBy(term);
                        break;

                    case PREFIX:
                        isMatch = ByteBufferUtil.startsWith(term, requestedValue);
                        break;
                }

                if (isMatch)
                    return true;
            }
            return false;
        }
        finally
        {
            analyzer.end();
        }
    }

    public Op getOp()
    {
        return operation;
    }

    private boolean hasLower()
    {
        return lower != null;
    }

    private boolean hasUpper()
    {
        return upper != null;
    }

    private boolean isLowerSatisfiedBy(ByteBuffer value)
    {
        if (!hasLower())
            return true;

        int cmp = validator.compare(value, lower.value.raw);
        return cmp > 0 || cmp == 0 && lower.inclusive;
    }

    private boolean isUpperSatisfiedBy(ByteBuffer value)
    {
        if (!hasUpper())
            return true;

        int cmp = validator.compare(value, upper.value.raw);
        return cmp < 0 || cmp == 0 && upper.inclusive;
    }

    public float getEuclideanSearchThreshold()
    {
        return boundedAnnEuclideanDistanceThreshold;
    }

    public String toString()
    {
        return String.format("Expression{name: %s, op: %s, lower: (%s, %s), upper: (%s, %s), exclusions: %s}",
                             context.getColumnName(),
                             operation,
                             lower == null ? "null" : validator.getString(lower.value.raw),
                             lower != null && lower.inclusive,
                             upper == null ? "null" : validator.getString(upper.value.raw),
                             upper != null && upper.inclusive,
                             Iterators.toString(Iterators.transform(exclusions.iterator(), validator::getString)));
    }

    public String getIndexName()
    {
        return context.getIndexName();
    }

    public int hashCode()
    {
        return new HashCodeBuilder().append(context.getColumnName())
                                    .append(operation)
                                    .append(validator)
                                    .append(lower).append(upper)
                                    .append(exclusions).build();
    }

    public boolean equals(Object other)
    {
        if (!(other instanceof Expression))
            return false;

        if (this == other)
            return true;

        Expression o = (Expression) other;

        return Objects.equals(context.getColumnName(), o.context.getColumnName())
                && validator.equals(o.validator)
                && operation == o.operation
                && Objects.equals(lower, o.lower)
                && Objects.equals(upper, o.upper)
                && exclusions.equals(o.exclusions);
    }

    /**
     * Returns an expression that matches keys not matched by this expression.
     */
    public Expression negated()
    {
        Expression result = new Expression(context);
        result.lower = lower;
        result.upper = upper;

        switch (operation)
        {
            case NOT_EQ:
                result.operation = Op.EQ;
                break;
            case NOT_CONTAINS_KEY:
                result.operation = Op.CONTAINS_KEY;
                break;
            case NOT_CONTAINS_VALUE:
                result.operation = Op.CONTAINS_VALUE;
                break;
            default:
                throw new UnsupportedOperationException(String.format("Negation of operator %s not supported", operation));
        }
        return result;
    }

    /**
     * A representation of a column value in its raw and encoded form.
     */
    public static class Value
    {
        public final ByteBuffer raw;
        public final ByteBuffer encoded;

        /**
         * The native representation of our vector indexes is float[], so we cache that here as well
         * to avoid repeated expensive conversions.  Always null for non-vector types.
         */
        public final float[] vector;

        public Value(ByteBuffer value, AbstractType<?> type)
        {
            this.raw = value;
            this.encoded = TypeUtil.encode(value, type);
            this.vector = type.isVector() ? TypeUtil.decomposeVector(type, raw) : null;
        }

        @Override
        public boolean equals(Object other)
        {
            if (!(other instanceof Value))
                return false;

            Value o = (Value) other;
            return raw.equals(o.raw) && encoded.equals(o.encoded);
        }

        @Override
        public int hashCode()
        {
            HashCodeBuilder builder = new HashCodeBuilder();
            builder.append(raw);
            builder.append(encoded);
            return builder.toHashCode();
        }
    }

    public static class Bound
    {
        public final Value value;
        public final boolean inclusive;

        public Bound(ByteBuffer value, AbstractType<?> type, boolean inclusive)
        {
            this.value = new Value(value, type);
            this.inclusive = inclusive;
        }

        @Override
        public boolean equals(Object other)
        {
            if (!(other instanceof Bound))
                return false;

            Bound o = (Bound) other;
            return value.equals(o.value) && inclusive == o.inclusive;
        }

        @Override
        public int hashCode()
        {
            HashCodeBuilder builder = new HashCodeBuilder();
            builder.append(value);
            builder.append(inclusive);
            return builder.toHashCode();
        }
    }
}
