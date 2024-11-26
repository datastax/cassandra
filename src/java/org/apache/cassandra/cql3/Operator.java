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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.utils.ByteBufferUtil;

public enum Operator
{
    EQ(0)
    {
        @Override
        public String toString()
        {
            return "=";
        }

        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand, @Nullable Index.Analyzer analyzer)
        {
            if (analyzer != null)
                return ANALYZER_MATCHES.isSatisfiedBy(type, leftOperand, rightOperand, analyzer);

            return type.compareForCQL(leftOperand, rightOperand) == 0;
        }
    },
    LT(4)
    {
        @Override
        public String toString()
        {
            return "<";
        }

        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand, @Nullable Index.Analyzer analyzer)
        {
            return type.compareForCQL(leftOperand, rightOperand) < 0;
        }
    },
    LTE(3)
    {
        @Override
        public String toString()
        {
            return "<=";
        }

        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand, @Nullable Index.Analyzer analyzer)
        {
            return type.compareForCQL(leftOperand, rightOperand) <= 0;
        }
    },
    GTE(1)
    {
        @Override
        public String toString()
        {
            return ">=";
        }

        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand, @Nullable Index.Analyzer analyzer)
        {
            return type.compareForCQL(leftOperand, rightOperand) >= 0;
        }
    },
    GT(2)
    {
        @Override
        public String toString()
        {
            return ">";
        }

        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand, @Nullable Index.Analyzer analyzer)
        {
            return type.compareForCQL(leftOperand, rightOperand) > 0;
        }
    },
    IN(7)
    {
        @Override
        public String toString()
        {
            return "IN";
        }

        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand, @Nullable Index.Analyzer analyzer)
        {
            List<?> inValues = ListType.getInstance(type, false).getSerializer().deserialize(rightOperand);
            return inValues.contains(type.getSerializer().deserialize(leftOperand));
        }
    },
    CONTAINS(5)
    {
        @Override
        public String toString()
        {
            return "CONTAINS";
        }

        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand, @Nullable Index.Analyzer analyzer)
        {
            CollectionType<?> collectionType = (CollectionType<?>) type;
            return collectionType.contains(leftOperand, rightOperand);
        }
    },
    CONTAINS_KEY(6)
    {
        @Override
        public String toString()
        {
            return "CONTAINS KEY";
        }

        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand, @Nullable Index.Analyzer analyzer)
        {
            MapType<?, ?> mapType = (MapType<?, ?>) type;
            return mapType.containsKey(leftOperand, rightOperand);
        }
    },

    NEQ(8)
    {
        @Override
        public String toString()
        {
            return "!=";
        }

        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand, @Nullable Index.Analyzer analyzer)
        {
            return type.compareForCQL(leftOperand, rightOperand) != 0;

        }
    },
    IS_NOT(9)
    {
        @Override
        public String toString()
        {
            return "IS NOT";
        }

        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand, @Nullable Index.Analyzer analyzer)
        {
            throw new UnsupportedOperationException();
        }
    },
    LIKE_PREFIX(10)
    {
        @Override
        public String toString()
        {
            return "LIKE '<term>%'";
        }

        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand, @Nullable Index.Analyzer analyzer)
        {
            return ByteBufferUtil.startsWith(leftOperand, rightOperand);
        }
    },
    LIKE_SUFFIX(11)
    {
        @Override
        public String toString()
        {
            return "LIKE '%<term>'";
        }

        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand, @Nullable Index.Analyzer analyzer)
        {
            return ByteBufferUtil.endsWith(leftOperand, rightOperand);
        }
    },
    LIKE_CONTAINS(12)
    {
        @Override
        public String toString()
        {
            return "LIKE '%<term>%'";
        }

        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand, @Nullable Index.Analyzer analyzer)
        {
            return ByteBufferUtil.contains(leftOperand, rightOperand);
        }
    },
    LIKE_MATCHES(13)
    {
        @Override
        public String toString()
        {
            return "LIKE '<term>'";
        }

        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand, @Nullable Index.Analyzer analyzer)
        {
            return ByteBufferUtil.contains(leftOperand, rightOperand);
        }
    },
    LIKE(14)
    {
        @Override
        public String toString()
        {
            return "LIKE";
        }

        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand, @Nullable Index.Analyzer analyzer)
        {
            throw new UnsupportedOperationException();
        }
    },
    ANN(15)
    {
        @Override
        public String toString()
        {
            return "ANN";
        }

        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand, @Nullable Index.Analyzer analyzer)
        {
            return true;
        }
    },
    NOT_IN(16)
    {
        @Override
        public String toString()
        {
            return "NOT IN";
        }

        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand, @Nullable Index.Analyzer analyzer)
        {
            return !IN.isSatisfiedBy(type, leftOperand, rightOperand, analyzer);
        }
    },
    NOT_CONTAINS(17)
    {
        @Override
        public String toString()
        {
            return "NOT CONTAINS";
        }

        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand, @Nullable Index.Analyzer analyzer)
        {
            return !CONTAINS.isSatisfiedBy(type, leftOperand, rightOperand, analyzer);
        }

    },
    NOT_CONTAINS_KEY(18)
    {
        @Override
        public String toString()
        {
            return "NOT CONTAINS KEY";
        }

        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand, @Nullable Index.Analyzer analyzer)
        {
            return !CONTAINS_KEY.isSatisfiedBy(type, leftOperand, rightOperand, analyzer);
        }
    },
    NOT_LIKE_PREFIX(19)
    {
        @Override
        public String toString()
        {
            return "NOT LIKE '<term>%'";
        }

        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand, @Nullable Index.Analyzer analyzer)
        {
            return !LIKE_PREFIX.isSatisfiedBy(type, leftOperand, rightOperand, analyzer);
        }
    },
    NOT_LIKE_SUFFIX(20)
    {
        @Override
        public String toString()
        {
            return "NOT LIKE '%<term>'";
        }

        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand, @Nullable Index.Analyzer analyzer)
        {
            return !LIKE_SUFFIX.isSatisfiedBy(type, leftOperand, rightOperand, analyzer);
        }
    },
    NOT_LIKE_CONTAINS(21)
    {
        @Override
        public String toString()
        {
            return "NOT LIKE '%<term>%'";
        }

        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand, @Nullable Index.Analyzer analyzer)
        {
            return !LIKE_CONTAINS.isSatisfiedBy(type, leftOperand, rightOperand, analyzer);
        }
    },
    NOT_LIKE_MATCHES(22)
    {
        @Override
        public String toString()
        {
            return "NOT LIKE '<term>'";
        }

        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand, @Nullable Index.Analyzer analyzer)
        {
            return !LIKE_MATCHES.isSatisfiedBy(type, leftOperand, rightOperand, analyzer);
        }
    },
    NOT_LIKE(23)
    {
        @Override
        public String toString()
        {
            return "NOT LIKE";
        }

        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand, @Nullable Index.Analyzer analyzer)
        {
            return !LIKE.isSatisfiedBy(type, leftOperand, rightOperand, analyzer);
        }
    },

    /**
     * An operator that only performs matching against analyzed columns.
     */
    ANALYZER_MATCHES(100)
    {
        @Override
        public String toString()
        {
            return ":";
        }

        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand, @Nullable Index.Analyzer analyzer)
        {
            assert analyzer != null : ": operation can only be computed by an indexed column with a configured analyzer";

            List<ByteBuffer> leftTokens = analyzer.analyze(leftOperand);
            List<ByteBuffer> rightTokens = analyzer.analyze(rightOperand);

            Iterator<ByteBuffer> it = rightTokens.iterator();

            do
            {
                if (!it.hasNext())
                    return true;
            }
            while (hasToken(type, leftTokens, it.next()));

            return false;
        }

        private boolean hasToken(AbstractType<?> type, List<ByteBuffer> tokens, ByteBuffer token)
        {
            for (ByteBuffer t : tokens)
            {
                if (type.compareForCQL(t, token) == 0)
                    return true;
            }
            return false;
        }
    },
    /**
     * An operator that performs a distance bounded approximate nearest neighbor search against a vector column such
     * that all result vectors are within a given distance of the query vector. The notable difference between this
     * operator and {@link #ANN} is that it does not introduce an arbitrary limit on the number of results returned,
     * and as a consequence, it can be logically combined with other predicates and even unioned with other
     * {@code #BOUNDED_ANN} predicates.
     */
    BOUNDED_ANN(101)
    {
        @Override
        public String toString()
        {
            return "BOUNDED_ANN";
        }

        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand, @Nullable Index.Analyzer analyzer)
        {
            throw new UnsupportedOperationException();
        }
    },
    // VSTODO find a way to break this out, but for now, we're doing this to get the poc running
    ORDER_BY_ASC(102)
    {
        @Override
        public String toString()
        {
            return "ASC";
        }

        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand, @Nullable Index.Analyzer analyzer)
        {
            throw new UnsupportedOperationException();
        }
    },
    ORDER_BY_DESC(103)
    {
        @Override
        public String toString()
        {
            return "DESC";
        }

        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand, @Nullable Index.Analyzer analyzer)
        {
            throw new UnsupportedOperationException();
        }
    };

    /**
     * The binary representation of this <code>Enum</code> value.
     */
    private final int b;

    /**
     * Creates a new <code>Operator</code> with the specified binary representation.
     * @param b the binary representation of this <code>Enum</code> value
     */
    Operator(int b)
    {
        this.b = b;
    }

    /**
     * Write the serialized version of this <code>Operator</code> to the specified output.
     *
     * @param output the output to write to
     * @throws IOException if an I/O problem occurs while writing to the specified output
     */
    public void writeTo(DataOutput output) throws IOException
    {
        output.writeInt(b);
    }

    public int getValue()
    {
        return b;
    }

    /**
     * Deserializes a <code>Operator</code> instance from the specified input.
     *
     * @param input the input to read from
     * @return the <code>Operator</code> instance deserialized
     * @throws IOException if a problem occurs while deserializing the <code>Type</code> instance.
     */
    public static Operator readFrom(DataInput input) throws IOException
    {
          int b = input.readInt();
          for (Operator operator : values())
              if (operator.b == b)
                  return operator;

          throw new IOException(String.format("Cannot resolve Relation.Type from binary representation: %s", b));
    }

    /**
     * Whether 2 values satisfy this operator (given the type they should be compared with).
     *
     * @param type the type of the values to compare.
     * @param leftOperand the left operand of the comparison.
     * @param rightOperand the right operand of the comparison.
     * @param analyzer an index-provided function to transform the compared values before comparison, or {@code null} if
     * the values don't need to be transformed.
     */
    public abstract boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand, @Nullable Index.Analyzer analyzer);

    public int serializedSize()
    {
        return 4;
    }

    /**
     * Checks if this operator is a like operator.
     * @return {@code true} if this operator is a like operator, {@code false} otherwise.
     */
    public boolean isLike()
    {
        return this == LIKE_PREFIX || this == LIKE_CONTAINS || this == LIKE_SUFFIX || this == LIKE_MATCHES;
    }

    /**
     * Checks if this operator is a slice operator.
     * @return {@code true} if this operator is a slice operator, {@code false} otherwise.
     */
    public boolean isSlice()
    {
        return this == LT || this == LTE || this == GT || this == GTE;
    }

    @Override
    public String toString()
    {
         return this.name();
    }

    /**
     * Checks if this operator is an IN operator.
     * @return {@code true} if this operator is an IN operator, {@code false} otherwise.
     */
    public boolean isIN()
    {
        return this == IN;
    }
}
