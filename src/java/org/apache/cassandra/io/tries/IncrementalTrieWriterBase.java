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
package org.apache.cassandra.io.tries;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.concurrent.LightweightRecycler;
import org.apache.cassandra.utils.concurrent.ThreadLocals;

/**
 * Helper base class for incremental trie builders.
 */
public abstract class IncrementalTrieWriterBase<Value, Dest, Node extends IncrementalTrieWriterBase.BaseNode<Value, Node>>
implements IncrementalTrieWriter<Value>
{
    protected final Deque<Node> stack = new ArrayDeque<>();
    protected final TrieSerializer<Value, ? super Dest> serializer;
    protected final Dest dest;
    protected ByteComparable prev = null;
    long count = 0;

    public IncrementalTrieWriterBase(TrieSerializer<Value, ? super Dest> serializer, Dest dest, Node root)
    {
        this.serializer = serializer;
        this.dest = dest;
        this.stack.addLast(root);
    }

    protected void reset(Node root)
    {
        this.prev = null;
        this.count = 0;
        this.stack.clear();
        this.stack.addLast(root);
    }


    @Override
    public void close()
    {
        this.prev = null;
        this.count = 0;
        this.stack.clear();
    }

    @Override
    public void add(ByteComparable next, Value value) throws IOException
    {
        ++count;
        int stackpos = 0;
        ByteSource sn = next.asComparableBytes(Walker.BYTE_COMPARABLE_VERSION);
        int n = sn.next();

        if (prev != null)
        {
            ByteSource sp = prev.asComparableBytes(Walker.BYTE_COMPARABLE_VERSION);
            int p = sp.next();
            while ( n == p )
            {
                assert n != ByteSource.END_OF_STREAM : String.format("Incremental trie requires unique sorted keys, got equal %s after %s.", next, prev);
                ++stackpos;
                n = sn.next();
                p = sp.next();
            }
            assert p < n : String.format("Incremental trie requires sorted keys, got %s after %s.", next, prev);
        }
        prev = next;

        while (stack.size() > stackpos + 1)
            completeLast();

        Node node = stack.getLast();
        while (n != ByteSource.END_OF_STREAM)
        {
            stack.addLast(node = node.addChild((byte) n));
            ++stackpos;
            n = sn.next();
        }

        Value existingPayload = node.setPayload(value);
        assert existingPayload == null;
    }

    public long complete() throws IOException
    {
        Node root = stack.getFirst();
        if (root.filePos != -1)
            return root.filePos;

        return performCompletion().filePos;
    }

    Node performCompletion() throws IOException
    {
        Node root = null;
        while (!stack.isEmpty())
            root = completeLast();
        stack.addLast(root);
        return root;
    }

    public long count()
    {
        return count;
    }

    protected Node completeLast() throws IOException
    {
        Node node = stack.removeLast();
        complete(node);
        return node;
    }

    abstract void complete(Node value) throws IOException;
    abstract public PartialTail makePartialRoot() throws IOException;

    static class PTail implements PartialTail
    {
        long root;
        long cutoff;
        long count;
        ByteBuffer tail;

        @Override
        public long root()
        {
            return root;
        }

        @Override
        public long cutoff()
        {
            return cutoff;
        }

        @Override
        public ByteBuffer tail()
        {
            return tail;
        }

        @Override
        public long count()
        {
            return count;
        }
    }

    static abstract class BaseNode<Value, Node extends BaseNode<Value, Node>> implements SerializationNode<Value>
    {
        private static final int CHILDREN_LIST_RECYCLER_LIMIT = 1024;
        private static final LightweightRecycler<ArrayList> CHILDREN_LIST_RECYCLER = ThreadLocals.createLightweightRecycler(CHILDREN_LIST_RECYCLER_LIMIT);
        private static final ArrayList EMPTY_LIST = new ArrayList<>(0);

        private static <Node> ArrayList<Node> allocateChildrenList()
        {
            return CHILDREN_LIST_RECYCLER.reuseOrAllocate(() -> new ArrayList(4));
        }

        private static <Node> void recycleChildrenList(ArrayList<Node> children)
        {
            CHILDREN_LIST_RECYCLER.tryRecycle(children);
        }

        Value payload;
        ArrayList<Node> children;
        final int transition;
        long filePos = -1;

        BaseNode(int transition)
        {
            children = EMPTY_LIST;
            this.transition = transition;
        }

        public Value payload()
        {
            return payload;
        }

        public Value setPayload(Value newPayload)
        {
            Value p = payload;
            payload = newPayload;
            return p;
        }

        public Node addChild(byte b)
        {
            assert children.isEmpty() || (children.get(children.size() - 1).transition & 0xFF) < (b & 0xFF);
            Node node = newNode(b);
            if (children == EMPTY_LIST)
                children = allocateChildrenList();

            children.add(node);
            return node;
        }

        public int childCount()
        {
            return children.size();
        }

        void finalizeWithPosition(long position)
        {
            this.filePos = position;

            // Make sure we are not holding on to pointers to data we no longer need
            // (otherwise we keep the whole trie in memory).
            if (children != EMPTY_LIST)
                recycleChildrenList(children);

            children = null;
            payload = null;
        }

        public int transition(int i)
        {
            return children.get(i).transition;
        }

        @Override
        public String toString()
        {
            return String.format("%02x", transition);
        }

        abstract Node newNode(byte transition);
    }
}
