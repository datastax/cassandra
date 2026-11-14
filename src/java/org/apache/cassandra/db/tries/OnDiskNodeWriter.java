/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.tries;

import java.io.IOException;

import org.apache.cassandra.io.util.DataOutputPlus;

/// The node-writing half of on-disk trie construction.
///
/// A trie writer ([OnDiskBaseTrieWriter]) walks a trie and collects, for every node on the path it is on, the content
/// the node carries and the children that have been completed below it. What happens to a node once the walk has left it is decided
/// here: a node writer chooses when a node is serialized and where in the output it is placed, and is the only thing
/// that writes to the output. The serialized form is the same for all node writers (see
/// [OnDiskTrie.md](./OnDiskTrie.md)) and is read by [OnDiskCursor]; they differ in the layout they produce:
/// - [OnDiskNodeWriterImmediate] writes every node as soon as it is complete. It keeps no state beyond the path and
///   is the fastest way to serialize a trie, but pays no attention to the pages of the output.
/// - [OnDiskNodeWriterPageAware] holds completed nodes back until it has collected more than a page of them, and
///   then lays them out so that branches that fit in a page do not cross page boundaries.
///
/// The trie writer drives a node writer with the following protocol:
/// - [#newNode] gives it the objects it keeps on its path. The trie writer reuses them, and only touches the fields of
///   [Node]; any other state a node needs, in particular its completed children, belongs to the node writer.
/// - When the walk leaves a node, the trie writer calls [#complete], which must take everything the path node holds and
///   leave it empty for reuse, and then either
///   - [#attachCompleted] to make the node just completed a child of the next node up the path, which may well be the
///     object that was just emptied, or
///   - [#finish] if the completed node is the root of the trie.
///
/// ### Nested tries
///
/// A node of a trie can refer to another trie, its alternate branch: this is how the deletion branches of a
/// deletion-aware trie are stored (see [OnDiskDeletionAwareTrieWriter]). Such a nested trie is a trie in its own
/// right, with content of a different type, and is written by a trie writer and node writer of its own; but where it is
/// placed is very much the business of the node writer of the trie it belongs to, which may want to keep the branch
/// close to the node that refers to it. The node writer for the nested tries is thus obtained from the main one with
/// [#nested], and the two are linked:
/// - [#finish] of a nested writer does not have to write the trie out; it only has to keep it for the main writer.
/// - [#attachBranch] of the main writer takes the trie the nested writer has finished and makes it the alternate
///   branch of a node on the main trie writer's path. From that point on the branch is part of that node, and is written
///   with it at the latest.
///
/// Only one nested trie can be in progress at a time, and it must be attached before another is started.
public abstract class OnDiskNodeWriter<T>
{
    /// A node on the trie writer's current path. Node writers extend this with the list of completed children, in whatever
    /// form suits them.
    ///
    /// The two content fields name the slots of the serialized node (see the generic content node in
    /// [OnDiskTrie.md](./OnDiskTrie.md)), not the side of the walk the content was reported on: the trie writer has already
    /// applied any swap of the two.
    public abstract static class Node<T>
    {
        int depth;
        T descentContent;
        T ascentContent;
    }

    /// The node writers to choose from, i.e. the layouts an on-disk trie can be given.
    public enum Type
    {
        /// Write each node as soon as it is complete. See [OnDiskNodeWriterImmediate].
        IMMEDIATE
        {
            @Override
            public <T> OnDiskNodeWriter<T> create(DataOutputPlus out, OnDiskTrieWriter.DataSerializer<T> serializer)
            {
                return new OnDiskNodeWriterImmediate<>(out, serializer);
            }
        },

        /// Buffer nodes and pack them into the pages of the output, which must support the page-aware methods of
        /// [DataOutputPlus]. See [OnDiskNodeWriterPageAware].
        PAGE_AWARE
        {
            @Override
            public <T> OnDiskNodeWriter<T> create(DataOutputPlus out, OnDiskTrieWriter.DataSerializer<T> serializer)
            {
                return new OnDiskNodeWriterPageAware<>(out, serializer);
            }
        };

        /// Create a node writer of this type for a trie with the given type of content.
        public abstract <T> OnDiskNodeWriter<T> create(DataOutputPlus out, OnDiskTrieWriter.DataSerializer<T> serializer);
    }

    final DataOutputPlus out;
    final OnDiskTrieWriter.DataSerializer<T> serializer;

    OnDiskNodeWriter(DataOutputPlus out, OnDiskTrieWriter.DataSerializer<T> serializer)
    {
        this.out = out;
        this.serializer = serializer;
    }

    /// Create a node for the trie writer to place on its path. The trie writer keeps one per level of nodes on the path and
    /// reuses it for the whole serialization.
    abstract Node<T> newNode();

    /// The walk has left `node`, which thus has all the content, children and alternate branch it is going to have;
    /// at least one of the three is present. The node is reached from its parent by the transition that
    /// [#attachCompleted] gives, followed by the single-child transitions `path[chainStart, chainEnd)`; for the root,
    /// which has no parent, the latter are the whole path to the node. The array belongs to the trie writer and is only
    /// valid during the call.
    ///
    /// The implementation takes everything the node holds and leaves `node` empty, ready to be reused.
    abstract void complete(Node<T> node, byte[] path, int chainStart, int chainEnd) throws IOException;

    /// Make the node last given to [#complete] a child of `parent`, reached by the given transition. Children are
    /// attached to a node in descending order of their transitions.
    abstract void attachCompleted(Node<T> parent, int transition);

    /// The node last given to [#complete] is the root of the trie.
    ///
    /// For a node writer that is not nested, this makes sure all of the trie is written and returns the position of
    /// the root, which is also the position the output is left at.
    ///
    /// A nested node writer may leave some or all of the trie unwritten, to be placed by the writer it is nested in
    /// once the trie is attached to a node with [#attachBranch]. It returns the position of the root if it is already
    /// known, and 0 otherwise.
    abstract long finish() throws IOException;

    /// Create a node writer for tries that are nested in the one this writes, i.e. that will become the alternate
    /// branches of its nodes, where `serializer` gives the form of their content. The nested writer writes to the
    /// same output and produces the same kind of layout.
    abstract <B> OnDiskNodeWriter<B> nested(OnDiskTrieWriter.DataSerializer<B> serializer);

    /// Make the trie that `nested` has last finished the alternate branch of `node`, a node on the path of the trie
    /// that is being written by this node writer. `nested` must have been created by [#nested] of this writer, and
    /// the trie must not be empty.
    ///
    /// With a node writer that writes at once, the branch must be finished and attached before any of the node's
    /// children is completed.
    abstract void attachBranch(Node<T> node, OnDiskNodeWriter<?> nested);

    /// Prepare for writing another trie to the same output. Only to be called when no trie is in progress, i.e. before
    /// the first [#complete] or after a [#finish] and, for a nested writer, the [#attachBranch] that goes with it.
    void reset()
    {
    }
}
