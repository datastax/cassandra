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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/// An independent structural decoder of the serialized form of on-disk tries, used to check what the writers produce
/// without going through the reader.
///
/// This is written from the format description in `OnDiskTrie.md` and deliberately shares no code with
/// [OnDiskCursor], [OnDiskReadNodeType] or [OnDiskWriteNodeType], so that a misunderstanding that the reader and the
/// writers have in common does not go unnoticed. It knows nothing about content: payloads are opaque bytes.
///
/// Starting from the root position it decodes every node that can be reached, and checks that:
/// - every pointer (explicit pointers, the implicit first child of sparse and bitmap nodes, the child of chain and
///   prefix nodes, the target of a relay, the deletion branch pointer of a deletion-aware data trie) leads backwards to
///   a position inside the data, and no node is referenced more than once;
/// - the extents of the decoded nodes do not overlap, i.e. every pointer leads to the end of a node and not into the
///   middle of one;
/// - every byte that is not part of a reachable node is padding: made of the padding byte only, and extending to a
///   page boundary. When the data has no pages there must be no such byte at all. This is also what catches a child of
///   a dense node whose delta equals the all-ones marker of an absent child, because that child becomes unreachable;
/// - nodes are well-formed (transition order in sparse nodes, child counts, content flags, relays that lead somewhere
///   else than where they are and not to another relay, prefix nodes followed -- directly or through a relay -- by a
///   node that can list their children, i.e. neither a leaf nor another prefix node);
/// - every logical node that is no larger than a page lies within a single page.
///
/// ### Encoded and logical nodes
///
/// The checks above are made on _encoded_ nodes: the units of the format, each ending in a node code. A node of the
/// trie is usually serialized as several of them, e.g. a sparse node listing its children, followed by a prefix node
/// with its content, followed by a chain node with the transitions that lead to it, and possibly preceded by a relay
/// to its first child. A writer treats this sequence as one item when it decides where to place it, and a sequence
/// that is larger than a page must cross page boundaries even when some of its parts are small. The page check is
/// therefore applied to these sequences, called _logical_ nodes here: an encoded node together with the prefix and
/// chain nodes that wrap it, and the relay in front of it. The number of encoded nodes which are no larger than a
/// page and cross a page boundary is only reported ([Result#smallNodesCrossingPages]); it can only be non-zero when
/// there are logical nodes larger than a page.
///
/// Where the serialized form does not say which logical node an encoded node belongs to (a chain node directly below
/// a prefix node may be either the prefix node's single transition or the tail of its child's chain), the two logical
/// nodes are taken as one, which errs on the side of not reporting a problem.
///
/// ### Foreign data
///
/// A trie may be interleaved with data that is not part of it, written to the same output while the trie was being
/// written (a deletion-aware trie does this with its deletion branches). To check how a writer deals with distances
/// that are too big to produce for real, the verifier accepts data from which such regions have been left out, see
/// [#withForeignData].
final class OnDiskTrieVerifier
{
    enum Type
    {
        LEAF, CHAIN, SPARSE, BITMAP, DENSE, PREFIX, RELAY
    }

    /// The ways a node can be referenced.
    enum Ref
    {
        ROOT(false),
        DELETION_BRANCH(false),
        EXPLICIT(false),
        FIRST_CHILD(true),
        CHAIN_CHILD(true),
        PREFIX_CHILD(true),
        RELAY_TARGET(false);

        /// True if the reference is not stored, i.e. the referenced node must end where the referencing one begins.
        final boolean implicit;

        Ref(boolean implicit)
        {
            this.implicit = implicit;
        }
    }

    /// An encoded node.
    static final class Node
    {
        /// The position of the node, i.e. the offset just after its code byte.
        final long end;
        final int code;
        Type type;
        /// The offset of the first byte of the node's data.
        long start;
        /// Bytes per pointer for the types that store pointers.
        int pointerBytes;
        /// The positions this node refers to, in ascending order of transition (in a deletion-aware data trie the
        /// deletion branch of a prefix node comes first). Null if none.
        long[] targets;
        /// The transition of each target for sparse, bitmap and dense nodes; the transitions of a chain node in the
        /// order they are taken. Null for other types.
        byte[] transitions;
        long descentStart = -1;
        int descentLength;
        long ascentStart = -1;
        int ascentLength;
        long deletionBranch = -1;

        Ref referencedAs;
        /// The position of the node that refers to this one, -1 for the root.
        long referencedFrom = -1;
        /// Identifies the logical node this is part of.
        int group = -1;
        /// True for a single-transition chain node directly below a prefix node.
        boolean transitionOfPrefix;
        boolean inDeletionBranch;

        Node(long end, int code)
        {
            this.end = end;
            this.code = code;
        }

        long size()
        {
            return end - start;
        }

        boolean hasChild()
        {
            return targets != null && (type != Type.PREFIX || (code & 1) != 0);
        }

        @Override
        public String toString()
        {
            return String.format("%s(%02x)[%x,%x)", type, code, start, end);
        }
    }

    /// A content item, with the path that leads to it.
    static final class Entry
    {
        /// The path to the node that holds the content, from the root of the trie it is in.
        final byte[] key;
        /// The path to the node whose deletion branch the content is in, or null if it is not in a deletion branch.
        final byte[] branchOf;
        /// True if the content is in the ascent-side slot of the node.
        final boolean ascent;
        final byte[] content;

        Entry(byte[] key, byte[] branchOf, boolean ascent, byte[] content)
        {
            this.key = key;
            this.branchOf = branchOf;
            this.ascent = ascent;
            this.content = content;
        }
    }

    static final class Result
    {
        /// Violations of the format or of the tree structure. The decoding of anything after the first is unreliable.
        final List<String> errors = new ArrayList<>();
        /// Logical nodes that are no larger than a page and are not within a single page.
        final List<String> layoutErrors = new ArrayList<>();
        /// The positions, i.e. the ends, of the logical nodes the layout errors are about.
        final List<Long> misplacedLogicalNodes = new ArrayList<>();

        long dataStart;
        long root;
        int pageSize;

        /// The number of encoded nodes, relays included.
        int nodes;
        final int[] nodesByType = new int[Type.values().length];
        int logicalNodes;
        int relays;
        /// Relays reached through a stored pointer (or as the root) rather than standing in for the first child of
        /// the node that follows them.
        int explicitlyReferencedRelays;
        /// Relays in front of a prefix node, i.e. standing in for a child that has taken over the transition that
        /// leads to it (see the chain folding of [OnDiskNodeWriterPageAware]).
        int relaysBelowPrefix;
        /// Relays in front of a node that takes no more than a page together with everything below it.
        int relaysOfBranchesThatFitAPage;
        /// The subset of the above where the branch also has no more than [#MAX_BUFFERED_LEVELS] levels of logical
        /// nodes, i.e. where a page-aware writer had no reason to separate the node from its first child.
        int needlessRelays;
        /// Logical nodes that take more than a page together with everything below them, or have more than
        /// [#MAX_BUFFERED_LEVELS] levels of logical nodes below them. These are the nodes a page-aware writer cannot
        /// write together with all of their children, and thus the only ones it may have to place a relay in front of.
        int logicalNodesWithBranchOverAPage;
        /// Nodes storing pointers (relays included), by the number of bytes per pointer.
        final int[] nodesByPointerBytes = new int[9];
        int maxPointerBytes;
        /// Nodes whose pointers are wider than the furthest of the nodes they point to requires.
        int nodesWithOversizedPointers;
        int contents;
        int ascentContents;
        int deletionBranches;
        int maxKeyLength;
        /// The largest number of encoded nodes on a path from the root.
        int maxNodeDepth;
        /// The largest number of logical nodes on a path from the root (of the trie or of a deletion branch).
        int maxLogicalDepth;
        int chainNodesOfFullLength;
        long largestNode;

        long paddingBytes;
        int paddingRuns;
        long longestPaddingRun;
        /// The number of pages the data touches. 0 if no page size was given.
        long pages;
        int nodesCrossingPages;
        /// Encoded nodes that are no larger than a page and cross a page boundary.
        int smallNodesCrossingPages;
        /// Logical nodes that are larger than a page.
        int oversizedLogicalNodes;
        /// Logical nodes larger than a page that cross more page boundaries than a node of their size has to.
        int oversizedLogicalNodesWithExtraCrossings;
        /// The number of paths from the root that end in a node without children.
        long leafPaths;
        /// The number of times a step from a node to the next on one of these paths leads to a different page, summed
        /// over all the paths, and the largest number for a single path.
        long pathPageCrossings;
        int maxPathPageCrossings;

        /// The content of the trie in the order of a forward walk: the descent-side content of a node, then
        /// everything in its deletion branch, then everything below it, then its ascent-side content.
        final List<Entry> entries = new ArrayList<>();
        /// All the encoded nodes, in the order of the same walk.
        List<Node> allNodes;

        boolean isValid()
        {
            return errors.isEmpty() && layoutErrors.isEmpty();
        }

        /// Fails if any structural or layout problem was found.
        void assertValid(String message)
        {
            if (!isValid())
                throw new AssertionError(message + ": " + describeProblems());
        }

        /// Fails if any structural problem was found, ignoring the placement of nodes in pages.
        void assertStructurallyValid(String message)
        {
            if (!errors.isEmpty())
                throw new AssertionError(message + ": " + describeProblems());
        }

        String describeProblems()
        {
            StringBuilder sb = new StringBuilder();
            sb.append(errors.size()).append(" structural and ").append(layoutErrors.size()).append(" layout problems in ").append(this);
            for (String error : errors)
                sb.append("\n  ").append(error);
            for (String error : layoutErrors)
                sb.append("\n  ").append(error);
            return sb.toString();
        }

        int count(Type type)
        {
            return nodesByType[type.ordinal()];
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("trie [%x,%x) pageSize %d: %d nodes (", dataStart, root, pageSize, nodes));
            for (Type type : Type.values())
                sb.append(type).append(' ').append(count(type)).append(type == Type.RELAY ? "" : ", ");
            sb.append(String.format("), %d logical, %d contents, max key length %d, max pointer bytes %d", logicalNodes, contents, maxKeyLength, maxPointerBytes));
            if (deletionBranches > 0)
                sb.append(String.format(", %d deletion branches", deletionBranches));
            if (pageSize > 0)
                sb.append(String.format(", %d pages, %d padding bytes in %d runs, %d logical nodes over a page, %d nodes crossing pages (%d of them small)," +
                                        " %d page crossings on %d leaf paths (max %d)",
                                        pages, paddingBytes, paddingRuns, oversizedLogicalNodes, nodesCrossingPages, smallNodesCrossingPages,
                                        pathPageCrossings, leafPaths, maxPathPageCrossings));
            return sb.toString();
        }
    }

    /// The number of levels of nodes [OnDiskNodeWriterPageAware] is prepared to keep in memory. Repeated here on
    /// purpose, see [Result#needlessRelays].
    static final int MAX_BUFFERED_LEVELS = 128;

    private static final int MAX_ERRORS = 20;

    private final ByteBuffer data;
    private long dataStart = 0;
    private int pageSize = 0;
    private boolean enforcePages = false;
    private byte padding = 0;
    private boolean deletionAware = false;
    private boolean collectEntries = true;
    /// The regions of foreign data, as pairs of start and end offsets in ascending order.
    private long[] foreign = new long[0];

    // state of a verification
    private Result result;
    private Map<Long, Node> nodes;
    private List<Node> nodeList;
    private long[] groupStart;
    private long[] groupEnd;
    private long[] groupBytes;
    private int groupCount;
    private byte[] key;

    /// @param data the serialized trie; only absolute access is used, the buffer's position is left alone
    OnDiskTrieVerifier(ByteBuffer data)
    {
        this.data = data;
    }

    OnDiskTrieVerifier(byte[] data)
    {
        this(ByteBuffer.wrap(data));
    }

    /// The offset at which the trie begins. Nothing before it may be referenced, and it is not examined.
    OnDiskTrieVerifier from(long dataStart)
    {
        this.dataStart = dataStart;
        return this;
    }

    /// The data is divided in pages of the given size, counted from offset 0, and unused space is filled with the
    /// given byte.
    OnDiskTrieVerifier pages(int pageSize, byte padding)
    {
        this.pageSize = pageSize;
        this.padding = padding;
        this.enforcePages = pageSize > 0;
        return this;
    }

    /// The data was written without regard for pages, but the page statistics should be collected for pages of the
    /// given size.
    OnDiskTrieVerifier pagesForStatisticsOnly(int pageSize)
    {
        this.pageSize = pageSize;
        this.enforcePages = false;
        return this;
    }

    /// The trie is the data trie of a deletion-aware trie: the ascent-side slot of a prefix node holds the position
    /// of the root of the node's deletion branch, which is a range trie.
    OnDiskTrieVerifier deletionAware()
    {
        this.deletionAware = true;
        return this;
    }

    OnDiskTrieVerifier withoutEntries()
    {
        this.collectEntries = false;
        return this;
    }

    /// The offsets `[from, to)` hold something that is not part of the trie and are NOT present in the data given to
    /// this verifier: all offsets, the ones stored in the trie included, are the ones of the complete file, and the
    /// data only has the bytes outside the foreign regions. Nothing in the trie may point into a foreign region, no
    /// node may extend over one, and the bytes between the nodes of the trie and a foreign region are subject to the
    /// same rules as all other bytes outside nodes. Regions must be given in ascending order.
    OnDiskTrieVerifier withForeignData(long from, long to)
    {
        assert from < to && (foreign.length == 0 || from >= foreign[foreign.length - 1]);
        foreign = Arrays.copyOf(foreign, foreign.length + 2);
        foreign[foreign.length - 2] = from;
        foreign[foreign.length - 1] = to;
        return this;
    }

    /// The offset one past the last byte of the data, counting the foreign regions.
    private long limit()
    {
        long limit = data.limit();
        for (int i = 0; i < foreign.length; i += 2)
            limit += foreign[i + 1] - foreign[i];
        return limit;
    }

    /// Returns true if some byte of `[from, to)` is foreign.
    private boolean hasForeign(long from, long to)
    {
        for (int i = 0; i < foreign.length; i += 2)
            if (from < foreign[i + 1] && to > foreign[i])
                return true;
        return false;
    }

    /// Decodes and checks the trie with the given root position, which is also where the data must end.
    Result verify(long root)
    {
        result = new Result();
        result.dataStart = dataStart;
        result.root = root;
        result.pageSize = pageSize;
        nodes = new HashMap<>();
        nodeList = new ArrayList<>();
        groupStart = new long[16];
        groupEnd = new long[16];
        groupBytes = new long[16];
        groupCount = 0;
        key = new byte[64];

        if (root > limit())
            error("root position %x is beyond the end of the data at %x", root, limit());
        else
        {
            Node rootNode = reference(null, Ref.ROOT, root, deletionAware);
            if (rootNode != null)
                walk(rootNode);
        }

        if (result.errors.isEmpty())
        {
            checkCoverage(root);
            checkPages(root);
        }
        result.logicalNodes = groupCount;
        result.allNodes = nodeList;
        Result r = result;
        result = null;
        nodes = null;
        nodeList = null;
        return r;
    }

    private void error(String format, Object... args)
    {
        if (result.errors.size() < MAX_ERRORS)
            result.errors.add(String.format(format, args));
    }

    // Reading

    private int byteAt(long offset)
    {
        long index = offset;
        for (int i = 0; i < foreign.length && foreign[i] <= offset; i += 2)
        {
            if (offset < foreign[i + 1])
            {
                // a node that reaches into foreign data; what is decoded from here on is meaningless
                error("the byte at %x, which is foreign data, is taken as part of a node", offset);
                return 0;
            }
            index -= foreign[i + 1] - foreign[i];
        }
        return data.get((int) index) & 0xFF;
    }

    /// Reads an unsigned integer stored reversed, i.e. with its least significant byte first, in `[from, from + bytes)`.
    private long readReversed(long from, int bytes)
    {
        long value = 0;
        for (int i = bytes - 1; i >= 0; --i)
            value = (value << 8) | byteAt(from + i);
        return value;
    }

    private byte[] bytes(long from, int length)
    {
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; ++i)
            bytes[i] = (byte) byteAt(from + i);
        return bytes;
    }

    private String hex(long from, long to)
    {
        StringBuilder sb = new StringBuilder();
        long shown = Math.min(to, from + 48);
        for (long i = from; i < shown; ++i)
            sb.append(String.format("%02x ", byteAt(i)));
        if (shown < to)
            sb.append("...");
        return sb.toString().trim();
    }

    // Decoding

    /// The size of the variable-length integer whose leading byte is the one before `end`. The integer is stored
    /// reversed, i.e. reading backwards from `end` gives the bytes of the usual encoding: a leading byte with as many
    /// high bits set as there are further bytes, followed by the rest of the value, most significant byte first.
    private int vintSize(long end)
    {
        return Integer.numberOfLeadingZeros(~byteAt(end - 1) & 0xFF) - 24 + 1;
    }

    private long readVint(long end, int size)
    {
        int extra = size - 1;
        long value = extra == 8 ? 0 : byteAt(end - 1) & (0xFF >> extra);
        for (int i = 1; i <= extra; ++i)
            value = (value << 8) | byteAt(end - 1 - i);
        return value;
    }

    /// Decodes the content item that ends at `end`. Returns the offset where it begins, or -1 if it is malformed.
    private long decodeContent(Node node, long end, boolean ascent, String what)
    {
        if (end - 1 < dataStart)
        {
            error("%s: no room for the length of the %s content", what, ascent ? "ascent" : "descent");
            return -1;
        }
        int vintSize = vintSize(end);
        if (end - vintSize < dataStart)
        {
            error("%s: no room for the length of the %s content", what, ascent ? "ascent" : "descent");
            return -1;
        }
        long length = readVint(end, vintSize);
        long start = end - vintSize - length;
        if (length < 0 || length > Integer.MAX_VALUE || start < dataStart)
        {
            error("%s: %s content of length %d does not fit before %x", what, ascent ? "ascent" : "descent", length, end);
            return -1;
        }
        if (ascent)
        {
            node.ascentStart = start;
            node.ascentLength = (int) length;
        }
        else
        {
            node.descentStart = start;
            node.descentLength = (int) length;
        }
        return start;
    }

    private static int bytesFor(long value)
    {
        int bytes = 1;
        while (bytes < 8 && (value >>> (bytes * 8)) != 0)
            ++bytes;
        return bytes;
    }

    /// Decodes the node at the given position. Returns null, after recording the problem, if it is malformed.
    ///
    /// @param branchPointers true if the ascent-side slot of a prefix node holds a deletion branch pointer
    private Node decode(long pos, boolean branchPointers, String what)
    {
        int code = byteAt(pos - 1);
        Node node = new Node(pos, code);
        long cur = pos - 1;
        if (code < 0x40)
        {
            // <data (n bytes)> 00nnnnnn
            node.type = Type.LEAF;
            node.start = cur - code;
            node.descentStart = node.start;
            node.descentLength = code;
        }
        else if (code < 0x80)
        {
            // <transition n> ... <transition 0> 01nnnnnn
            node.type = Type.CHAIN;
            int length = (code & 0x3F) + 1;
            node.start = cur - length;
            if (node.start >= dataStart)
            {
                node.transitions = new byte[length];
                for (int i = 0; i < length; ++i)
                    node.transitions[i] = (byte) byteAt(cur - 1 - i);
                node.targets = new long[]{ node.start };
            }
        }
        else if (code < 0xE0)
        {
            // <pointer n+1> ... <pointer 1> <transition n+1> ... <transition 0> 1nnnnnbb
            node.type = Type.SPARSE;
            int count = ((code >> 2) & 0x1F) + 2;
            node.pointerBytes = (code & 3) + 1;
            long transitionsStart = cur - count;
            node.start = transitionsStart - (long) (count - 1) * node.pointerBytes;
            if (node.start >= dataStart)
            {
                for (int i = 0; i + 1 < count; ++i)
                    if (byteAt(transitionsStart + i) <= byteAt(transitionsStart + i + 1))
                    {
                        error("%s: transitions of sparse node at %x are not in strictly descending order: %s", what, pos, hex(transitionsStart, cur));
                        return null;
                    }
                // In the file both the transitions and the pointers go from the last child to the first, and the
                // first child has no pointer.
                node.transitions = new byte[count];
                node.targets = new long[count];
                for (int j = 0; j < count; ++j)
                {
                    int i = count - 1 - j;
                    node.transitions[j] = (byte) byteAt(transitionsStart + i);
                    node.targets[j] = j == 0 ? node.start : node.start - readReversed(node.start + (long) i * node.pointerBytes, node.pointerBytes);
                }
            }
        }
        else if (code < 0xE8)
        {
            // <pointer n-1> ... <pointer 1> <256-bit bitmap> 11100bbb
            node.type = Type.BITMAP;
            node.pointerBytes = (code & 7) + 1;
            long bitmapStart = cur - 32;
            if (bitmapStart < dataStart)
            {
                error("%s: bitmap node at %x begins before the start of the data", what, pos);
                return null;
            }
            // The bitmap is a 256-bit number with its most significant byte first: transition t is bit (t % 8) of
            // the byte 31 - t / 8.
            byte[] transitions = new byte[256];
            int count = 0;
            for (int t = 0; t < 256; ++t)
                if (((byteAt(bitmapStart + 31 - (t >> 3)) >> (t & 7)) & 1) != 0)
                    transitions[count++] = (byte) t;
            // Up to all 256: a dense node can need wider pointers than a bitmap node with the same children.
            if (count < 2)
            {
                error("%s: bitmap node at %x has %d children", what, pos, count);
                return null;
            }
            node.start = bitmapStart - (long) (count - 1) * node.pointerBytes;
            if (node.start >= dataStart)
            {
                if (count <= 25 && node.pointerBytes <= 4)
                {
                    error("%s: bitmap node at %x has %d children and %d-byte pointers, which is what sparse nodes are for", what, pos, count, node.pointerBytes);
                    return null;
                }
                node.transitions = Arrays.copyOf(transitions, count);
                node.targets = new long[count];
                for (int j = 0; j < count; ++j)
                {
                    int i = count - 1 - j;
                    node.targets[j] = j == 0 ? node.start : node.start - readReversed(node.start + (long) i * node.pointerBytes, node.pointerBytes);
                }
            }
        }
        else if (code < 0xF0)
        {
            // <pointer 255> ... <pointer 0> 11101bbb, all-ones for an absent child
            node.type = Type.DENSE;
            node.pointerBytes = (code & 7) + 1;
            node.start = cur - 256L * node.pointerBytes;
            if (node.start >= dataStart)
            {
                long absent = node.pointerBytes == 8 ? -1L : (1L << (node.pointerBytes * 8)) - 1;
                byte[] transitions = new byte[256];
                long[] targets = new long[256];
                int count = 0;
                for (int t = 0; t < 256; ++t)
                {
                    long delta = readReversed(node.start + (255L - t) * node.pointerBytes, node.pointerBytes);
                    if (delta != absent)
                    {
                        transitions[count] = (byte) t;
                        targets[count++] = node.start - delta;
                    }
                }
                if (count < 2)
                {
                    error("%s: dense node at %x has %d children", what, pos, count);
                    return null;
                }
                node.transitions = Arrays.copyOf(transitions, count);
                node.targets = Arrays.copyOf(targets, count);
            }
        }
        else if (code < 0xF8)
        {
            // <ascent content><length> <descent content><length> 11110dap
            node.type = Type.PREFIX;
            boolean hasDescent = (code & 4) != 0;
            boolean hasAscent = (code & 2) != 0;
            boolean hasChild = (code & 1) != 0;
            if (!hasDescent && !hasAscent)
            {
                error("%s: prefix node at %x has no content", what, pos);
                return null;
            }
            if (hasDescent)
            {
                cur = decodeContent(node, cur, false, what);
                if (cur < 0)
                    return null;
            }
            if (hasAscent)
            {
                cur = decodeContent(node, cur, true, what);
                if (cur < 0)
                    return null;
            }
            node.start = cur;
            if (hasAscent && branchPointers)
            {
                if (node.ascentLength > 8)
                {
                    error("%s: deletion branch pointer of prefix node at %x has %d bytes", what, pos, node.ascentLength);
                    return null;
                }
                // The pointer is a delta from the start of the slot, which is where the node starts. A delta of 0
                // takes no bytes, and is only possible when the node has no children.
                long delta = node.ascentLength > 0 ? readReversed(node.ascentStart, node.ascentLength) : 0;
                if (delta == 0 ? node.ascentLength != 0 : bytesFor(delta) != node.ascentLength)
                {
                    error("%s: deletion branch pointer delta %x of prefix node at %x is stored in %d bytes", what, delta, pos, node.ascentLength);
                    return null;
                }
                if (delta == 0 && hasChild)
                {
                    error("%s: deletion branch pointer of prefix node at %x with a child has delta 0", what, pos);
                    return null;
                }
                node.deletionBranch = node.start - delta;
                node.targets = hasChild ? new long[]{ node.deletionBranch, node.start } : new long[]{ node.deletionBranch };
            }
            else if (hasChild)
                node.targets = new long[]{ node.start };
        }
        else
        {
            // <pointer> 11111bbb
            node.type = Type.RELAY;
            node.pointerBytes = (code & 7) + 1;
            node.start = cur - node.pointerBytes;
            if (node.start >= dataStart)
            {
                long delta = readReversed(node.start, node.pointerBytes);
                if (delta <= 0)
                {
                    error("%s: relay at %x has the distance %d", what, pos, delta);
                    return null;
                }
                node.targets = new long[]{ node.start - delta };
            }
        }

        if (node.start < dataStart)
        {
            error("%s: %s node at %x begins at %x, before the start of the data at %x", what, node.type, pos, node.start, dataStart);
            return null;
        }
        return node;
    }

    private static Ref edgeRef(Node node, int edge)
    {
        switch (node.type)
        {
            case CHAIN:
                return Ref.CHAIN_CHILD;
            case RELAY:
                return Ref.RELAY_TARGET;
            case PREFIX:
                return node.deletionBranch >= 0 && edge == 0 ? Ref.DELETION_BRANCH : Ref.PREFIX_CHILD;
            case SPARSE:
            case BITMAP:
                return edge == 0 ? Ref.FIRST_CHILD : Ref.EXPLICIT;
            case DENSE:
                return Ref.EXPLICIT;
            default:
                throw new AssertionError();
        }
    }

    /// Follows a reference: checks where it leads, decodes the node there and registers it.
    private Node reference(Node from, Ref ref, long target, boolean branchPointers)
    {
        String what = from == null ? String.format("%s at %x", ref, target)
                                   : String.format("%s of %s -> %x", ref, from, target);
        // A node takes at least a byte, thus its position cannot be the very start of the data.
        if (target <= dataStart || target > limit())
        {
            error("%s leads outside the data, which starts at %x", what, dataStart);
            return null;
        }
        if (hasForeign(target - 1, target))
        {
            error("%s leads into foreign data", what);
            return null;
        }
        if (from != null && target > from.start)
        {
            error("%s does not lead to a position before the node", what);
            return null;
        }
        Node existing = nodes.get(target);
        if (existing != null)
        {
            error("%s leads to %s which is already referenced as %s from %x", what, existing, existing.referencedAs, existing.referencedFrom);
            return null;
        }
        Node node = decode(target, branchPointers, what);
        if (node == null)
            return null;
        if (hasForeign(node.start, node.end))
        {
            error("%s leads to %s, which extends over foreign data", what, node);
            return null;
        }

        // What follows a prefix node, directly or through a relay, is the node that lists its children.
        boolean listsChildrenOfPrefix = ref == Ref.PREFIX_CHILD
                                        || ref == Ref.RELAY_TARGET && from.referencedAs == Ref.PREFIX_CHILD;
        if (listsChildrenOfPrefix && (node.type == Type.LEAF || node.type == Type.PREFIX))
        {
            error("%s leads to %s, which cannot list the children of a prefix node", what, node);
            return null;
        }
        // A relay stands for a node and readers follow it only once.
        if (ref == Ref.RELAY_TARGET && node.type == Type.RELAY)
        {
            error("%s leads to another relay", what);
            return null;
        }

        node.referencedAs = ref;
        node.referencedFrom = from == null ? -1 : from.end;
        node.inDeletionBranch = ref == Ref.DELETION_BRANCH || from != null && from.inDeletionBranch;
        nodes.put(target, node);
        nodeList.add(node);
        assignGroup(from, ref, node);

        ++result.nodes;
        ++result.nodesByType[node.type.ordinal()];
        result.largestNode = Math.max(result.largestNode, node.size());
        if (node.type == Type.CHAIN && node.transitions.length == 64)
            ++result.chainNodesOfFullLength;
        if (node.type == Type.RELAY)
        {
            ++result.relays;
            if (!ref.implicit)
                ++result.explicitlyReferencedRelays;
            if (ref == Ref.PREFIX_CHILD)
                ++result.relaysBelowPrefix;
        }
        if (ref == Ref.DELETION_BRANCH)
            ++result.deletionBranches;
        if (node.pointerBytes > 0)
            checkPointerBytes(node);
        return node;
    }

    private void checkPointerBytes(Node node)
    {
        ++result.nodesByPointerBytes[node.pointerBytes];
        result.maxPointerBytes = Math.max(result.maxPointerBytes, node.pointerBytes);
        long furthest = 0;
        for (int i = edgeRef(node, 0) == Ref.FIRST_CHILD ? 1 : 0; i < node.targets.length; ++i)
            furthest = Math.max(furthest, node.start - node.targets[i]);
        // The furthest delta of a dense node must be below the all-ones value.
        int needed = bytesFor(node.type == Type.DENSE ? furthest + 1 : furthest);
        if (node.pointerBytes > needed)
            ++result.nodesWithOversizedPointers;
    }

    /// Decides if the node is part of the same logical node as the one that refers to it, see the class description.
    private void assignGroup(Node from, Ref ref, Node node)
    {
        boolean sameAsReferrer;
        switch (ref)
        {
            case FIRST_CHILD:
                // a relay in front of the node whose first child it stands for
                sameAsReferrer = node.type == Type.RELAY;
                break;
            case PREFIX_CHILD:
                // The node that lists the children of the prefix node, or the relay in front of the prefix node when
                // its only child has taken over the transition. A chain of several transitions is that child.
                sameAsReferrer = node.type != Type.CHAIN || node.transitions.length == 1;
                node.transitionOfPrefix = node.type == Type.CHAIN && sameAsReferrer;
                break;
            case CHAIN_CHILD:
                if (from.transitionOfPrefix)
                    // The single transition of a prefix node leads to another node, unless that is a relay to it.
                    // If a chain node of the maximum length follows, the transition can also be the 65th of the
                    // child's own chain; treat the two logical nodes as one.
                    sameAsReferrer = node.type == Type.RELAY || node.type == Type.CHAIN && node.transitions.length == 64;
                else
                    // the chain of transitions that leads to a node is part of it
                    sameAsReferrer = true;
                break;
            default:
                sameAsReferrer = false;
                break;
        }

        if (sameAsReferrer)
            node.group = from.group;
        else
        {
            if (groupCount == groupStart.length)
            {
                groupStart = Arrays.copyOf(groupStart, groupCount * 2);
                groupEnd = Arrays.copyOf(groupEnd, groupCount * 2);
                groupBytes = Arrays.copyOf(groupBytes, groupCount * 2);
            }
            node.group = groupCount++;
            groupStart[node.group] = node.start;
            groupEnd[node.group] = node.end;
            groupBytes[node.group] = 0;
        }
        groupStart[node.group] = Math.min(groupStart[node.group], node.start);
        groupEnd[node.group] = Math.max(groupEnd[node.group], node.end);
        groupBytes[node.group] += node.size();
    }

    // Walking

    private static final class Frame
    {
        final Node node;
        final int keyLength;
        /// Where the key of the trie the node is in begins: 0, or the length of the key of the owner of the deletion
        /// branch.
        final int keyBase;
        final int pageCrossings;
        int nextEdge = 0;

        // collected from the nodes below
        long branchBytes;
        /// The number of levels of logical nodes in the branch.
        int logicalLevels = 1;
        /// True if one of the encoded nodes of this logical node, among the ones walked so far, has a relay in
        /// place of its first child.
        boolean relayInLogicalNode;

        Frame(Node node, int keyLength, int keyBase, int pageCrossings)
        {
            this.node = node;
            this.keyLength = keyLength;
            this.keyBase = keyBase;
            this.pageCrossings = pageCrossings;
            this.branchBytes = node.size();
        }
    }

    private long pageOf(Node node)
    {
        // the page holding the node's code, which is where reading it starts
        return (node.end - 1) / pageSize;
    }

    private void addEntry(Frame frame, boolean ascent, long start, int length)
    {
        ++result.contents;
        if (ascent)
            ++result.ascentContents;
        if (collectEntries)
            result.entries.add(new Entry(Arrays.copyOfRange(key, frame.keyBase, frame.keyLength),
                                         frame.node.inDeletionBranch ? Arrays.copyOf(key, frame.keyBase) : null,
                                         ascent,
                                         bytes(start, length)));
    }

    /// Walks everything reachable from the given node in forward order, without recursion as tries can be deep.
    private void walk(Node root)
    {
        ArrayList<Frame> stack = new ArrayList<>();
        Frame rootFrame = new Frame(root, 0, 0, 0);
        stack.add(rootFrame);
        enter(rootFrame, 1, 1);
        while (!stack.isEmpty())
        {
            Frame frame = stack.get(stack.size() - 1);
            Node node = frame.node;
            int edges = node.targets == null ? 0 : node.targets.length;
            if (frame.nextEdge < edges && result.errors.size() < MAX_ERRORS)
            {
                int edge = frame.nextEdge++;
                Ref ref = edgeRef(node, edge);
                boolean inBranch = node.inDeletionBranch || ref == Ref.DELETION_BRANCH;
                Node child = reference(node, ref, node.targets[edge], deletionAware && !inBranch);
                if (child == null)
                    continue;

                int keyLength = frame.keyLength;
                int keyBase = frame.keyBase;
                if (ref == Ref.DELETION_BRANCH)
                    keyBase = keyLength;
                else if (node.type == Type.CHAIN)
                {
                    ensureKeyCapacity(keyLength + node.transitions.length);
                    System.arraycopy(node.transitions, 0, key, keyLength, node.transitions.length);
                    keyLength += node.transitions.length;
                }
                else if (node.transitions != null)
                {
                    ensureKeyCapacity(keyLength + 1);
                    key[keyLength++] = node.transitions[edge];
                }

                if (ref.implicit && child.type == Type.RELAY)
                    frame.relayInLogicalNode = true;

                int crossings = frame.pageCrossings;
                if (pageSize > 0 && pageOf(child) != pageOf(node))
                    ++crossings;
                Frame childFrame = new Frame(child, keyLength, keyBase, crossings);
                stack.add(childFrame);
                enter(childFrame, stack.size(), keyLength - keyBase);
            }
            else
            {
                stack.remove(stack.size() - 1);
                Frame parent = stack.isEmpty() ? null : stack.get(stack.size() - 1);
                boolean topOfLogicalNode = parent == null || parent.node.group != node.group;
                exit(frame, topOfLogicalNode);
                if (parent != null)
                {
                    parent.branchBytes += frame.branchBytes;
                    if (!topOfLogicalNode)
                    {
                        parent.logicalLevels = Math.max(parent.logicalLevels, frame.logicalLevels);
                        parent.relayInLogicalNode |= frame.relayInLogicalNode;
                    }
                    else if (node.referencedAs != Ref.DELETION_BRANCH)  // a deletion branch is a trie of its own
                        parent.logicalLevels = Math.max(parent.logicalLevels, frame.logicalLevels + 1);
                }
                if (parent == null || node.referencedAs == Ref.DELETION_BRANCH)
                    result.maxLogicalDepth = Math.max(result.maxLogicalDepth, frame.logicalLevels);
            }
        }
    }

    private void ensureKeyCapacity(int length)
    {
        if (length > key.length)
            key = Arrays.copyOf(key, Math.max(length, key.length * 2));
    }

    private void enter(Frame frame, int nodeDepth, int keyLength)
    {
        Node node = frame.node;
        result.maxNodeDepth = Math.max(result.maxNodeDepth, nodeDepth);
        result.maxKeyLength = Math.max(result.maxKeyLength, keyLength);
        if (node.descentStart >= 0)
            addEntry(frame, false, node.descentStart, node.descentLength);
        if (node.type == Type.LEAF || node.type == Type.PREFIX && (node.code & 1) == 0)
        {
            ++result.leafPaths;
            result.pathPageCrossings += frame.pageCrossings;
            result.maxPathPageCrossings = Math.max(result.maxPathPageCrossings, frame.pageCrossings);
        }
    }

    private void exit(Frame frame, boolean topOfLogicalNode)
    {
        Node node = frame.node;
        if (node.ascentStart >= 0 && node.deletionBranch < 0)
            addEntry(frame, true, node.ascentStart, node.ascentLength);
        // At the top of a logical node the frame has the size and depth of the whole branch that hangs from it.
        if (topOfLogicalNode && pageSize > 0 && (frame.branchBytes > pageSize || frame.logicalLevels > MAX_BUFFERED_LEVELS))
            ++result.logicalNodesWithBranchOverAPage;
        if (topOfLogicalNode && frame.relayInLogicalNode && pageSize > 0 && frame.branchBytes <= pageSize)
        {
            ++result.relaysOfBranchesThatFitAPage;
            if (frame.logicalLevels <= MAX_BUFFERED_LEVELS)
                ++result.needlessRelays;
        }
    }

    // Checks of the file as a whole

    /// Checks that the nodes do not overlap and that everything between them is padding.
    private void checkCoverage(long root)
    {
        Node[] sorted = nodeList.toArray(new Node[0]);
        Arrays.sort(sorted, Comparator.comparingLong((Node n) -> n.start).thenComparingLong(n -> n.end));
        long covered = dataStart;
        Node last = null;
        for (Node node : sorted)
        {
            if (node.start < covered)
                error("%s (%s from %x) overlaps %s (%s from %x)", node, node.referencedAs, node.referencedFrom, last, last.referencedAs, last.referencedFrom);
            else if (node.start > covered)
                checkUnreachable(covered, node.start);
            if (node.end > covered)
            {
                covered = node.end;
                last = node;
            }
        }
        if (covered != root)
            error("the reachable nodes end at %x and the root at %x", covered, root);
    }

    /// Checks the bytes between two nodes, which are padding or foreign.
    private void checkUnreachable(long from, long to)
    {
        for (int i = 0; i < foreign.length && from < to; i += 2)
        {
            if (foreign[i + 1] <= from || foreign[i] >= to)
                continue;
            if (foreign[i] > from)
                checkPadding(from, foreign[i]);
            from = foreign[i + 1];
        }
        if (from < to)
            checkPadding(from, to);
    }

    private void checkPadding(long from, long to)
    {
        if (!enforcePages)
        {
            error("the bytes [%x,%x) are not part of any reachable node: %s", from, to, hex(from, to));
            return;
        }
        for (long i = from; i < to; ++i)
            if ((byte) byteAt(i) != padding)
            {
                error("the bytes [%x,%x) are not part of any reachable node and are not padding; first at %x: %s", from, to, i, hex(i, to));
                return;
            }
        if (to % pageSize != 0)
        {
            error("the padding [%x,%x) does not end at a page boundary", from, to);
            return;
        }
        result.paddingBytes += to - from;
        ++result.paddingRuns;
        result.longestPaddingRun = Math.max(result.longestPaddingRun, to - from);
    }

    private void checkPages(long root)
    {
        if (pageSize <= 0)
            return;

        result.pages = (root - 1) / pageSize - dataStart / pageSize + 1;
        for (Node node : nodeList)
        {
            if (node.start / pageSize != (node.end - 1) / pageSize)
            {
                ++result.nodesCrossingPages;
                if (node.size() <= pageSize)
                    ++result.smallNodesCrossingPages;
            }
        }

        for (int group = 0; group < groupCount; ++group)
        {
            long start = groupStart[group];
            long end = groupEnd[group];
            long size = end - start;
            if (groupBytes[group] != size)
                error("the parts of the logical node [%x,%x) are not contiguous", start, end);
            long crossings = (end - 1) / pageSize - start / pageSize;
            if (size > pageSize)
            {
                ++result.oversizedLogicalNodes;
                if (crossings > (size - 1) / pageSize)
                    ++result.oversizedLogicalNodesWithExtraCrossings;
            }
            else if (crossings != 0 && enforcePages && result.layoutErrors.size() < MAX_ERRORS)
            {
                StringBuilder parts = new StringBuilder();
                for (Node node : nodeList)
                    if (node.group == group)
                        parts.append(' ').append(node);
                result.layoutErrors.add(String.format("the logical node [%x,%x) of %d bytes crosses the page boundary at %x:%s",
                                                      start, end, size, (end - 1) / pageSize * pageSize, parts));
                result.misplacedLogicalNodes.add(end);
            }
        }
    }
}
