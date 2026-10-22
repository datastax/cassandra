<!---
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at
 
     http://www.apache.org/licenses/LICENSE-2.0
 
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# On-disk trie design

The on-disk representation of a trie is an immutable file that is constructed by `OnDiskTrieWriter` and read as `OnDiskTrie`. 
It implements the full trie/cursor functionality and relies on several features for efficiency:
- Multiple node types, delta encoding for pointers and variable pointer sizes.
- Node code bytes that include sizing and length for some node types and that are sometimes implicit.
- Implicit pointer value for the first child of a node.
- Bottom-to-top and back-to-front construction.
<!-- to add page-packing and metadata storage -->

The sections below will describe an on-disk trie's layout in detail, discuss the rationale behind the features above,
and detail the process of constructing a trie from a cursor, as well as the implementation of range and deletion-aware
tries.

## Layout

An on-disk trie is laid out as a sequence of nodes, where each node contains some data and ends in a "node code". The
last node written is the root of the trie; when the trie fills the file, it is placed immediately before the end of the
file. `OnDiskTrieWriter.write` returns the root's position, which is what a reader is opened with. An empty trie writes
nothing, and its root is reported as 0, which the readers take as the empty trie (no node can end at position 0).

`<node_0 data><node_0 code> <node_1 data><node_1 code> ... <root data><root code>`

The node code determines the type of node, and it also includes bits that specify length, features and/or bytes per
pointer. The six node types we currently use are detailed below.

The trie is meant to be read in reverse; constructing on-disk tries in this way greatly simplifies the construction
process. The position of a node is thus given by the file position immediately following it. For example, the
root's position is exactly the end of the file. The first byte, read in reverse from that position, gives us the node
code, and the rest of the node's data precedes it.

Children are always written before their parents, thus every pointer in the trie points backwards and is encoded as a
delta that is subtracted from a position in the node that holds it: for a child pointer, the position at the start of
the node's data; for the deletion branch pointer of a deletion-aware trie, the position at the start of its
slot (see the generic content nodes below). We use a variable number of bytes per pointer (encoded in the node code, or
for a deletion branch pointer in the slot's length) to store this delta. The first child of a node is written last,
thus it immediately precedes the parent's data -- in other words its delta is always 0 and does not need to be stored.
<!-- to change for page-packed-->

### Leaf nodes (code `00nnnnnn`)

`<data (n bytes)> 00nnnnnn`

Leaf nodes are nodes that contain content and do not have further children. These are the most common nodes in every
trie.

A leaf node's code includes 6 bits of length. This determines the length `n` of the payload (between 0 and 63 bytes),
which is placed in the `n` bytes immediately before the code. The trie infrastructure does not itself interpret
the bytes of the content, instead relying on a pluggable deserializer.

This node type cannot handle leaves with content 64 bytes or longer. These are encoded using the generic content type.

Example: `00 00 00 01 04` could encode a 32-bit integer payload with value 1.

### Chain nodes (code `01nnnnnn`)

`<transition n> <transition n-1> ... <transition 0> 01nnnnnn`

Chain nodes encode a sequence of nodes that each have a single child. The node code's 6 bits encode the length of the
sequence minus one, to encode 1 to 64 possible transitions.

The transitions are written in reverse order. Among the other advantages this has, it allows us to address positions
inside the chain (e.g. the state after only 1 of the transitions was taken) by supplying a combination of a node code
and data position.

Example: `43 42 41 42` encodes the transition `ABC ->` (equivalent to `A -> B -> C ->`)
If this example is placed on bytes 11-14 of a file, the pointer 15 points to the node; equivalently, the combination
`(code 42, pos 14)` also points to the same node, and we have the implicit positions `(code 41, pos 13)` describing the
`BC ->` transition and `(code 40, pos 12)` for the `C ->` one.

### Sparse nodes (code `1nnnnnbb`)

`<pointer n+1><pointer n>...<pointer 1> <transition n+1><transition n>...<transition 0> 1nnnnnbb`

A sparse node represents a node that has between 2 and 25 children inclusive (encoded as `n = childCount - 2` for
`n < 24`). The transition characters are explicitly listed, as well as the pointers to the children, except the first
child which has an implicit 0 pointer. Each pointer is encoded as an unsigned integer of `b+1` bytes which specifies the
distance between the start of this node and the respective child. Each pointer is stored reversed.

Note that not all codes starting with 1 encode sparse nodes; values of `n` between 24 and 31 encode other node types.
If a child of a sparse node is more than `0xFFFFFFFF` bytes away (i.e. where the delta cannot fit in 4 bytes), a sparse
node cannot be used (a bitmap node will be used instead).

Example: the trie `A -> 01, B -> 02, C -> 03` is written as `03 01·02 01·01 01·04 02 43 42 41 84`: three leaves and
a sparse node with 3 children (`10000100` has n=1 and b=0, i.e. one-byte pointers). The leaves end at positions 2, 4
and 6, and the sparse node, which starts at 6 and ends at 12, specifies:
```
A -> 6
B -> 4
C -> 2
```
All pointer targets are calculated from the position where the node starts (`12 - 6 = 6`). The first child has an
implicit 0 delta, the second has the delta `02`, and the third -- `04`.

### Bitmap nodes (code `11100bbb`)

`<pointer n-1><pointer n-2>...<pointer 1> <256-bit bitmap> 11100bbb`

A bitmap node can represent any node a sparse node cannot: one with more than 25 children, or fewer whose pointers
need more than 4 bytes. Which of it and a dense node is used is decided by their encoded sizes (see below). The child
transitions are stored as bits in the 32-byte bitmap (the respective bit is set for every child transition present in
the node). Like everything else, the bitmap is read back to front: the byte immediately before the node code holds the
bits for transitions `00`-`07`, lowest bit first, and the 32nd byte before the code those for `F8`-`FF`. Each
pointer is encoded using `b+1` bytes specifying the distance from the start of this node. As before, the first pointer
is an implicit 0.

A bitmap node is used when it is smaller than a dense node for the same children, i.e. when
`32 + (childCount - 1) * (b+1)` is less than the size of the dense node's 256 pointers, whose width can be a byte more
than the bitmap's (see below).

Example: the trie mapping each of the 26 capital letters A-Z (`41`-`5A`) to a one-byte value, `A -> 01` to
`Z -> 1A`, is written as 26 leaves (`1A 01 19 01 ... 01 01`) followed by
```
32 30 2E 2C 2A 28 26 24 22 20 1E 1C 1A 18 16 14 12 10 0E 0C 0A 08 06 04 02
00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 07 FF FF FE 00 00 00 00 00 00 00 00
E0
```
a bitmap node with one byte per pointer. The first child, "A", immediately precedes the node, "B" is 2 bytes further
towards the front of the file, and so on to "Z", 50 (`32`) bytes away. In the bitmap, `07 FF FF FE` are the bits for
transitions `58`-`5F`, `50`-`57`, `48`-`4F` and `40`-`47`; reading back, `FE` is the ninth byte before the code.

### Dense nodes (code `11101bbb`)

`<pointer 255><pointer 254>...<pointer 1><pointer 0> 11101bbb`

A dense node is a node where all 256 pointers are explicitly specified. If there is no child for a specific transition
value, we use a pointer with all bits set to 1, otherwise we store it reversed with `b+1` bytes as before.
The number of bytes per pointer is chosen so that the furthest child's delta is strictly less than the all-ones value.

Unlike the other node types, dense nodes do not have a child with an implicit 0 pointer, because the first child in
this definition always corresponds to the 00 transition, and a dense node may be missing a child for it.

Example: a trie with an empty leaf (`00`) for every transition but `00` is written as 255 leaves followed by
`FE FD ... 02 01 00 FF E8`: a dense node with one byte per pointer, which has no child for `00` (`FF`), whose `01` child
immediately precedes the node (delta `00`), whose `02` child is a byte further, and so on to `FF`, 254 (`FE`) bytes
away. Reading back from the code, the pointers are in transition order.

### Generic content nodes (code `11110dap`)

`<ascent content bytes><varint-encoded length> <descent content bytes><varint-encoded length> 11110dap`

The generic content nodes are used to store content when it is not suitable for a leaf node:
- when the node has ascent-side content
- when the node is a prefix (i.e. augments one of the node types with children)
- when the content cannot fit in 64 bytes

The generic content node has three flags specifying:
- ***d***escent-side content: if the flag is set, the node code is preceded by content which is to be presented on the
  descent path, with its length encoded as a variable-length unsigned integer. The length is Cassandra's unsigned vint
  written byte-reversed, so that its leading byte, which carries the length of the encoding, is the one next to what
  follows it and is read first. That gives 1 to 9 bytes; in the 9-byte form the leading byte is `FF` and the 8 bytes
  before it hold the full 64-bit value, reversed like the pointers.
- ***a***scent-side content: if the flag is set, the descent-side content (or the node code, if this is not present) is
  preceded by content which is to be presented on the ascent path, with its length.
- ***p***refix: if the flag is not set, the node has no children. If it is set, the node immediately preceding this node
  specifies its children.

The exact meaning of descent and ascent path is determined by the specific trie type:
- Plain tries do not contain ascent-side data (unordered ones always present content on the descent path, and ordered
  ones present it on the ascent path when walked in reverse).
- Range tries may have both ascent- and descent-side content. When the trie is walked in the forward direction, the
  two are presented according to the name; when it is walked in reverse, the two are swapped.
- Deletion-aware tries do not have ascent-side data in their live path, but instead store pointers to deletion branches
  in the ascent-side slot. Like the child pointers, the pointer is a delta, subtracted from the position where the slot
  begins (i.e. the start of the node's payload), and is stored as a minimal-width reversed integer whose width is given
  by the slot's length. Deletion branches in turn are range tries and follow the range trie behaviour. Because the
  ascent-side slot is taken, the live data of a deletion-aware trie can only have descent-side content; the writer
  rejects a trie that presents live content on the return path.

Example (a fragment: the chain's child, which would precede it, is left out): `40 40 02 01 02 01 F7` encodes the
combination of a prefix node that adds ascent and descent-side content both encoded as a single `02` byte (for example,
this could be a row-level marker), and a single-transition chain for the 0x40 transition. In other words,
```
< -> content 02
@ -> ...
> -> content 02
```

A plain trie only has the descent side: `a -> 01, ab -> 02` is written as `02 01·62 40·01 01 F5·61 40`, where `01 01 F5`
is a prefix node with the descent-side content `01` and a child, the chain for `b` immediately before it.

In a deletion-aware trie, the trie with the live value `07` at `a` and a deletion of `[ab, ac)` (written as a deletion
branch at `a`, whose markers at `b` and `c` serialize as two bytes) is written as
`05 FF 02·FF 05 02·03 63 62 80·00 01 07 01 F6·61 40`. The first three nodes are the deletion branch, a range trie
whose root, a sparse node, ends at position 10. The node at `a` is the prefix node `00 01 07 01 F6`: code `11110110` has
both content flags and no child; reading back, the descent-side content is `07`, and the ascent-side slot holds the
one-byte delta `00` -- the slot begins at position 10, exactly where the branch root is.

### Reserved (code `11111xxx`)

This node code is currently unused. It is kept for a page-packed layout, and readers reject it as corruption, so that
a trie written with a future layout fails to read rather than being misread.

### Versioning

The format carries no in-band version. The format that contains a trie -- the messaging version of a message, the
commit-log version of a record -- determines how it is written, and every change to this format must be gated by a
change of that containing version. The reserved node codes `11111xxx` are what lets a reader built for this layout
reject, rather than misread, a future page-packed one.

### Example

The
```
tr ->
  actor -> 01
  ee -> 02
  ie -> 03
```
example from `InMemoryTrie.md` encodes as
`03 01·65 40·02 01·65 40·01 01·72 6F 74 63 43·0B 07 69 65 61 84·72 74 41`.  
(The middle dots · are placed for clarity at the boundaries between nodes.)

The root of this trie is at position 24 and is a chain node with 2 transitions. It leads to a sparse node at position
21, which has 3 children. The first child (at position 15) is reached with "a", the second child (at position 8) -- 
with "e", and the third child (at position 4) -- with "i".

Each of these children contain a chain (respectively with 4, 1 and 1 transitions) and a leaf node (at positions 10, 6
and 2) with their respective payload.

## Features

### Node types

Using multiple node types is standard in modern trie/radix tree implementations, done to improve the space usage of the
structure, as well as the lookup performance which is often determined by the space usage because of caching. Our
choice of types is driven primarily by the size of the resulting entry in the file, but the choice also naturally
prefers types that are also more time efficient when the data becomes dense.

Generally, unlimited-length sparse nodes are sufficient to implement tries space-efficiently, but on skips (i.e. slices
/ point queries) they require binary search to find the child to descend to. Binary search is known to have poor branch
predictability and thus sparse nodes become inefficient when the number of children is large, and is often replaced 
with linear search for small numbers of children. This is one of the reasons we limit the number of children for 
which we choose the sparse type.

We further decide between the bitmap type (where a skip involves fixed-time bit-counting with no branching) and the 
dense type (where a skip is a direct index calculation) based on the size of the serialization on disk.

Single-child nodes are grouped into child nodes for two reasons:
- to save space by using a single node code for multiple transitions,
- to naturally support the `advanceMultiple` operation of trie cursors.

Finally, while we could store all content with the generic content type, the leaves of a trie are the most numerous
nodes and thus the one-byte saving we achieve by having a dedicated leaf type has a significant effect on the size of
the resulting files.

### Reverse file layout

One of the objectives of the file format was to write the serialization quickly and easily, with limited 
intermediate state that needs to be maintained. To achieve this, we write nodes in the order in which they are 
completed. Because we perform construction from a trie cursor, this is done by walking the trie in depth-first order
and thus the nodes we complete first are leaves. By the time we complete a parent node, all its children are already 
written, and we can immediately write the parent as well.

This achieves a kind of reversed structure of the file, where the root comes somewhat unexpectedly at the end. We do 
two further simple tricks to embrace this layout:
- placing the node code as the last byte of the node's serialization in the file and using the position after the 
  serialization as the node pointer,
- walking the trie in reverse order, so that the first child is placed closest to its parent.

If we were to reverse the file bytes, it would look very natural: the root is at the beginning, its first child 
immediately after, and so on. However, we don't need to physically do the reversal as we can interpret it just as 
easily when it is left as is.

### Sized delta-encoded pointers and implicit first child

As an immediate consequence of the reverse file layout, we have that the first child of a node immediately precedes it.
Similarly, all children of a node are placed as close as possible to it, and since most nodes in a trie
are at its lowest levels, closest to the leaves, most children and parents are placed very close together.

It thus makes a lot of sense to store pointers in the trie as deltas from the position of the parent, as these deltas
are going to be very small for most nodes in the trie. To make the deltas as small as possible, we take them from the
file position immediately before the parent's serialization to the child pointer, i.e. the file position immediately
after the child's serialization. This naturally maps the first child pointer to the delta 0, which we can now omit 
from the serialization of the parent.

This is especially helpful for the chain and prefix type, which do not need to store a pointer at all, but also 
reduces the size of the sparse and bitmap node types.

We can't use variable-sized integers to store pointers, because we want to be able to read a specific pointer 
without decoding the ones that precede it. Instead, we select a number of bytes to use for each node based on the 
furthest child's position and store it as part of the node code.

## Construction

On-disk trie construction is done by `OnDiskTrieWriter`. The procedure is the classic incremental trie construction
mechanism, with a couple of quirks.

The trie is walked depth-first in reverse order, maintaining the path we took, as well as a write-time representations
of the information we have collected for nodes on the current path. Once we enter a node, we save its content in 
this representation. We then walk its deletion branch (if this is a deletion-aware trie and there is one) and its 
children recursively. This gives us a pointer for each child. If the recursion stops on the ascent path, we store 
the ascent-side content as well. When we ascend back to a smaller depth, the node's information is fully collected.

If the collected node has no children, but has content, we write a leaf or non-prefix generic content node (using the
configured serializer for the content). Otherwise, we use the number of children, as well as their pointers (for 
sizing), to decide on the type of child-carrying node to use. We write this to the file, using the current file position
as a base for the deltas we construct for the child pointers. If a prefix node is needed to augment this 
with content, we write one to the file as well. If the ascent that completed the node takes us beyond the immediate 
parent level, we create a chain node with the remaining transitions taken to reach this node's level from the 
closest parent's. If no parent exists, or its depth is smaller than the ascent depth minus one, we create a new parent 
node.

Because chain and prefix nodes have their child/augmented node immediately before them in the file, the
writing method above gives us a valid layout. The file position after we have completed this writing is returned 
back to the parent as the child pointer.

Note that it is possible to see nodes with no children and no content (e.g. as the result of intersection that matched
only up to some prefix of existing data). Such a node is not created, or not written: nothing is written for that
branch, and no child is recorded for it in the parent.

For the example above, the construction proceeds like this:
- We descend along "trie", saving the bytes into the current path array.
- We create a `Node` object to collect the content "03" at depth 4.
- The cursor tells us to ascend to depth 3, character "e". As this is above depth 4, the last `Node` is now complete:
  - It has no children and small descend-side content, thus we can use a leaf. We serialize the content and write 
    the node code for content of one byte, yielding `03 01`.
  - The ascent depth is 3, which is lower than the node's. This means that we need to create a chain node with the 
    last one character of the path, resulting in `65 40`.
  - The current file position is 4, which the recursion returns.
- There is currently no node at depth 2 (the ascent depth minus one), thus we create one and map the "i" 
  transition to the pointer 4.
- We cut the path array to length 2 and then add "e".
- The cursor descends with one "e" to depth 4.
- We create a second `Node` to collect the content "02" at depth 4.
- The cursor tells us to ascend to depth 3 again, with the character "a". Taking the same steps as above, we write 
  `02 01 65 40` and return 8.
- We map the "e" transition to the resulting pointer 8 in the `Node` we already have at depth 2.
- We cut the path array to length 2 and then add "a".
- The cursor descends with "ctor" to depth 7.
- A new node is created to store the content "01".
- The cursor tells us to ascend to the exhausted state:
  - As this is above depth 7, we follow the steps above to write the leaf `01 01` and four-character chain `72 6F 74 
  63 43`, and to return 15.
  - We find another node below the ascend depth (at depth 2) and attach the resulting position 15 for the character 
    "a" in this parent node.
  - The ascent depth marks this node as complete, thus we must write it:
    - We calculate the deltas from the current file position (15) for the three child pointers: 11, 7 and 0.
    - The node has 3 children, and all of the deltas fit in one byte. We select a sparse node with n=1 and b=0.
    - We write the deltas in the order the children were written, skipping the implied 0 of the last: `0B 07`.
    - We write the transition bytes in the same order: `69 65 61`.
    - We write the node code: `84`.
    - There is no further parent, and the ascent depth (0 for an exhausted cursor) is lower than the node's, thus we 
      write the two character chain `72 74 41` and return the file position 24.
- The root of the trie is at the end of the file, at position 24.

## Consuming

Consuming a trie is by creating an `OnDiskTrie` over a given file, which in turn will create an `OnDiskCursor` to 
present the file's contents. Because a file is open while a cursor is walking it, this necessitates that cursors must
be closeable.

Like `InMemoryTrie`, we maintain as little information as possible while we perform a walk; we don't store the full 
path used to reach the current position, but only backtracking positions. I.e. node codes and file positions of the 
nodes that have further children, as well as either the index or the transition byte of the next child (whichever 
is more efficient for the node type).

Content that is to be presented on the return path is recorded as a backtracking entry as well, and, as in
`InMemoryTrie`, at the depth of the parent: on the parent's level the return path of a child comes after the descent
into it and before the next sibling, which is where a skip to that return-path position must find it.

Also like `InMemoryTrie`, on-disk range tries track the currently applicable range and handle skips by descending 
into the branch.

Corrupt input -- a damaged commit-log record or message payload -- is reported as an `UncheckedIOException` wrapping
an `IOException`. Besides lengths, the reader checks that every child it descends to ends at or before the start of the
node pointing to it, so that every walk moves towards the start of the data and terminates. Terminating is all that
is guaranteed, not work proportional to the size of the data: two pointers may legally point at the same earlier node,
so crafted input can make a walk take a number of steps exponential in its size.

## Range and deletion-aware tries

Range tries, deletion branches among them, are always written ordered: a boundary that is presented before a node's
branch is stored on the node's descent side and one presented after it on the ascent side. A range cursor also
reports the range state at its position. It follows it from the boundaries it passes, and after a skip, which can land
inside a range, works it out from the nearest content below the position it landed on. A tail of a range trie presents
at its root, as boundaries, the ranges that are open there.

A deletion-aware trie is a data trie written unordered, with live content on the descent side only, whose nodes can
point to a deletion branch: a range trie written in the same stream just before the node, whose root the node's
ascent-side slot gives as a delta from the start of the slot (see the generic content nodes above). The reader flags
such a node with `MAY_HAVE_DELETION_BRANCH_BIT` from the node code alone, and only reads the slot when the cursor is
asked for the branch.
