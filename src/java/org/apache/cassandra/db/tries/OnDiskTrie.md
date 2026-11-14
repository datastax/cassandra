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
- A choice of layouts: nodes can be written as soon as they are complete, which is the fastest way to serialize a 
  trie, or be packed into the pages of the file, which minimizes the number of pages that reading a path through the 
  trie has to touch.
<!-- to add metadata storage -->

The sections below will describe an on-disk trie's layout in detail, discuss the rationale behind the features above,
and detail the process of constructing a trie from a cursor, as well as the implementation of range and deletion-aware
tries.

## Layout

An on-disk trie is laid out as a sequence of nodes, where each node contains some data and ends in a "node code". The
last node written is the root of the trie; when the trie fills the file, it is placed immediately before the end of the
file. `OnDiskTrieWriter.write` returns the root's position, which is what a reader is opened with. An empty trie writes
nothing, and its root is reported as 0, which the readers take as the empty trie (no node can end at position 0).

`<node_0 data><node_0 code> <node_1 data><node_1 code> ... <root data><root code>`

When nodes are packed into pages, runs of padding can separate them; no pointer leads into padding.

The node code determines the type of node, and it also includes bits that specify length, features and/or bytes per
pointer. The seven node types we currently use are detailed below.

The trie is meant to be read in reverse; constructing on-disk tries in this way greatly simplifies the construction
process. The position of a node is thus given by the file position immediately following it. For example, the
root's position is exactly the end of the file. The first byte, read in reverse from that position, gives us the node
code, and the rest of the node's data precedes it.

Children are always written before their parents, thus every pointer in the trie points backwards and is encoded as a
delta that is subtracted from the position at the start of the current node's data. This includes the deletion branch
pointer of a deletion-aware trie (see the generic content nodes below). We use a variable number of bytes per pointer
(encoded in the node code, or for a deletion branch pointer in the slot's length) to store this delta. The first child
of a node is normally written last, thus it immediately precedes the parent's data -- in other words its delta is
always 0 and does not need to be stored.

When the nodes are packed into pages, a parent can end up being placed away from its first child. In this case the
parent is immediately preceded by a relay node, which takes the first child's place and supplies its position.

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

A sparse node represents a node that between 2 and 25 children inclusive (encoded as `n = childCount - 2` for `n < 24`).
The transition characters are explicitly listed, as well as the pointers to the children, except the first child which
has an implicit 0 pointer. Each pointer as encoded as an unsigned integer of `b+1` bytes which specifies the distance
between the start of this node and the respective child. Each pointer is stored reversed.

Note that not all codes starting with 1 encode sparse nodes; values of `n` between 24 and 31 encode other node types.
If a child of a sparse node is more than `0xFFFFFFFF` bytes away (i.e. where the delta cannot fit in 4 bytes), a sparse
node cannot be used (a bitmap node will be used instead).

Example: `04 01 82 00 43 42 41 85` encodes a sparse node with 3 children (10000101 has n=1 and b=1). If it resides
(i.e. ends) on position `0x20E`, then the node specifies:
```
A -> 0x206
B -> 0x184
C -> 0x102
```
All pointer targets are calculated from the position to the left of the node (`0x20E - 8 = 0x206`). The first child has
an implicit 0 delta, the second has the delta `0x0082`, and the third -- `0x0104`.

### Bitmap nodes (code `11100bbb`)

`<pointer n-1><pointer n-2>...<pointer 1> <256-bit bitmap> 11100bbb`

A bitmap node is used to represent any node a sparse node cannot -- one with more than 25 children, or fewer whose
pointers need more than 4 bytes -- unless a dense node is no larger (see below). The child transitions are stored as
bits in the 32-byte bitmap (the respective bit is set for every child transition present in the node). Like everything
else, the bitmap is read back to front: the byte immediately before the node code holds the bits for transitions
`00`-`07`, lowest bit first, and the 32nd byte before the code those for `F8`-`FF`. Each pointer is encoded using `b+1`
bytes specifying the distance from the start of this node. As before, the first pointer is an implicit 0.

Example:
`... 80 03 01 89 22 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 07 FF FF FE 00 00 00 00 00 00
00 00 E2`
encodes a bitmap node that has transitions for the 26 capital letters A-Z (0x41-0x5A). The first child immediately
precedes this node (1 + 32 + 3x25 bytes before this node's position), the second is `0x002289` bytes further towards
the front of the file, the third -- `0x010380` bytes from the position of the first child. The rest are omitted
for brevity.

### Dense nodes (code `11101bbb`)

`<pointer 255><pointer 254>...<pointer 1><pointer 0> 11101bbb`

A dense node is a node where all 256 pointers are explicitly specified. If there is no child for a specific transition
value, we use a pointer with all bits set to 1, otherwise we store it reversed with `b+1` bytes as before.
The number of bytes per pointer is chosen so that the furthest child's delta is strictly less than the all-ones value.

Unlike the other node types, dense nodes do not have a child with an implicit 0 pointer, because the first child in
this definition always corresponds to the 00 transition, and a dense node may be missing a child for it.

A bitmap node is used when it is smaller than a dense node for the same children, i.e. when
`32 + (childCount - 1) * (b+1)` is less than the size of the dense node's 256 pointers, whose width can be a byte more
than the bitmap's. This can be the case even when there is a child for every transition, so a bitmap node can have
up to 256 children.

Example: `... 00 00 00 00 FF FF FF FF EB`
encodes a dense node that has 4 bytes per pointer, that has no child for 00 and whose 01 child immediately precedes the
node (1 + 256x4 bytes before this node's position). The other transitions are omitted from the example.

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
  specifies its children. That node is of one of the types that list children (chain, sparse, bitmap or dense), or a
  relay to one (see below). It cannot be a leaf or another generic content node, which would give the node a second
  set of content; readers reject these as corrupt.

The exact meaning of descent and ascent path is determined by the specific trie type:
- Plain tries do not contain ascent-side data (unordered ones always present content on the descent path, and ordered
  ones present it on the ascent path when walked in reverse).
- Range tries may have both ascent- and descent-side content. When the trie is walked in the forward direction, the
  two are presented according to the name; when it is walked in reverse, the two are swapped.
- Deletion-aware tries do not have ascent-side data in their live path, but instead store pointers to deletion branches
  in the ascent-side slot. Like a child pointer, the pointer specifies the distance between the start of this node,
  which is where the ascent-side slot begins, and the root of the deletion branch. It is stored reversed in as few
  bytes as it needs, and the slot's length gives their number. A node with no children can have its deletion branch
  immediately before it; the delta is then 0 and takes no bytes, i.e. the slot is empty, with length 0. Deletion
  branches in turn are range tries and follow the range trie behaviour. Because the ascent-side slot is taken, the live
  data of a deletion-aware trie can only have descent-side content; the writer rejects a trie that presents live
  content on the return path.

Example: `40 40 02 01 02 01 F7` encodes the combination of a prefix node that adds ascent and descent-side content both
encoded as a single `02` byte (for example, this could be a row-level marker), and a single-transition chain for the
0x40 transition. In other words,
```
< -> content 02
@ -> ...
> -> content 02
```

Example: `34 12 02 07 01 F7` encodes a node of a deletion-aware trie with the live content `07` and children (given by
the node immediately preceding it), whose deletion branch's root is `0x1234` bytes before the start of this node.
Without children, the deletion branch would immediately precede the node, which would be encoded as `00 07 01 F6`.

### Relay nodes (code `11111bbb`)

`<pointer> 11111bbb`

A relay node stands in for a node that is placed elsewhere in the file. It holds only a pointer, encoded as an unsigned
integer of `b+1` bytes (stored reversed like all other pointers) which specifies the distance between the start of the
relay node and the node it stands for. A relay is not a node of the trie: it does not take part in any transition and 
is fully equivalent to its target, and readers follow it as soon as they see its code. The target is a node, not
another relay: nothing is gained by one relay leading to another, and readers reject it as corrupt.

Relays make it possible to rely on the implicit position of a node, i.e. on the first child of a chain, sparse or 
bitmap node and on the node augmented by a prefix node being placed immediately before it, even when a different 
placement is better or necessary. The page-aware construction below uses them to put a parent in a different page 
than its first child. Dense nodes store an explicit pointer for every child and never need one.

Example: `... 11 01 F9 03 62 61 80` ending at position `0x1235` is a sparse node with two children preceded by a 
relay with a two-byte pointer. The sparse node starts at `0x1231`, where it expects to find its first child `a`. This
is the position of the relay, which starts at `0x122E` and gives the child's actual position as 
`0x122E - 0x0111 = 0x111D`. The second child `b` is at `0x1231 - 0x03 = 0x122E`, immediately before the relay.

### Versioning

The format carries no in-band version. The format that contains a trie -- the messaging version of a message, the
commit-log version of a record -- determines how it is written, and every change to this format must be gated by a
change of that containing version. Relay nodes are only written by the page-aware construction; the node code 
`11111xxx` was reserved before they were introduced, so a reader built for the earlier layout rejects a trie that 
contains them as corrupt rather than misreading it.

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

On-disk trie construction is done by `OnDiskBaseTrieWriter` and its subclasses, `OnDiskTrieWriter` for plain and range
tries and `OnDiskDeletionAwareTrieWriter` for deletion-aware ones. The procedure is the classic incremental trie
construction mechanism, with a couple of quirks.

The trie is walked depth-first in reverse order, maintaining the path we took, as well as a write-time representations
of the information we have collected for nodes on the current path. Once we enter a node, we walk its deletion branch 
(if this is a deletion-aware trie and there is one) and save it in this representation together with the node's
content. We then walk its children recursively. This gives us a reference for each child. If the recursion stops on 
the ascent path, we store the ascent-side content as well. When we ascend back to a smaller depth, the node's 
information is fully collected.

What happens to a node whose information is complete is decided by an `OnDiskNodeWriter`, which the trie writer is 
given when it is created. The trie writer hands every completed node to the node writer together with the chain of 
single-child transitions that leads to it from its parent, i.e. from the closest shallower node on the path or from 
the depth the walk has ascended to, whichever is deeper (the trie writer creates the parent if the path has no node at 
that depth). It then asks the node writer to attach the completed node to that parent as a child, giving it the 
transition that leaves the parent. The node writer is the only component that writes to the file, and it alone 
determines the order and placement of the nodes; the encoding of the nodes is the same regardless of the node writer 
in use, and all resulting files are read by the same code. There are two implementations, selected by 
`OnDiskNodeWriter.Type`:
- `OnDiskNodeWriterImmediate` writes each node at the time it is completed. This is described first below.
- `OnDiskNodeWriterPageAware` holds on to completed nodes to arrange them in the pages of the file. This is described 
  in the next section.

A deletion branch is a trie of its own, with a different type of content, and is walked by a trie writer of its own 
when the walk of the main trie reaches the node it belongs to. Its placement in the file, however, is part of the 
layout of the main trie. The node writer for the deletion branches is thus created by the main node writer as a 
_nested_ one, and a deletion branch whose walk has completed is attached to its node in the main trie in the same way 
as a completed child. The pointer to its root is stored in the node when the node is written.

### Immediate construction

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

### Page-aware construction

Files are read in pages, and the cost of a lookup in a trie that is not fully cached is determined by the number of
different pages that need to be fetched to follow a path from the root. The immediate construction pays no attention 
to this; while it keeps the lowest levels of the trie close together, it lets nodes and branches straddle page 
boundaries wherever they happen to fall. `OnDiskNodeWriterPageAware` produces a layout where a branch of the trie, 
i.e. a node together with everything below it, sits within a single page whenever it is small enough to fit in one. 
This is the same method as the one used for the trie indexes of the BTI sstable format (see `BtiFormat.md`), adapted 
to the features of this format.

To do this, the page-aware node writer does not write nodes when they are completed, but keeps them in memory, 
attached to their parents, for as long as the branch that they form is no bigger than a page. When a node is 
completed whose branch is larger than a page, the node writer _lays out_ its children:
- It takes the child with the largest branch that still fits in the remainder of the current page, and writes that 
  branch as the immediate construction would: all buffered descendants first, in order, then the child itself.
- If none of the children fits, it pads the file to the next page boundary and continues with the largest child.
- This is repeated until all children are written.

The node itself remains in memory, now with the positions of its children, and will be written when its own parent 
is laid out, or as part of a branch which has become small enough to fit a page with it. In the resulting file the 
pages typically hold collections of sibling branches, and the upper levels of the trie end up in pages of their own.

A node whose children were laid out is no longer written immediately after its first child. When such a node is 
written, the node that lists its children is preceded by a relay with the position of the first child. (The decision 
is made by position: a relay is written whenever the first child does not end exactly where the node begins, which 
also covers the case of padding between the two.)

A node with only one child is normally written as a single-transition chain node, augmented by a prefix node if it 
has content or a deletion branch. If the child is still in memory when the parent is completed and itself begins with 
a chain, the page-aware node writer adds the transition to the child's chain instead, saving a byte and a step for the
readers. 

Note that the immediate construction does not do this _chain folding_. By the time it finds out that a node has no 
other children, the chain of the child is already written, including the node code that carries its length. It would
be possible if a child left the code of its leading chain for the parent to write, which would then be able to extend
the chain, but this is not worth the complication.

A node can also end up with a single child and nothing else, i.e. neither content nor a deletion branch. The writer of
the trie creates one when the source walks into a branch that turns out to hold no content (as an intersection does
along the boundaries of its set) after it has completed a sibling of that branch. Such a node is only a stretch of the
path to its child. If the child is still in memory, the page-aware node writer does not keep the node at all: it adds
the transitions that lead to the node and the one from the node to the child to the front of the child's chain, which
then takes the node's place. The result is what would be written if the empty branch were not there. Besides saving
space, this is necessary: if the transition were folded as above, the node would have nothing to write but a relay
whenever the child is written away from it, and a relay in front of the node's parent would then lead to that relay.
(When the child is already written, the node is a chain node for its transition, like in the immediate construction.)

A deletion branch is handled as one more child of the node it belongs to. It is kept in memory with the node for as 
long as the branch of the node, deletion branch included, fits a page, in which case it is written just before the 
node's other children; otherwise it is laid out together with them. As a result a deletion branch is placed in the 
same page as its node whenever the branch of the node, deletion branch included, fits in one, in which case reading a 
path through the node does not have to fetch another page for the deletions that apply to it. While a deletion branch 
is being walked it is treated like any other trie, i.e. its nodes are held in memory unless it grows bigger than a 
page, when its children are laid out as described above.

#### Sizes

To know whether a branch fits in a page, the node writer needs to know its size before writing it. This takes some 
care because the size of a pointer depends on the distance it needs to cover: while all distances inside a branch that 
is fully in memory are known in advance, the pointers to children and deletion branches that have already been written
grow as the file position advances. The size of a branch that has either is thus a function of the position at which 
it would be placed.

The node writer calculates sizes by performing the same steps as it would do to write, and stores them in the nodes 
together with the range of positions for which they remain valid, i.e. up to the closest position at which some pointer
in the branch would need an extra byte. The node type, pointer size and need for a relay are decided by the same 
code for both sizing and writing, which makes the calculated sizes exact. To know the size of a node's content, the
node writer serializes it at the time the node is completed, and keeps the serialized form until the node is written.

#### Exceptions

Some nodes cannot be placed inside a page, because they are bigger than one. This can be due to content that is 
larger than the page size, or (when pages are small) due to the number and size of pointers in the node. Such nodes 
are written as soon as their children are laid out, starting a new page if this reduces the number of page boundaries 
that the node crosses. Because a node is read by stepping backwards from its position, this requires file positions 
to be continuous across page boundaries, which is not the case for all types of file (e.g. encrypted ones); writing a 
node that is larger than a page to such a file fails.

When the walk of the trie completes, the part of the trie that remains in memory is the root node with the top of the 
trie, which must be written out for the root position to be known. To keep it in a single page, the file is padded if
necessary.

The writing and sizing of a branch are recursive, and the depth of a branch that fits in a page is only limited by
the page size. To make sure the recursion cannot exceed the stack, a branch whose depth of in-memory nodes exceeds a
fixed limit is also laid out, in the same way as if it had become bigger than a page.

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

A relay is a node type of its own for the reader, whose only action is to move on to its target as soon as it is
loaded, be it as a node the cursor descends into or as the node that follows a prefix node. A cursor is thus never
left on a relay: neither the node it is positioned on (which is what a tail trie is rooted at, and where the pointer
to a deletion branch is looked for), nor the node whose children it is iterating, nor any backtracking entry is one.
In the first case the relay's target also becomes the node the cursor is positioned on; in the second that remains
the prefix node.

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
point to a deletion branch: a range trie written in the same stream before the node, whose root the node's
ascent-side slot gives as a delta from the start of the node (see the generic content nodes above). The reader flags
such a node with `MAY_HAVE_DELETION_BRANCH_BIT` from the node code alone, and only reads the slot when the cursor is
asked for the branch.
