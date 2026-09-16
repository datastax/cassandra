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
last node in the file is the root of trie, placed immediately before the end of the file.

`<node_0 data><node_0 code> <node_1 data><node_1 code> ... <root data><root code>`

The node code determines the type of node, and it also includes bits that specify length, features and/or bytes per
pointer. The six node types we currently use are detailed below.

The trie is meant to be read in reverse; constructing on-disk tries in this way greatly simplifies the construction
process. The position of a node is thus given by the file position immediately following it. For example, the
root's position is exactly the end of the file. The first byte, read in reverse from that position, gives us the node
code, and the rest of the node's data precedes it.

Children are always written before their parents, thus every pointer in the trie is encoded as a delta that is
subtracted from the position at the start of the current node's data. We use a variable number of bytes per pointer
(encoded in the node code) to store this delta. The first child of a node is written last, thus it
immediately precedes the parent's data -- in other words its delta is always 0 and does not need to be stored. 
<!-- to change for page-packed-->

### Leaf nodes (code `00nnnnnn`)

`<data (n bytes)> 00nnnnnn`

Leaf nodes are nodes that contain content and do not have further children. These are the most common nodes in every
trie.

A leaf node's code includes 6 bits of length. This determines the length `n` of the payload (between 0 and 63 bytes),
which is placed in the `n` bytes immediately before the code. The trie infrastructure takes does not itself interpret
the bytes of the content, instead relying on a pluggable deserializer.

This node type cannot handle leaves with content 64 bytes or longer. These are encoded using the generic content type.

Example: `00 00 00 01 04` could encode a 32-bit integer payload with value 1.

### Chain nodes (code `01nnnnnn`)

`<transition n> <transition n-1> ... <transition 0> 01nnnnnn`

Chain nodes encode a sequence of nodes that each have a single child. The node code's 6 bits encode the length of the
sequence minus one, to encoding 1 to 64 possible transitions.

The transitions are written in reverse order. Among the other advantages this has, it allows us to address positions
inside the chain (e.g. the state after only 1 of the transitions was taken) by supplying a combination of a node code
and data position.

Example: `33 32 31 42` encodes the transition `ABC ->` (equivalent to `A -> B -> C ->`)
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

Example: `04 01 82 00 33 32 31 85` encodes a sparse node with 3 children (10000101 has n=1 and b=1). If it resides
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

A bitmap node represents a node with more than 25 children. The child transitions are stored as bits in the bitmap
(the respective bit is set for every child transition present in the node). Each pointer is encoded using `b+1` bytes
specifying the distance from the start of this node. As before, the first pointer is an implicit 0.

Example: 
`... 01 03 80 00 22 89 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 07 FF FF FE 00 00 00 00 00 00 00 
00 E2`
encodes a bitmap node that has transitions for the 26 capital letters A-Z (0x41-0x5A). The first child immediately
precedes this node (1 + 32 + 3x25 bytes before this node's position), the second is `0x002289` bytes further towards
the front of the file, the third -- `0x010380` bytes from the position of the first child. The rest are omitted
for brevity.

### Dense nodes (code `11101bbb`)

`<pointer 255><pointer 254>...<pointer 1><pointer 0> 11101bbb`

A dense node is a node where all 256 pointers are explicitly specified. If there is no child for a specific transition
value, we use a pointer with all bits set to 1, otherwise we store it reversed with `b+1` bytes as before.

Unlike the other node types, dense nodes do not have a child with an implicit 0 pointer, because the first child in
this definition always corresponds to the 00 transition, and a dense node may be missing a child for it.

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
  descent path, with its length encoded as a variable-length unsigned integer.
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
  in the ascent-side slot. Deletion branches in turn are range tries and follow the range trie behaviour.

Example: `40 40 02 01 02 01 F7` encodes the combination of a prefix node that adds ascent and descent-side content both
encoded as a single `02` byte (for example, this could be a row-level marker), and a single-transition chain for the
0x40 transition. In other words,
```
< -> content 02
@ -> ...
> -> content 02
```

### Reserved (code `11111xxx`)

This node code is currently unused.

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
only up to some prefix of existing data). In this case we don't write anything and return -1 to the parent, signalling
that this branch is empty and it need not write a pointer for this child.

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
- We map the "e" transition to the resuling pointer 8 in the `Node` we already have at depth 2.
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
    - We write the deltas back-to-front, skipping the implied 0: `07 0B`.
    - We write the transition bytes back-to-front: `61 65 69`.
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

Also like `InMemoryTrie`, on-disk range tries track the currently applicable range and handle skips by descending 
into the branch.
