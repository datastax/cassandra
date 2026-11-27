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

# Pattern matching on tries

`Trie.patternMatched(TriePattern)` returns a view of a trie that only presents the content whose keys match a pattern.
It is used to serve SQL `LIKE` queries from the terms dictionaries of storage-attached indexes, whose keys are the
UTF-8 encoded bytes of the indexed `text` and `ascii` values.

The obvious way to do this is to walk the whole trie and check every key against the pattern. The approach described
here matches the pattern while the trie is walked, one byte at a time, which lets the walk skip every branch that cannot
contain a match. For example, matching `alpha/*/report????.csv` only visits the branch starting with `alpha/`, and below
it only the branches that can still be followed by `report`, four characters and `.csv`.

## Patterns

A `TriePattern` is a sequence of elements, each of which matches
- a literal character,
- any single character (`?` in glob syntax, `_` in `LIKE` syntax), or
- any sequence of characters, including the empty one (`*` in glob syntax, `%` in `LIKE` syntax).

A key matches if all of it matches the pattern. Patterns and keys are UTF-8 encoded: the builder rejects literals that
are not valid UTF-8, and the matchers rely on the keys being valid UTF-8, without checking it. Literals are never
interpreted as wildcards; parsing a syntax with escapes is up to the creator of the pattern. `TriePattern.glob` parses
glob patterns without escapes, and `LikePatterns.forPatternOperator` translates `LIKE` patterns, with their `%` and `_`
wildcards and `\` escapes.

## Matching a key with a set of positions

Number the elements of a pattern of length `n` from `0` to `n-1`, and call position `i` the point in the pattern
before element `i`. Position `n` is the end of the pattern. While a key is read, the state of the match is the set of
positions that the bytes read so far can reach. The key matches if, after its last byte, the set contains position `n`.

The state starts as `{0}`. To read a byte `b`, every position `i` in the current state whose element accepts `b`
contributes position `i + 1` to the next state. Some elements, the _loops_, can be repeated any number of times,
including zero:
- if element `i` is a loop that accepts `b`, position `i` also stays in the state (the loop takes another byte);
- whenever position `j` of a loop is added to the state, position `j + 1` is added too (the loop is skipped).

For example, for the pattern `a*b` the positions are `0` (before `a`), `1` (before `*`), `2` (before `b`) and `3` (the
end). Reading the key `axb`:

| byte | state      | notes                                                      |
|------|------------|------------------------------------------------------------|
|      | `{0}`      |                                                            |
| `a`  | `{1, 2}`   | `a` takes `0` to `1`, `1` is a loop so `2` is added        |
| `x`  | `{1, 2}`   | `*` accepts `x` and stays at `1`, also advancing to `2`    |
| `b`  | `{1, 2, 3}`| `*` stays and advances, `b` takes `2` to `3`               |

The final state contains `3`, so the key matches. Note that the state holds all the ways the bytes read so far can be
matched at the same time, so the matcher never backtracks: reading a byte takes time proportional to the length of the
pattern, regardless of how many stars it has. (This is the classic simulation of a nondeterministic automaton, whose
states are the positions of the pattern.)

### Elements

The elements are classes of bytes, some of which are loops:

| Element             | Accepts                                       | Loop |
|---------------------|-----------------------------------------------|------|
| literal `b`         | `b`                                           | no   |
| any string          | all bytes                                     | yes  |
| character start     | `0x00`-`0x7F` and `0xC0`-`0xFF`               | no   |
| continuation bytes  | `0x80`-`0xBF`                                 | yes  |

A literal character is the sequence of literal bytes of its encoding. A UTF-8 character is one byte that is not a
continuation byte followed by its continuation bytes, so the any-character wildcard is expanded to a character start
followed by a continuation loop. This matches exactly one character, whatever the length of its encoding, provided that
the loop takes all the continuation bytes of the character (see below).

### Normalization

As the literals are valid UTF-8, the wildcards are on character boundaries: each one follows whole characters, and is
followed by the start of a character or the end of the key. `TriePattern.Builder` normalizes the pattern so that the
matchers can rely on a few properties:
- Runs of any-string wildcards are collapsed, and any-character wildcards are moved before them: `*?` becomes `?*`, as
  both match one or more characters. An any-string wildcard is therefore always followed by a literal or the end of
  the pattern.
- A character that is followed by an any-string does not need its continuation loop, as the any-string takes the
  continuation bytes.

As a result two loops are never adjacent, so skipping a loop never lands on another loop: adding `j + 1` for a loop at
`j` never has to be repeated.

The continuation loop of a character must take all of the character's continuation bytes, otherwise the element after
it could take some of them and the any-character wildcard would match part of a character. It does, because the
element after the loop is a literal that starts a character, another character start or the end of the pattern, none
of which accepts a continuation byte.

### Using the kind of each byte

The rules above look at one byte at a time, so the state can hold positions that the next byte cannot take. As the
keys are valid UTF-8, the kind of a byte tells where it is in its character: an ASCII byte (`0x00`-`0x7F`) is a whole
character, a lead byte (`0xC0`-`0xFF`) starts a character of two to four bytes, and a continuation byte (`0x80`-`0xBF`)
continues one. The matchers use this to leave out the positions that cannot lead to a match:
- When a character start takes an ASCII byte, the character is complete. Its continuation loop would be left at the
  next byte, which is not a continuation byte, so the loop is not entered: the position goes directly past it.
- When a character start takes a lead byte, continuation bytes must follow, and only the continuation loop (or the
  any-string that replaces it, see above) can take them. The loop is entered, but not skipped.
- A continuation byte does not tell whether the character ends with it, so a continuation loop that takes it both stays
  and is left, as the general rules say.

The positions left out could only take bytes that cannot come next in a valid key, so no valid key matches
differently. But the walks of a trie (below) work better with fewer positions: when the state is a single literal, the walk can go directly to
the child with that byte instead of visiting each child. For example, for `alpha/????/node/*` and a path with four
ASCII characters after `alpha/`, the state is the position of the `/` that follows, while under the general rules it
would also hold the continuation loop of the last `?`, a wildcard.

## Matching a trie

The state reached by a key prefix only depends on that prefix, so when the trie is walked, the state at a node can be
computed from the state at its parent and the byte that leads to the node. A `PatternMatcher` keeps the state at every
depth of the current path, and `descend(depth, byte)` computes the state at a depth from the state at the previous
one. The walk can move on to a sibling or to any ancestor's sibling, because their parents' states are still in the
history.

`PatternCursor` wraps the cursor of the source trie and uses the states as follows:
- If the state at a node is empty, no key in the node's branch can match, and the cursor skips the whole branch.
- If the state contains the end position, the node's content is presented; otherwise it is hidden.
- If the state at a node contains only literal positions, only their bytes can follow. If there is just one, the cursor
  descends to the child with that byte directly (using `skipTo`) instead of going to the first child. When a child is
  rejected, the cursor skips to the next byte that any position in the parent's state accepts, instead of visiting each
  sibling in turn.
- When the pattern ends in an any-string wildcard and its position is reached, every key in the branch matches. The
  cursor stops tracking the state until the walk leaves the branch, and a tail cursor taken in the branch is simply the
  source's tail cursor.
- While the state contains an any-string wildcard, no branch can be ruled out, and the walk is as expensive as a full
  walk of the source. The cursor then uses the source's `advanceMultiple`, which can move over chains of single-child
  nodes in one step, and receives the bytes of the chain through `TransitionsReceiver` to keep the states up to date.
  Without an any-string, the cursor advances one step at a time, so that it can return to skipping as soon as only
  literal positions are active.
- In reverse direction, ordered tries present the content of prefixes on the return path, after the node's children.
  The cursor does not descend from such a position, as the children have already been visited.

A tail cursor (`Cursor.tailCursor`) continues matching the pattern from the state of the node it starts at: the
matcher's `tail(depth)` creates a new matcher whose initial state is the state at that depth.

## Matching an on-disk trie

The matcher does not depend on cursors, and `org.apache.cassandra.io.tries.PatternValueIterator` uses it in the same
way to list the payloaded nodes of an on-disk trie whose keys match a pattern, in forward or reverse order. It walks
the nodes with a stack of positions, each with the index of the child being visited, and the matcher's state history
follows the depth of the stack:
- A child is only entered if the state for its transition byte is not empty. When a child is rejected, the walk
  continues from the child that holds the next byte the parent's state can accept, found with the node's `search`
  instead of examining each child in turn.
- When the state of a node is a single literal position, the only child to visit is found with `search`.
- Below the point where a trailing any-string wildcard matches, the state is not tracked and every payload is
  listed.
- In forward order the payload of a node is listed when the walk descends to it, and in reverse order when it ascends
  from it, after its children.

SAI uses it through `PatternTermsDictionaryReader` to serve `LIKE` queries from the on-disk terms dictionaries, as
`Trie.patternMatched` does for the memtable index.

## Matching single values

`TriePattern.matches` checks a single value with the same state machine: it starts from the initial state and reads
the value's bytes in turn, stopping early when the state becomes empty or a trailing any-string wildcard is reached. It
does not use the matcher's state history, so one pattern can check values concurrently. `Operator.LIKE_MATCHES` uses
it when rows are filtered with a `RowFilter` (e.g. by the coordinator's replica filtering protection). SAI uses it to
check every row it reads, as it does for all its restrictions: an index can return rows whose current value no longer
matches (the index of an sstable keeps listing a row under the value it had in that sstable), and the query planner
can choose to check a restriction on the rows instead of using its index.

`TriePattern.literalPrefix` returns the bytes of the leading literal elements, which every matching key starts with.
SAI uses the range of terms with that prefix to estimate how many rows a pattern selects and to skip the sstable
indexes whose terms cannot match.

## Representing the state

The set of positions is a bit set in which bit `i` stands for position `i`, so shifting the set left by one moves
every position past its element, and shifting it by two also moves it past the element after that. The transitions
above can then be computed for all positions at once with a few bitwise operations (the "shift-and" algorithm), using
masks prepared from the pattern:
- `patternMatch`, a table with an entry for each of the 256 byte values, where `patternMatch[b]` is the set of
  positions whose element accepts `b`;
- `isLoop`, the set of loop positions, which stay in the state when they accept a byte;
- `advanceOne[c]` and `advanceTwo[c]`, the positions that move past one and two elements when they accept a byte of
  class `c = b >>> 6`: `0` and `1` for ASCII bytes, `2` for continuation bytes and `3` for lead bytes.

`patternMatch` is filled by going over the elements and setting bit `i` in the entry of every byte that element `i`
accepts (see the table of elements above):
- a literal `b` sets its bit in `patternMatch[b]` only;
- an any-string sets its bit in all 256 entries;
- a character start sets its bit in the entries of `0x00`-`0x7F` and `0xC0`-`0xFF`, and a continuation loop in the
  entries of `0x80`-`0xBF`.

The advance masks follow the rules for loops and characters above. The bit of a position is in the masks of the byte
classes for which its element moves it:

| element at the position                         | past one element after | past two elements after |
|-------------------------------------------------|------------------------|-------------------------|
| literal, not followed by an any-string          | all bytes              | none                    |
| literal followed by an any-string               | all bytes              | all bytes               |
| any-string                                      | all bytes              | none                    |
| character start followed by a continuation loop | lead bytes             | ASCII bytes             |
| character start followed by an any-string       | ASCII and lead bytes   | ASCII bytes             |
| continuation loop                               | continuation bytes     | none                    |

Moving past two elements skips the loop after the first one: the any-string after a literal or after a character
that was a single ASCII byte, or the continuation loop of such a character. As two loops are never adjacent, there is
never a second loop to skip.

A byte `b` is read with

```
match = state & patternMatch[b]
c     = b >>> 6
next  = match & isLoop | (match & advanceOne[c]) << 1 | (match & advanceTwo[c]) << 2
```

where
- `match` is the set of positions in the state whose element accepts `b`,
- `match & isLoop` keeps the loops that accept `b`,
- `(match & advanceOne[c]) << 1` moves positions past their element, and
- `(match & advanceTwo[c]) << 2` moves positions past their element and the loop after it.

For `a*b`, the pattern of the example above, this gives the masks below (bits written from position 3, the end, down
to position 0). The pattern has no character wildcards, so the advance masks are the same for all byte classes.

| mask                                | bits   | positions                          |
|-------------------------------------|--------|------------------------------------|
| `patternMatch['a']`                 | `0011` | `0` (`a`) and `1` (`*`)            |
| `patternMatch['b']`                 | `0110` | `1` (`*`) and `2` (`b`)            |
| `patternMatch[x]` for any other `x` | `0010` | `1` (`*`)                          |
| `isLoop`                            | `0010` | `1` (`*`)                          |
| `advanceOne[c]`                     | `0111` | `0` (`a`), `1` (`*`) and `2` (`b`) |
| `advanceTwo[c]`                     | `0001` | `0` (`a`, followed by `*`)         |

Reading the key `axb` with `a*b` again:

| byte | `match` | `next` | state       |
|------|---------|--------|-------------|
|      |         | `0001` | `{0}`       |
| `a`  | `0001`  | `0110` | `{1, 2}`    |
| `x`  | `0010`  | `0110` | `{1, 2}`    |
| `b`  | `0110`  | `1110` | `{1, 2, 3}` |

The pattern `a?b` has the elements `a`, character start, continuation loop and `b`, and the positions `0` to `4`. Its
masks are:

| mask                                             | bits    | positions                                        |
|--------------------------------------------------|---------|--------------------------------------------------|
| `patternMatch['a']`                              | `00011` | `0` (`a`) and `1` (character start)              |
| `patternMatch['b']`                              | `01010` | `1` (character start) and `3` (`b`)              |
| `patternMatch[x]` for other non-continuation `x` | `00010` | `1` (character start)                            |
| `patternMatch[x]` for continuation bytes         | `00100` | `2` (continuation loop)                          |
| `isLoop`                                         | `00100` | `2` (continuation loop)                          |
| `advanceOne[ASCII]`                              | `01001` | `0` (`a`) and `3` (`b`)                          |
| `advanceTwo[ASCII]`                              | `00010` | `1` (character start)                            |
| `advanceOne[lead]`                               | `01011` | `0` (`a`), `1` (character start) and `3` (`b`)   |
| `advanceOne[continuation]`                       | `01101` | `0` (`a`), `2` (continuation loop) and `3` (`b`) |
| `advanceTwo[lead]`, `advanceTwo[continuation]`   | `00000` |                                                  |

Reading the keys `axb` and `aéb` (`é` is encoded as `C3 A9`):

| key   | byte | `match` | `next`  | state    | notes                                          |
|-------|------|---------|---------|----------|------------------------------------------------|
|       |      |         | `00001` | `{0}`    |                                                |
| `axb` | `a`  | `00001` | `00010` | `{1}`    |                                                |
|       | `x`  | `00010` | `01000` | `{3}`    | the character is complete, the loop is skipped |
|       | `b`  | `01000` | `10000` | `{4}`    |                                                |
| `aéb` | `a`  | `00001` | `00010` | `{1}`    |                                                |
|       | `C3` | `00010` | `00100` | `{2}`    | continuation bytes must follow                 |
|       | `A9` | `00100` | `01100` | `{2, 3}` | the loop stays and is also left                |
|       | `b`  | `01000` | `10000` | `{4}`    |                                                |

After `ax` the state only holds the literal `b`, so a walk can go directly to the child `b`. Under the general rules
it would be `{2, 3}`.

The operations do not know what the elements are; all of that is in the masks, whose construction gives the matching
these properties:
- Literals and classes of bytes are treated alike: they only differ in how many entries have their bit. Reading a byte
  costs the same whatever wildcards the pattern has.
- The advance masks only need to be right for the bytes that a position accepts, as `match` holds no other positions.
  The bit of a literal can be in the masks of all classes, and `isLoop` is the same for all classes: a continuation
  loop never stays for an ASCII byte, because it does not accept one.
- Several positions can accept the same byte, as `*` and `b` do for `b`, and they all advance at once. This is how the
  state follows all the ways the key can be matched.
- The end position has no element, so its bit is in no entry, and reading any byte after it is reached clears it
  (reading `y` after `axb` gives `0110`). The end is only in the final state if the key ends where the pattern does.
- A loop stays in the state only for the bytes the loop accepts. For example, a continuation loop is left at the first
  byte that is not a continuation byte, which the element after the loop must then take.
- If no position in the state accepts the byte, `match` and the next state are empty, which is how a walk finds out
  that no key in a branch can match. Many entries are zero: for a pattern of literals, all but the entries of its
  bytes.
- An any-string bit is in every entry and in `isLoop`, so once an any-string position is reached, it stays in the
  state whatever bytes follow. When the pattern ends in an any-string wildcard, the matchers use its position as the
  end position: a key that reaches it matches, and so does every extension of the key (the `MATCHES_ALL_EXTENSIONS`
  flag). The end position after it is not used.
- The initial state is `{0}`, plus position `1` if element `0` is a loop, which is skipped like any loop that is
  entered.

The table also gives the bytes that can follow a path: `b` can if `patternMatch[b] & state` is not empty. When the
state has a wildcard position, `nextAcceptedByte` uses this to find the next byte the walk has to visit; when it only
has literal positions, it looks at their bytes instead.

Two implementations differ in how they store the set:
- `PatternMatcher64` uses a single `long`, which serves patterns of up to 63 positions (bit `n` is the end). Its table
  is 256 `long`s. It keeps the advance masks already shifted, in one array of eight `long`s with the masks of class
  `c` at `2c` and `2c + 1`, and reads a byte with
  `match & isLoop | match << 1 & shifts[i] | match << 2 & shifts[i + 1]`, where `i = (b >>> 5) & 6`.
- `PatternMatcherLong` uses several `long` words for longer patterns. It applies the advance masks before shifting,
  so that the bits it carries from the top of each word into the next one are already masked:
  `advanceOne >>> 63 | advanceTwo >>> 62`. Its masks and states are kept in flat arrays: the table holds the words
  of byte `b` from index `b * numLongs`, the advance masks the two masks of each class and word next to each other,
  and the state history the words of depth `d` from index `d * numLongs`. As a trailing any-string wildcard is the end
  position, a pattern of 64 positions that ends in one still fits in a single word.

`descend` returns the flags of the new state (whether it is a match, matches all extensions, or contains an any-string
or other wildcards), which the walks keep for the current node instead of querying the state again. The bytes of chains
received through `advanceMultiple` are processed in a single call, so that the matcher's state can stay in a local
variable while they are processed, and the flags of the states in the chain, which the walks do not use, are not
computed. `flags(depth)` computes the flags of a state from its bits when they are needed.

`PatternMatcher.create` chooses `PatternMatcher64` when the pattern fits, as it is the faster of the two.
