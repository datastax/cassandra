<!--
Copyright IBM Corp.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Group B — messaging with DSE 6 peers

Part of the [reviewer triage guide](README.md), which explains the tiers, the review order and the layout of a brief.

## P10 — pre-4.0 message codec, payload serializers and the read guard (#2745)

**Tier:** 1 — it changes the internode wire format (message header, params, payload serializers, deletion-time
encoding) and edits serializer, read and write paths that run on every node, with or without DSE.

**Size and shape:** 29 production files (about +1660/-130), 16 test files (about +5220/-50), no fixtures. Four
commits: the codec, a LIVE complex-deletion fix, and two test-only follow-ups. `Message.java` (+660) carries most of
the risk. `DseReadGuard` is new and short. Every other production file adds a `DseCompat.isPre40Layout(version)`
branch next to the 4.0 code. `PagingState`, `DseCompatRefusals` and `RowFilter.Expression.kindCode` are comment or
accessor changes and can be skimmed. `LegacyFlag` is restored from the pre-CASSANDRA-18314 tree.

**Gating:** `DseCompat.enabled()` (`internode_compatibility_mode: DSE_6`) or `DseCompat.isPre40Layout(version)`
(version below 12 and the mode on). `MessagingService.minimum_version` stays 40, so the pre-4.0 branches stay out of
reach without the mode. Always-on changes for a cluster that never had DSE:
* `Message.Serializer.serializePost40` writes the payload with `message.getPayloadSerializer()`, which falls back to
  `RequestCallbacks.responseSerializer` when `verb.serializer()` is null (REQUEST_RSP, INTERNAL_RSP,
  UNUSED_CUSTOM_VERB). Before, such a message failed on a null serializer.
* Below 4.0, the `Message.Serializer` entry points and the address serializer now throw `AssertionError` even with
  assertions disabled.
* `ParamType` gains ids 2, 3, 4 and 7. `lookUpById` still returns null for them from 4.0 on, so the value is
  skipped as before.
* `Message`, `Mutation`, `CounterMutation` and `ReadResponse.LocalDataResponse` each carry one more field, null
  without the mode.
* Signature changes on hot paths: `inferMessageSize(..., version)`, and `SerializationHeader.writeDeletionTime` /
  `deletionTimeSerializedSize` with a version argument, called from `UnfilteredSerializer` and
  `UnfilteredRowIteratorSerializer`. Without the mode they fall through to the old methods.

**Where to look**
* `src/java/org/apache/cassandra/net/Message.java` — `Serializer.inferMessageSizePre40`, `extractParamsSizePre40`,
  `deserializeParamPre40`: these frame a stream of untrusted peer bytes. Are lengths and counts bounded everywhere
  (negative values, max message size, a 65535-byte alias)? Can a declared param length that is shorter than what the
  serializer reads desynchronise the stream?
* `src/java/org/apache/cassandra/net/Message.java` — `getPayloadSerializer(Verb, id, from)` and
  `expiresAtNanosPre40`: a DSE response is decoded with the serializer of whatever callback is registered under
  (id, peer) when the bytes arrive. If the callback has expired, the payload is skipped and null is delivered. Check
  that `ResponseVerbHandler` and the latency subscribers cope with a REQUEST_RSP/INTERNAL_RSP verb where they used to
  see the specific response verb.
* `src/java/org/apache/cassandra/net/Message.java` — `toPre40FailureResponse` / `toPost40FailureResponse`,
  `addFlagsToLegacyParams` / `removeFlagsFromLegacyParams`: is the mapping of flags and failures to the FAIL,
  FAIL_REASON, CAL_BAC and TrackRepaired params complete, and are params without a `legacyAlias` (warnings, sizes,
  REQUEST_VERB_NAME) safe to drop silently?
* `src/java/org/apache/cassandra/db/SerializationHeader.java` — `writeDeletionTime(dt, out, version)` and
  `pre40LiveLocalDeletionTimeDelta`: LIVE is written as the delta `Integer.MAX_VALUE - (int) minLocalDeletionTime`.
  Check `isForSSTable` excludes every sstable writer, and that the writer and size paths always agree.
* `src/java/org/apache/cassandra/db/ReadResponse.java` — `Pre40Data.at`: the local data response is deserialized
  again at the current version and serialized at the peer's version. The single-entry cache is `volatile`. Are the
  table and column filter captured at build time the right ones if the schema changes in between?
* `src/java/org/apache/cassandra/net/MessagingService.java` — `doSend` / `refuseForPre40Peer`, and
  `OutboundConnection.onFailedSerialize` / `onRefusedForPre40Peer`: the refusal tests `versions.get(to) <
  VERSION_40`. `EndpointMessagingVersions.get` returns `min(raw, current_version)`, so a peer stored at a raw DSE
  version (256 or more, from gossip `NET_VERSION`) reads as current. Is the serialization-time fallback in
  `OutboundConnection` the intended path for such peers?
* `src/java/org/apache/cassandra/service/DseReadGuard.java` — `unsupportedReadFeature`, called from
  `StorageProxy.readRegular` and `getRangeSlice`: is the list of DS-only features complete (operators above LIKE,
  kinds from 100, map entry with a non-EQ operator, OR, nested groups, index hints, optimizer options, user
  expressions, GROUP BY with a selector)? Can any read reach replicas without passing through these two entry points
  (Paxos reads, read repair, internal queries)?
* `src/java/org/apache/cassandra/db/MutationVerbHandler.java` — `forwardToLocalNodes`: in the mode, each forwarded
  target gets `builder.withId(id)` when the coordinator's ids differ. Without the mode it always reuses the
  forwarder's message, as before.

**Modified or deleted existing code**
* `src/java/org/apache/cassandra/net/Message.java` — `Serializer.serialize/deserialize/serializedSize/
  inferMessageSize/extractHeader` now dispatch on the version to Post40 or Pre40 variants. — History: 4bbfd64fcd7,
  2023, CASSANDRA-18314 (lift `minimum_version` to 40) removed the pre-4.0 codec this restores. Invariant to keep:
  the 4.0+ bytes must stay identical.
* `src/java/org/apache/cassandra/net/ParamType.java` — legacy aliases added, and `lookUpById` maps the pre-4.0-only
  ids to null. — History: 310a48efcfd, 2019, CASSANDRA-15066 (messaging rewrite); "do not re-use old ids" is the
  enum's own rule, and the ids reused here are the old 3.0 ones.
* `src/java/org/apache/cassandra/locator/InetAddressAndPort.java` — `Serializer.serialize/deserialize/extract/
  serializedSize` and `FwdFrmSerializer` accept address-only layouts in the mode; new `pre40DeserializeWithLength`.
  — History: 4bbfd64fcd7, 2023, CASSANDRA-18314 turned the pre-4.0 sizes 4 and 16 into an `AssertionError`.
* `src/java/org/apache/cassandra/net/ForwardingInfo.java` — serializer writes an int count and int ids below 4.0;
  `useSameMessageID` is re-added. — History: 4bbfd64fcd7, 2023, CASSANDRA-18314.
* `src/java/org/apache/cassandra/db/MutationVerbHandler.java` — `forwardToLocalNodes`, per-target ids in the mode.
  — History: 310a48efcfd, 2019, CASSANDRA-15066 ("reuse the same Message if all ids are identical"); c149e3412e0,
  2024 (fork, "Track internode msg bytes").
* `src/java/org/apache/cassandra/db/ReadCommand.java` — `Serializer.serialize` clears the NEEDS_RECONCILIATION and
  ACCEPTS_TRANSIENT bits below 4.0. — History: f7984627e7d, 2023, CASSANDRA-19018 (reconciliation for SAI
  intersections of partially updated rows); f7431b43287, 2018, CASSANDRA-14404 (transient replication). The patch
  states that a filtered read above CL ONE involving a DSE 6 replica can miss rows, as on DSE 6. That breaks the
  CASSANDRA-19018 guarantee during the window.
* `src/java/org/apache/cassandra/db/ReadResponse.java` — `Serializer` drops the repaired-data digest below 4.0;
  `LocalDataResponse` gains `Pre40Data`. — History: 4bbfd64fcd7, 2023, CASSANDRA-18314 (the asserts);
  d2923275e36, 2020, CEP-14 Paxos (the constructor).
* `src/java/org/apache/cassandra/db/filter/ColumnFilter.java` — `Serializer.makeHeaderByte` masks out
  FETCH_ALL_STATICS below 4.0; the deserializer maps FETCH_ALL to `ALL_COLUMNS`; version 10 omits the fetched
  columns. — History: 2059c7586ef, 2021, CASSANDRA-16686 (static data on empty partitions, the bit masked here);
  4bbfd64fcd7, 2023, CASSANDRA-18314.
* `src/java/org/apache/cassandra/db/filter/DataLimits.java` — `Serializer` uses DSE ordinals at version 11 in the
  mode and refuses unknown ordinals with an `IOException`. — History: b4c00802616, 2024, CNDB-9163 (DSE ordinals at
  VERSION_30). Its behaviour at VERSION_30 without the mode is kept.
* `src/java/org/apache/cassandra/exceptions/RequestFailureReason.java` — `Serializer` writes a 2-byte code below
  4.0, and codes above 1 become UNKNOWN. — History: 2054a126f8c, 2025, CNDB-12620 (vint code).
* `src/java/org/apache/cassandra/db/rows/UnfilteredSerializer.java` — `serialize`, `serializeRowBody`,
  `writeComplexColumn`, `serializeMarker` and the size methods take the version; new `checkPre40DeletionTime(s)`
  refuses deletion times past 2038. — History: 80a141ce01c, 2015, "Fix Coverity Defects" (the 3.0 row layout; ticket not
  established).
* `src/java/org/apache/cassandra/service/reads/range/RangeCommandIterator.java` — `supportsEndpointGrouping` is
  false in the mode. — History: b470ce389ee, 2021, STAR-184 (endpoint grouping for range reads).
* `src/java/org/apache/cassandra/service/reads/repair/BlockingPartitionRepair.java` — `msgVersionIdx` is based on
  `DseCompat.OLDEST_PRE40_VERSION` in the mode. — History: 644676b088b, 2018, CASSANDRA-10726. Invariant to keep:
  the index is never negative for any version `versions.get` returns.
* `src/java/org/apache/cassandra/net/OutboundConnection.java` — `onFailedSerialize` short-circuits refused verbs and
  sizes pre-4.0 messages defensively. — History: 310a48efcfd, 2019, CASSANDRA-15066.
* `src/java/org/apache/cassandra/net/InboundMessageHandler.java` — `processFirstFrameOfLargeMessage` /
  `processOneContainedMessage` pass the version to `inferMessageSize`. — History: 4bbfd64fcd7, 2023,
  CASSANDRA-18314.

**Failure and rollback risks**
* Rollback (the patch states this as a known gap): bb sstables written in the mode keep the default LIVE encoding.
  After a rollback to DSE, a 32-bit reader takes LIVE complex deletions in them for real deletions. The patch says
  this gives digest mismatches with no data loss.
* `Pre40Data` keeps a second serialized copy of every local data response sent to a DSE 6 peer for the life of the
  response. That is memory and CPU pressure on replicas during the window.
* Message ids and forwarding ids are narrowed with `Ints.checkedCast`. `Message.nextId` is an `AtomicInteger`, so
  ids from this node fit. An id received from a peer and forwarded is read as an int below 4.0, so it fits too.
* Responses decoded by callback: a callback that expires between `inferMessageSize` and payload decoding drops the
  payload. It is counted in `skippedPayloadsPre40` and logged at TRACE only.
* A verb queued before the peer's version was known fails at serialization time and is counted. Its callback fails
  with UNKNOWN through `RequestCallbacks.onRefused`, not with a timeout.
* The 2038 check throws `IllegalStateException` from inside serialization. The patch states the HCD_1 storage mode
  means it never fires. If it does fire, it surfaces as a failed serialize on the connection.

**Test integrity**
* Existing tests changed: `test/unit/org/apache/cassandra/net/MessageTest.java` is restructured. `testInferMessageSize`,
  `testCycle*` and `testFailureResponse*` are now parameterised by version, and every VERSION_40 assertion is still
  there. `ForwardingInfoTest` only gains cases. No assertion was weakened or deleted.
* New tests, real round trips: `V11SerializationAuditTest`, `ColumnFilterTest`, `DataLimitsTest`,
  `SerializationHeaderTest`, `ReadResponseTest`, `BlockingReadRepairTest`, `Dse6EndpointGroupingTest`.
  `Dse6ReadGuardTest` runs through CQL (CQLTester).
* New tests, golden bytes: `DseForwardToCaptureTest`, `DseForwardFromCaptureTest` and
  `MutationVerbHandlerForwardingTest` use hex copied from packet captures between DSE 6.9.26 and this software.
  These bytes do not come from the code under test. `DseHintsAcknowledgementTest` and `HintMessages` use hand-built
  hex.
* New tests, hand-built: `MessageTest.pre40LengthsNoMessageHasAreRefused` patches bytes produced by this codec.
* No test in this patch runs DSE 6.8 bytes, or an end-to-end connection, at version 11. The handshake that makes
  that possible arrives in P11.
* Not covered: `Pre40Data` under concurrent sizing and serialization. The `RequestCallbacks.onRefused` path in
  `OutboundConnection.onRefusedForPre40Peer` is not exercised here (the commit says "exercised once the handshake can
  establish a connection", which is P11's `Dse6RefusedVerbTest`).

**Suggested reviewer questions**
* `EndpointMessagingVersions.get` caps a raw DSE version (1024) at `current_version`, so
  `MessagingService.doSend` sees such a peer as 4.0+. Should the pre-queue refusal use `getRaw` mapped by
  `DseCompat.messagingVersionFromGossip`, or is the serialization-time fallback deliberate?
* With a DSE response verb being REQUEST_RSP or INTERNAL_RSP, what do `latencySubscribers.maybeAdd(cb,
  message.verb(), ...)`, replica sensors and any verb-keyed metrics record? Is that acceptable for the upgrade window?
* Is dropping NEEDS_RECONCILIATION towards DSE replicas (no CASSANDRA-19018 protection for SAI intersections above
  CL ONE) acceptable, or should the guard refuse such reads instead?
* `ColumnFilter.Serializer.deserialize` in the mode turns every FETCH_ALL from a DSE peer into `ALL_COLUMNS`. Does
  that change what an HCD replica returns, and therefore digests, compared with a DSE replica of the same read?
* Should the rollback gap (LIVE complex deletions in bb sstables) be closed before release, or documented in the
  runbook?

## P11 — pre-4.0 handshake and legacy framing with DSE 6 peers (#2748)

**Tier:** 1 — it changes the internode handshake on both sides, the frame decoders and encoders, outbound
reconnection behaviour on the event loop, and the handshake timeout path that runs on every node.

**Size and shape:** 25 production files (about +2100/-60), 2 yaml files (comments only), 11 test files (about
+4300/-30), no fixtures. Five production files are restored almost verbatim from the tree before
CASSANDRA-18314 (4bbfd64fcd7): `FrameDecoderLegacy` (one javadoc paragraph added), `FrameEncoderLegacy`,
`FrameEncoderLegacyLZ4`, `LegacyLZ4Constants` (identical) and `FrameDecoderLegacyLZ4` (a `channelRead` override and a
decompressed-length check added). Those can be read as diffs against `4bbfd64fcd7^`. New logic is in
`DseDialectVersions` (323), `DsePeers` (172), `HandshakeProtocol`, `InboundConnectionInitiator`,
`OutboundConnectionInitiator` and `OutboundConnection`. The Config, DatabaseDescriptor, InternodeCompatibilityMode,
DseCompat, DsePeersRecord and yaml changes are comment-only and can be skimmed.

**Gating:** `DseCompat.enabled()` gates pre-4.0 decoding of `Initiate` and `Accept`, the third message, the legacy
framing (selected only when `useMessagingVersion < VERSION_40`), seeding from saved peers, DSE identification and the
reconnection rules. Always-on changes for a cluster that never had DSE:
* `InboundConnectionInitiator.Handler.handlerAdded`: the handshake timeout no longer dereferences a null
  `initiate`. Before, it threw a NullPointerException and left the socket open; now it closes the socket and logs
  the remote socket address.
* A `channelInactive` override was added. Its body acts only in the mode.
* `HandshakeProtocol.Initiate` gains `requestMessagingVersion`, which is `acceptVersions.min` without the mode, and
  `equals` now compares it. `toString` prints the framing by name.
* The `OutboundConnectionInitiator` debug line prints the requested version instead of the maximum twice.
* `OutboundConnections.LARGE_MESSAGE_THRESHOLD` now uses `LegacyLZ4Constants.HEADER_LENGTH`, the same 21-byte value.
* New MBean attribute `MessagingServiceMBean.getDsePeers`, empty without the mode. `nodetool compatstatus` prints one
  more line, "DSE peers : not tracked (...)". A nodetool of this version against an older server reports it as not
  reported.

**Where to look**
* `src/java/org/apache/cassandra/net/HandshakeProtocol.java` — `Initiate.maybeDecode` and
  `Accept.maybeDecode(in, requestMessagingVersion)`: in the mode, a 4-byte answer below 12, or of 256 and more, is
  taken as pre-4.0. Can a legitimate 4.0+ `Accept` (8 bytes plus CRC) ever start with an int that matches, so that a
  5.0 peer is misread as pre-4.0 by a node in the mode?
* `src/java/org/apache/cassandra/net/OutboundConnectionInitiator.java` — `Handler.channelActive` and `decode`:
  outbound messaging requests `max(request, VERSION_50)` in the legacy byte. A pre-4.0 reply to the 4.0 probe
  becomes `retry(DSE_DIALECT)`. A reply to this node's pre-4.0 message must echo the request, or be a well-formed DSE
  version at 11. Is every other answer final, with no ping-pong between 11 and 12+?
* `src/java/org/apache/cassandra/net/InboundConnectionInitiator.java` — `initiatePre40` and `confirmPre40`: streaming,
  versions below 10 and encryption are refused before the 4-byte answer. The encryption check runs on the socket IP
  with the default storage port. The third message's peer-supplied address becomes the endpoint identity, with the
  default port. Is that check equivalent to the 4.0-style one under `internode_encryption: dc|rack`? Is the identity
  safe when `storage_port` differs between nodes?
* `src/java/org/apache/cassandra/net/InboundConnectionInitiator.java` — `setupMessagingPipeline`: in the mode the
  version is recorded through `DseDialectVersions.recordHandshakeVersion`. Pre-4.0 peers get `FrameDecoderLegacy` /
  `FrameDecoderLegacyLZ4`, and CRC framing is downgraded to unframed. Confirm that 4.0+ peers in the mode always take
  the old decoder branch.
* `src/java/org/apache/cassandra/net/DseDialectVersions.java` — `recordHandshakeVersion`, `connectionVersion`,
  `supersededDialect`, `versionRaised`: these are the rules that move a peer between 11 and 4.0+. They run on netty
  event loops and the gossip stage, with the gossip-state read under the `EndpointMessagingVersions` monitor. Check
  them for flapping, and for a peer that never leaves the dialect after an upgrade.
* `src/java/org/apache/cassandra/net/OutboundConnection.java` — `Connecting.onCompletedHandshake` (superseded
  dialect: close and reconnect once at once, then `retryLater`), the `retry` branch calling `answeredInTheDialect`,
  and the `attempt` path calling `connectionVersion`. Does `retryLater` keep the queue, capacity and state machine
  invariants that `onFailure` keeps, including the dormant state when nothing is pending?
* `src/java/org/apache/cassandra/net/FrameDecoderLegacyLZ4.java` — the added `channelRead(ctx, msg)` override
  (releases blocks after `discard`) and the decompressed-length check. These are the only changes to restored code.
  Are references released on every path?
* `src/java/org/apache/cassandra/net/MessagingService.java` — `listen` / `seedDseDialectPeers`: peers stored with a
  `4.0.0.<build>` release are assumed at 11 before listening, through `EndpointMessagingVersions.assume`, which does
  not feed `DsePeersRecord`. What happens when this read races with a gossip update at startup?

**Modified or deleted existing code**
* `src/java/org/apache/cassandra/net/HandshakeProtocol.java` — `Initiate.encodeFlags`, `encode`, `maybeDecode`,
  `equals`, `toString`; `Accept.maybeDecode`; new `ConfirmOutboundPre40`. — History: 4bbfd64fcd7, 2023,
  CASSANDRA-18314 made `maybeDecode` return null for any legacy version below 4.0 ("5.0+ does not support pre40").
  The invariant that with NONE a pre-4.0 first message is never decoded is kept.
* `src/java/org/apache/cassandra/net/InboundConnectionInitiator.java` — `handlerAdded` (timeout), new
  `channelInactive`, `decode`, `initiate`, `setupMessagingPipeline`. — History: not established by blame in this
  triage (the file comes from CASSANDRA-15066, 310a48efcfd, 2019, and CASSANDRA-18314 removed the legacy decoders it
  selected).
* `src/java/org/apache/cassandra/net/OutboundConnectionInitiator.java` — `initiateMessaging`/`initiateStreaming`
  take a requested version; `channelActive` and `decode` change as above. — History: CASSANDRA-15066 (310a48efcfd,
  2019); the pre-4.0 branches were removed by CASSANDRA-18314 (4bbfd64fcd7, 2023).
* `src/java/org/apache/cassandra/net/OutboundConnection.java` — `Connecting.onCompletedHandshake`, retry branch,
  `attempt`; new `retryLater`. — History: 310a48efcfd, 2019, CASSANDRA-15066 (connection state machine).
* `src/java/org/apache/cassandra/net/OutboundConnections.java` — `HEADER_LENGTH` moved to `LegacyLZ4Constants`, same
  value. — History: CASSANDRA-18314 kept this constant after deleting the legacy LZ4 code.
* `src/java/org/apache/cassandra/net/EndpointMessagingVersions.java` — new `assume`. Existing methods are untouched.
* `src/java/org/apache/cassandra/net/FrameDecoder.java` — new `isDiscarded`.
* `src/java/org/apache/cassandra/tools/nodetool/CompatStatus.java`, `NodeProbe`, `MessagingServiceMBean(Impl)` — the
  "DSE peers" line and attribute. These were added by this series (P02, P05).

**Failure and rollback risks**
* Wrong identification: a peer whose saved release is stale, upgraded while this node was down, is connected to at
  11 first. The patch relies on gossip, `versionRaised` (interrupt outbound) and `supersededDialect` to recover. A
  bug here shows up as connections that keep reconnecting, or as a peer stuck at 11 that P10 then refuses
  DS-only verbs to.
* A DSE 6.8.65 peer answers this node's pre-4.0 message with 1024, not an echo. The second commit exists because the
  first one refused that, and nodes in the mode could not start ("Unable to gossip with any peers"). Any later
  tightening of `isDseHandshakeVersion` has the same blast radius.
* `DsePeers` is in-memory only. After a restart, identification depends on handshakes again. The patch states that
  until then a DSE peer is treated as HCD 1.2 at the dialect.
* Rollback to DSE: a node rolled back keeps a stale gossip `NET_VERSION` of 12 or more for a while.
  `recordHandshakeVersion` keeps the 4.0+ version for plain pre-4.0 handshakes from such a peer, until new gossip
  arrives. Only DSE-only first messages override this.
* Inbound encryption is checked twice (socket IP before the answer, claimed address after the third message). A
  peer that claims an address it does not own is accepted under that identity, as in the 4.0-style handshake.
* `retryLater` reschedules on the event loop with a 100 ms to 1 s backoff. Without pending messages it leaves the
  connection dormant.

**Test integrity**
* Existing tests changed: `FramingTest` moves its serializer setup into a new `@BeforeClass` and adds legacy cases;
  the old assertions are kept. `CompatStatusTest` replaces one ordering assertion with two stricter ones and adds the
  new line. `Dse6RefusedVerbTest` (added by this series, P10) gains connection-level cases. `Dse6ModeSmokeTest` and
  `DsePeersRecordTest` change only expected log text and javadoc. Nothing was weakened.
* New tests, real: `DseHandshakeTest` (43 tests) uses real sockets (`InboundSockets`, `OutboundConnection`), a fake
  DSE storage port and captured DSE handshake bytes. `DseLegacyLZ4ConnectionTest` runs real connections.
  `DseDialectVersionsTest` uses the node's versions and `OutboundConnection`. `CC4NodesFileReaderDseTest` reads the
  saved peers. `Dse6ModeSmokeTest` is in-JVM.
* New tests, mocked or hand-built: `LegacyLZ4FramingTest` uses netty `EmbeddedChannel` with streams produced by the
  restored encoder. The fake DSE port in `DseHandshakeTest` is written from the patch's reading of DSE's code, not
  from DSE itself.
* Not covered by any test in the patch: the TLS path of `channelInactive` (the javadoc says "reasoned from netty's
  code, not covered by a test"); a real DSE 6.8 or 6.9 node over the wire (only captured bytes and the fake port);
  `recordHandshakeVersion` under concurrent gossip updates.

**Suggested reviewer questions**
* Can a 5.0 or HCD 2.x peer's 12-byte `Accept` ever be misread as a pre-4.0 4-byte answer by
  `Accept.maybeDecode` in the mode, for example during a rolling restart of the mode-off nodes?
* `ConfirmOutboundPre40` and `pre40EncryptionCheckAddress` fix the peer's port to this node's storage port. Is a
  cluster with per-node `storage_port` or `legacy_ssl_storage_port_enabled` out of scope, and is that refused
  anywhere?
* The outbound 4.0-style probe now carries `max(request, 13)` in the legacy byte, instead of `acceptVersions.min`,
  for every peer in the mode, including HCD 2.x peers. Is any 4.0+ reader known to look at that byte beyond "at least
  12"?
* `versionRaised` interrupts every outbound connection to a peer, including those already at 4.0+. Could a peer whose
  version flips between gossip and handshake keep causing interrupts?
* The handshake timeout fix applies outside the mode. Should it be split out as a product bug fix with its own test?

## Group notes (P10, P11)
* P10 contains the codec but cannot run end to end without P11. `Dse6RefusedVerbTest` gains its connection-level
  cases only in P11, and the commit of P10 says the refusal at serialization time is "exercised once the handshake
  can establish a connection at version 11". Review the two together. P11 must not merge without P10.
* Both patches build on `DseCompat` (`enabled`, `isPre40Layout`, `DSE_DIALECT`, `messagingVersionFromGossip`,
  `isDse6ReleaseVersion`), `DsePeersRecord`, `DseCompatRefusals` and `nodetool compatstatus`, which come from earlier
  patches outside this group.
* Version reading is split across components. `EndpointMessagingVersions.get` caps a raw DSE version at
  `current_version`. P11's `DseDialectVersions` and `DsePeersRecord` read `getRaw` through
  `messagingVersionFromGossip`, while P10's `MessagingService.doSend`, `BlockingPartitionRepair` and the outbound
  `messagingVersion` read `get`. Decide whether one helper should answer "does this peer speak the dialect".
* Several files are restored from the code that CASSANDRA-18314 (4bbfd64fcd7, 2023) deleted: `LegacyFlag` (P10),
  the legacy frame decoders and encoders, and `LegacyLZ4Constants` (P11). They keep their original ASF header.
  Diffing them against `4bbfd64fcd7^` shows they are nearly verbatim.
* Duplicated parsing: `DsePeers.gossipedDseVersion` and `DseDialectVersions.gossipedVersion40OrLater` both parse
  `NET_VERSION` from gossip with slightly different ranges.

## P12 — gossip with DSE 6 peers (#2749)

**Tier:** 1 — it changes gossip state serialization (application-state ordinals, which states are sent, the published
`NET_VERSION` and a new `DSE_GOSSIP_STATE`), the echo/mark-alive path of `Gossiper`, and
`StorageService.updateNetVersion`, all in existing gossip product code.

**Size and shape:** production about 1,600 added lines: two new files (`gms/DseGossipStates.java` about 640,
`gms/DseEchoRetries.java` about 220), the rest spread over 22 existing files (a large share is Javadoc). Tests about
2,980 added lines (three new unit classes of about 850 lines each, plus edits to ten existing tests), no fixtures.
Skimmable: `CassandraRelevantProperties`, `InternodeCompatibilityMode`, `DseCompat`, `MessagingServiceMBean`,
`InboundConnectionInitiator`, `OutboundConnectionInitiator`, `DsePeersRecord` (comment-only hunks), `CompatStatus` and
`MessagingServiceMBeanImpl` (wording of output strings).

**Gating:** `internode_compatibility_mode: DSE_6` (`DseCompat.enabled()`), and within it a pre-4.0 messaging version on
the connection plus the peer's classification (`DsePeers.isDse` / `isPresumed`). What changes for a cluster that never
had DSE: no functional change. Every new branch tests `DseCompat.enabled()` (`DsePeers.forget`,
`DseGossipStates.sendsAllOwnStates`, `recipient`, `isPre40Layout`, `addDseState`, `warnIfNoneKnownAtStartup`,
`checkDse6GossipedDseVersion`). Two existing paths were rewritten without a mode check: the pre-4.0 branches of
`EndpointStateSerializer.filterOutgoingStates` and `deserialize`. They are not reachable on the wire outside the mode
(`MessagingService.minimum_version` is `VERSION_40` on `main`, and the pre-4.0 codec is refused without the
mode). `Gossiper.evictFromMembership` goes from private to package-private and `EndpointStateSerializer` becomes a
singleton with a private constructor.

**Where to look**
* `src/java/org/apache/cassandra/gms/DseGossipStates.java` — `fromDse` / `toDse`: the ordinal map (DSE 15 to 20 kept in
  this software's padding `X2`..`X7`, DSE 21 becomes `DISK_USAGE`, DSE 22+ dropped, `STATUS_WITH_PORT` becomes `STATUS`
  only when there is no `STATUS`, `SSTABLE_VERSIONS`/`INDEX_STATUS` not sent). Is the table in the class Javadoc right
  for every DSE 6.8/6.9 release in scope? Is it safe to relay DSE values stored in `X2`..`X7` between nodes of this
  software at 4.0+?
* `DseGossipStates.Recipient.decide` and `GossipDigestAck.statesToWrite` / `GossipDigestAck2.statesToWrite`: the choice
  of ordinals and whether states are withheld is made once, at the first `serializedSize`/`serialize` at a pre-4.0
  version, under `synchronized`. Confirm that size and bytes cannot disagree when an identification lands between the
  two calls, and that a message whose states are withheld cannot drop a state the peer needs (for example `STATUS` on a
  decommission).
* `DseGossipStates.receivedFrom`: the heuristic that drops a whole endpoint state when a not-identified sender has a
  bare number at ordinals 15 to 20 or any value at 21, and the rule that ends a presumption when an ack/ack2 that
  answers a request carries no state. Is there a legitimate HCD 1.2 (`-Dds.current_messaging_version=11`) state this
  drops, given the Javadoc note that HCD 1.2 has `DISK_USAGE`/`SSTABLE_VERSIONS` swapped against this software?
* `DseGossipStates.sendsAllOwnStates` used in `Gossiper.examineGossiper` and
  `GossipDigestAckVerbHandler.requestedStates`: this reverts the CASSANDRA-19983 "send all own states" behaviour for
  identified DSE peers. Does losing that repair leave a DSE node unable to recover a state of this node that it holds at
  the same version with entries missing?
* `Gossiper.markAlive` / `dseEchoHandler` with `DseEchoRetries`: in the mode every peer (not only DSE) gets echo
  requests with `invokeOnFailure` and resends driven by `isDue`. Check the interaction with `inflightEcho` (the failure
  callback keeps the pending echo), the identity comparison `last.state == state` against the `equals` used for
  `inflightEcho`, and that the record is dropped on every path that removes `inflightEcho` (`markDead`,
  `removeEndpoint`, `evictFromMembership`, the answer).
* `StorageService.updateNetVersion`: in the mode a DSE `NET_VERSION` (256+) is stored as 11, a gossiped 12 does not
  lower a negotiated higher version, a non-numeric value is logged instead of `AssertionError`, and `versionRaised`
  closes connections. The `synchronized (MessagingService.instance().versions)` block must pair with
  `DseDialectVersions.recordHandshakeVersion`; check the lock order with the event-loop callers.
* `StorageService` restart handling (the hunk before `updateNetVersion(endpoint, netVersion)` on restart): in the mode
  it reads `NET_VERSION` from the gossiper's current state instead of the state passed in. Confirm that the passed state
  is the pre-restart one in every caller.
* `VersionedValue.networkVersion` and `StorageService.addDseState`: the node gossips `NET_VERSION` 12 and a JSON
  `DSE_GOSSIP_STATE` with `dse_version` 6.8.1 (by default) at `X_11_PADDING` to all peers. See the question on nodes
  already in `NONE` below.

**Modified or deleted existing code**
* `src/java/org/apache/cassandra/gms/EndpointState.java` — `EndpointStateSerializer.filterOutgoingStates`,
  `filterOutgoingState`, `filterIncomingStates`, `splitStatusAndPort`, `splitAddressAndPort`: the pre-4.0 translation is
  deleted. Pre-4.0 now writes DSE ordinals (`dseOrdinals`) or this software's ordinals with the legacy index status;
  `deserialize` no longer translates (the verb handlers do) and, in the mode only, drops unknown ordinals instead of
  throwing. — History: `820a3c0ab0e` (2026, HCD-241, "DSE 6.8/9 upgradeability"), an earlier DSE upgrade attempt in
  `main`. Invariant: a pre-4.0 peer receives states it can parse. HCD-241 wrote the address into `STORAGE_PORT`
  when no port was present; the new code never puts a non-number into a DSE port state.
* `src/java/org/apache/cassandra/gms/GossipDigestAckVerbHandler.java` — `doVerb` / new `requestedStates`, and
  `Gossiper.examineGossiper`: the "reply everything about me" branch is skipped for identified DSE peers. — History:
  `426eebb5139` (2024, CASSANDRA-19983). Invariant: a peer asking about this node always gets its full state.
* `src/java/org/apache/cassandra/gms/Gossiper.java` — `markAlive`: a second callback with retries in the mode. —
  History: the `inflightEcho` single-request logic, `3ad4794aa00` (2026, CASSANDRA-18866, "Node does not send multiple
  inflight echos"). Invariant: at most one echo request per endpoint state. The mode deliberately relaxes it to one per
  timeout or per gossip interval.
* `Gossiper.applyStateLocally` — `DseGossipStates.deriveAddressesAndPorts` runs before
  `removeMajorVersion3LegacyApplicationStates`. DSE's release `4.0.0.x` parses as major 4, so legacy `INTERNAL_IP` /
  `RPC_ADDRESS` are stripped once the combined states exist. `derive` falls back to the stored combined state for that
  reason.
* `Gossiper.evictFromMembership` / `removeEndpoint` — `DsePeers.forget` added. — History: `b1232ae4c90` (2021, STAR-996)
  for the `Nodes.peers()` removal next to it.
* `src/java/org/apache/cassandra/service/StorageService.java` — `updateNetVersion`: the parse is split from `set`, with
  a mode-only branch. — History: `f3b7599e3b6` (2016; the commit message reads CASSANDRA-1128) sets the version from
  gossip early. Invariant: a gossiped version is recorded before the node is used. The mode now sometimes keeps a higher
  negotiated version instead.
* `src/java/org/apache/cassandra/gms/VersionedValue.java` — `networkVersion()`: publishes `NET_VERSION_PUBLISHED` (12)
  in the mode. — History: `e6530cc3723` (2012, CASSANDRA-4317). Invariant: `NET_VERSION` is the node's real messaging
  version.
* `src/java/org/apache/cassandra/gms/ApplicationState.java` — comments only. `X2`..`X7` are declared reserved while the
  mode exists, and `X_11_PADDING` now carries `DSE_GOSSIP_STATE`. — History: `73317c4427e` (2012, padding for 1.1) and
  the CASSANDRA-16484 padding rule.

**Failure and rollback risks**
* Mixed `DSE_6`/`NONE` nodes of this software: a node in the mode gossips `NET_VERSION` 12. A node already restarted
  with `NONE` runs the unmodified `updateNetVersion` and stores 12 for it, possibly below the version their handshake
  negotiated. Only nodes in the mode protect the negotiated version. Not established whether the runbook forbids that
  mixed state.
* Rollback to DSE: DSE peers have stored `dse_version` 6.8.1 for the node from `DSE_GOSSIP_STATE`. The patch states that
  DSE then counts the node as 6.8.1, keeps NodeSync-type features on (NodeSync must stay disabled), and on 6.9 logs a
  version downgrade. A node rolled back to DSE gossips its real `DSE_GOSSIP_STATE` with a new generation, so this should
  clear. Not tested here.
* The `DsePeers` record and the `PRESUMED` set are in memory only. After a restart there is one gossip round in which a
  DSE peer that is not yet identified gets this software's ordinals, or (when presumed) acks without states. States
  received from DSE nodes in that window are dropped whole by `receivedFrom` and requested again. Both are by design,
  per the class Javadoc.
* `DsePeers.presume` / `identify` check-then-act race: `presume` may insert after `identify` removed the entry.
  `isPresumed` also checks `!IDENTIFIED.containsKey`, so this is harmless as written.
* Under a partition in the mode, `DseEchoRetries` sends one echo per request timeout, or one per gossip interval when
  requests fail, for as long as the peer's state keeps arriving through other nodes. Upstream sends one.
* `X2`..`X7` are now semantically used. Any later fork or upstream change that assigns those padding slots would collide
  while the mode exists.

**Test integrity**
* Existing tests changed:
  * `EndpointStateDeserializationEdgeCasesTest`: rewritten. The HCD-241 expectations are reversed: an address without a
    port is no longer expected in `STORAGE_PORT`; it becomes `INTERNAL_IP` / `NATIVE_TRANSPORT_ADDRESS` alone, and
    `STATUS_WITH_PORT` keeps its pieces minus ports instead of being cut to the first token. IPv6 and non-address cases
    were added.
  * `EndpointStateIndexStatusTest`: `testIndexStatusDroppedForPre40Peers` (asserted null) is renamed and inverted to
    `testIndexStatusRewrittenForHcd12PeersAtPre40Versions` (legacy format sent), and `testIndexStatusNotSentToDsePeers`
    is added.
  * `DseHandshakeTest.dseNodeReconnectingAt11IsNotIdentifiedByTheHandshakes` is renamed `...IsIdentifiedByGossip`. It
    keeps the "handshakes identify nothing" assertion and adds gossip identification.
  * `V11SerializationAuditTest`: the gossip verbs, previously skipped, are now audited (strengthened).
  * `CompatStatusTest`, `DseCompatTest`: expected message strings updated. `DseDialectVersionsTest`,
    `Dse6StartupChecksTest`, `DsePeersRecordTest`, `CC4NodesFileReaderDseTest`: additions; no assertion found removed.
* New tests: `DseGossipTest`, `DsePeerGossipTest` and `DseEchoRetryTest` are single-JVM unit tests. The gossip ones
  round trip states through the real serializers and verb-handler readers (via the test helper `GossipMessages`), so
  writer and reader are the same code under test. The only external reference is a captured `nodetool gossipinfo` of a
  DSE 6.9.26 node (values, not wire bytes; tokens replaced). `DseEchoRetryTest` captures echo requests in the outbound
  sink and drives callbacks with a test clock; its Javadoc says the pre-4.0 failure-response layout "comes from DSE's
  code, not from a capture". No multi-node or in-JVM dtest.
* Not covered by any test in the patch: gossip with a real DSE node on the wire (ordinals of DSE 6.8.x specifically,
  only 6.9.26 values are used); the restart-path `NET_VERSION` re-read in `StorageService`; a mixed `DSE_6`/`NONE`
  cluster of this software; the effect of the published `DSE_GOSSIP_STATE` on DSE (stated as observed, not tested).

**Suggested reviewer questions**
* Is reverting CASSANDRA-19983 behaviour for DSE peers (`sendsAllOwnStates`) acceptable, or should it be limited to the
  states DSE re-fires listeners for?
* Can `receivedFrom` drop the state of a legitimate HCD 1.2 node at the dialect (bare numbers at 15 to 20, given the
  `DISK_USAGE`/`SSTABLE_VERSIONS` swap)? If so, how long is that node invisible?
* Should a node in `NONE` also refuse to lower a negotiated version on a gossiped 12, given that peers in `DSE_6`
  publish 12?
* Is gossiping `dse_version` 6.8.1 to DSE 6.9 clusters safe? The patch itself says the 6.9 feature-deactivation path was
  not observed.
* Should `DseEchoRetries` be limited to DSE peers instead of every peer in the mode?

## P18 — internode TLS ports of DSE 6 peers (#2750)

**Tier:** 1 — it changes which port outbound internode connections dial, adds a listening socket, and removes the
plain-text SSL fallback in `OutboundConnection` and `NettyStreamingConnectionFactory`, which is connection-establishment
product code (all of it gated).

**Size and shape:** production about 330 added lines: one new file `net/DseTlsPorts.java` (about 275), about 50 lines in
`InboundSockets`, `OutboundConnection`, `OutboundConnectionSettings` and `NettyStreamingConnectionFactory`, and a 3-line
comment in each of `conf/cassandra.yaml` and `conf/cassandra_latest.yaml`. Tests about 1,125 lines: `DseTlsPortsTest`,
`DseTlsFallbackTest` (unit, real sockets), the in-JVM dtest `Dse6InternodeTlsPortTest`, and 26 added lines in
`DseHandshakeTest`. Two commits with the same subject.

**Gating:** `internode_compatibility_mode: DSE_6` together with internode encryption for the peer. The listener also
needs `legacy_ssl_storage_port_enabled: false` and `ssl_storage_port != storage_port`. Port choice applies only at a
pre-4.0 messaging version. The fallback refusal applies only to `NO_SSL` towards a peer not known at 4.0+. What changes
for a cluster that never had DSE: nothing. `withLegacyPortIfNecessary`, `sslFallback` and
`listensOnSslStoragePortForDse` all return the upstream answer when `DseCompat.enabled()` is false, and the yaml change
is comment only.

**Where to look**
* `src/java/org/apache/cassandra/net/OutboundConnectionSettings.java` — `withLegacyPortIfNecessary`: reintroduces a
  method of the same name that CASSANDRA-18314 removed. It dials `ssl_storage_port` according to
  `DseTlsPorts.dialsSslStoragePort` (DSE version below 6.9.7, or the local `legacy_ssl_storage_port_enabled` when the
  version is unknown). `encryption == null` is the per-peer "not encrypted" test (as `withEncryption()`). Is the 6.9.7
  cut-over right for every DSE 6.9 build in scope?
* `DseTlsPorts.dseMessagingVersion` / `dseMessagingVersionOfRelease`: the version comes from gossip `NET_VERSION`
  (256+), else from the stored `system.peers` release parsed by `DseCompat.dse6MinorAndPatch` (`4.0.0.697` read as
  6.9.7, `4.0.0.6926` as 6.9.26). Check that the build-number parsing cannot confuse 6.9.x patch levels. Also check that
  a stale release of a peer already upgraded to this software sends it to `ssl_storage_port`, where it listens only if
  it is in the mode with encryption on.
* `src/java/org/apache/cassandra/net/OutboundConnection.java` — `attempt`: the port is re-chosen on each attempt from
  `template`, and `NO_SSL` is replaced by `SERVER_CONFIG` through `DseTlsPorts.sslFallback`. Does the substitution
  change the retry cadence (the same TLS strategy now appears twice per cycle), and does it apply to peers of this
  software whose version is not yet known on the first connection after a restart?
* `DseTlsPorts.plainFallbackRefusal`: "known at 4.0+" requires the attempt version, the connection's copied versions and
  the node's versions to agree. Is reading the global `MessagingService.instance().versions` from the event loop
  consistent with how `DseDialectVersions.connectionVersion` reads it a few lines earlier?
* `src/java/org/apache/cassandra/net/InboundSockets.java` — `bindings`: opens the TLS-only legacy socket in the mode
  although `legacy_ssl_storage_port_enabled` is false. What happens on a bind failure (the port is busy)? Does it refuse
  the start, and is that the intended failure mode?
* `src/java/org/apache/cassandra/streaming/async/NettyStreamingConnectionFactory.java` — `connect`: `break` when the
  strategy would change, which assumes `NO_SSL` is last in `SslFallbackConnectionType.values()`. Streaming connections
  get no `withLegacyPortIfNecessary`. Not established whether streaming to a DSE peer at the dialect can happen at all.

**Modified or deleted existing code**
* `src/java/org/apache/cassandra/net/OutboundConnection.java` — `attempt`: the SSL fallback index is now filtered, and
  the port re-chosen. — History: the fallback rotation is from `ca75ffe4d09` (2022, CASSANDRA-17923, "Mixed mode support
  for internode authentication during TLS upgrades"). The re-evaluation comment that mentions the legacy port is from
  `678ca3fc29c` (2020, CASSANDRA-15727), and the original pre-4.0 port choice was removed in `4bbfd64fcd7` (2023,
  CASSANDRA-18314, "Lift MessagingService.minimum_version to 40"). Invariant: optional encryption may retry in plain
  text; the mode breaks it on purpose for peers not known at 4.0+.
* `src/java/org/apache/cassandra/net/InboundSockets.java` — `bindings`: the legacy SSL socket condition gains `||
  forDse`. — History: line last touched by `a3258d66bcc` (2022, CASSANDRA-15234, config rename). The legacy socket dates
  from the 4.0 messaging rewrite `310a48efcfd` (2019, CASSANDRA-15066). Invariant: that socket opens only on explicit
  opt-in.
* `src/java/org/apache/cassandra/streaming/async/NettyStreamingConnectionFactory.java` — `connect`: an early `break` in
  the fallback loop. — History: `ca75ffe4d09` (2022, CASSANDRA-17923).

**Failure and rollback risks**
* Under `internode_encryption: dc|rack` in the mode, a peer whose version is unknown (including a node of this software
  right after a restart, before any handshake or gossip) gets no plain-text fallback. If that peer accepts only plain
  text, connections keep failing; a WARN is logged once per peer for the life of the process. This is intended per the
  commit message, but it covers more than DSE peers.
* A port wrongly chosen (a stale release, or a version unknown with `legacy_ssl_storage_port_enabled` true towards DSE
  6.9.7+) makes connections fail until gossip corrects the version. Recovery depends on `dseMessagingVersion` re-reading
  gossip on every attempt, which it does.
* Rollback to DSE: no persistent state is written. The extra listener disappears with the process.
* The extra listener adds an open port in the mode. It is TLS only, and the dtest asserts that a plain connection is
  refused.

**Test integrity**
* Existing tests changed: `DseHandshakeTest` (26 lines added, none removed). No assertion weakened.
* New tests: `Dse6InternodeTlsPortTest` is a real two-node in-JVM dtest with TLS keystores, but only for the inbound
  side and between two nodes of this software at 4.0+; its Javadoc says no node dials the way DSE does.
  `DseTlsFallbackTest` uses real sockets (`InboundSockets` on 127.0.0.41 to .55, Linux loopback) for the fallback
  refusal, and table-driven decisions for every encryption setting and version source. `DseTlsPortsTest` is a unit test
  of the port rules, with gossip state and stored releases set in-process.
* Not covered by any test in the patch: an outbound connection that actually lands on `ssl_storage_port` at the dialect
  end to end (only the decision function is tested); a connection to a real DSE node of either port model; streaming to
  a DSE peer; a bind failure of the extra listener.

**Suggested reviewer questions**
* Should the no-plain-fallback rule be limited to peers identified or presumed as DSE (`DsePeers`) instead of every peer
  whose version is unknown?
* For a DSE 6.9.7+ peer whose version is unknown, is following the local `legacy_ssl_storage_port_enabled` right, or
  should an unknown DSE 6 release default to `ssl_storage_port`, which every DSE 6 model opens when encryption is on?
* Must the start be refused when the forced `ssl_storage_port` listener cannot bind, and is that documented for
  operators?
* Is the `4.0.0.<build>` to DSE version parsing (`dse6MinorAndPatch`) unambiguous for every 6.8/6.9 build in scope?

## Group notes (P12, P18)
* P12 and P18 are siblings, both based on P11 (`dse6/p11-handshake`) and independent of each other. Both edit
  `test/unit/org/apache/cassandra/net/DseHandshakeTest.java` and both read `DsePeers.gossipedDseVersion`, so expect a
  test-file merge when they are stacked.
* P18 reads the DSE version from the gossip `NET_VERSION` state (`DseTlsPorts.dseMessagingVersion`), not from
  `MessagingService.versions`. That keeps it correct after P12, where `updateNetVersion` stores 11 instead of DSE's raw
  value. Its `knownVersion` still maps a raw 256+ value to the dialect, which only matters without P12. P12 makes nodes
  in the mode gossip `NET_VERSION` 12. P18 treats values below 256 as "not DSE" and so falls back to the local
  `legacy_ssl_storage_port_enabled` for those peers. Read together, they are consistent.
* Both patches extend the "assumed at the dialect from a stored DSE 6 release" seeding of P11
  (`DseDialectVersions.seed`). P12 adds the presumption (`DsePeers.presume`), and P18 uses the same stored release to
  choose the port. A stale stored release therefore affects both the gossip content (P12 withholds states) and the port
  dialled (P18), until gossip corrects it.
* P12 replaces HCD-241's pre-4.0 gossip translation from `main` (`820a3c0ab0e`). A reviewer familiar with HCD-241
  should confirm that nothing outside this series relied on the old `filterIncomingStates` / `filterOutgoingStates`
  behaviour.
