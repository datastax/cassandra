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

# Security: DSE access controls this software does not enforce, and internode encryption

**Read this before upgrading a DSE cluster.** Sections 1 to 4 are written for the upgrade from DSE 6.8 and 6.9; section
5 says what differs for the upgrade from DSE 5.1.51, whose procedure is `RUNBOOK-DSE51.md`. The runbook (`RUNBOOK.md`,
version 2.2, with its reference `RUNBOOK-REFERENCE.md`) reproduces these rules in its section "Security first"; the
preflight tool (`dsepreflight`, runbook section 1 item 0) blocks on every control of section 1 below and warns on the
controls of section 2 that carry a code in brackets (except LDAP, Kerberos and LDAP role management, which block,
`AUTH_SCHEME`), and an upgraded node refuses to start on the controls it can see (section 1).

Sections 1 to 3 are about DSE's access controls; section 4 is about internode encryption during the upgrade; section 5
is the upgrade from DSE 5.1.

DataStax Enterprise stores access controls that this software does not understand. Anything that *restricts* access in
DSE and is silently ignored here would **grant access after the upgrade**. The project therefore **fails fast**: a node
refuses to start in `internode_compatibility_mode: DSE_6` while such controls exist, the same rule as for the reads only
DSE can serve. The operator removes or replaces them on DSE before upgrading the first node.

## 1. Controls that would open access if ignored: the upgrade is refused

| DSE control | Where DSE stores it | What would happen if ignored | Status |
|---|---|---|---|
| `RESTRICT` (per-role deny list, e.g. `RESTRICT SELECT ON ks.t TO analyst`) | `system_auth.role_permissions.restricted` (the only store; a restriction on any role or parent resource wins) | The role regains every permission the restriction removed, typically through a grant inherited from another role. Nothing is logged. | The preflight blocks (`RESTRICT`) and an upgraded node refuses to start. Remedy on DSE before the upgrade: `UNRESTRICT`. Override: `-Dcassandra.internode_compatibility_mode.ignore_dse_restrictions=true`. |
| Row-level access control (RLAC: `RESTRICT ROWS ON <ks>.<table> USING <column>`, then `GRANT … ON ROWS` to targets) | `DSE_RLACA` key in the table `extensions` (value: the tenant column name), `rows/<target>/data/<ks>/<table>` resources in `system_auth.role_permissions`; enabled by `authorization_options.allow_row_level_security` in `dse.yaml` | A role limited to some rows of a table reads every row of it. | The preflight blocks (`ROW_LEVEL_ACCESS`) and an upgraded node refuses both the `rows/` grants and any table with `DSE_RLACA`, whether or not RLAC is enabled in `dse.yaml` (fail closed). Remedy on DSE before the upgrade: `UNRESTRICT ROWS ON <ks>.<table>` and `REVOKE` the row grants, then set `allow_row_level_security: false` (the preflight blocks while it is true). Override: `-Dcassandra.internode_compatibility_mode.ignore_dse_restrictions=true`. |
| Scheme permissions (`authentication_options.scheme_permissions: true` in `dse.yaml`, with `authentication_options.enabled: true`: a role logs in only with the schemes it has `EXECUTE` on) | `dse.yaml`, and `EXECUTE` grants on `authentication_schemes/<scheme>` resources in `system_auth.role_permissions` | A role DSE keeps out could log in. | The preflight blocks (`SCHEME_PERMISSIONS`). An upgraded node does not read `dse.yaml`, so only the preflight catches it. Remedy on DSE before the upgrade: give the roles that must log in `EXECUTE` on their scheme, make the others unable to log in (`ALTER ROLE … WITH LOGIN = false`), then set `scheme_permissions: false`. |

**The check at startup.** An upgraded node refuses on the table extension key `DSE_RLACA`, the only one DSE writes (by
`RESTRICT ROWS`, removed by `UNRESTRICT ROWS`). Other extension keys are not DSE access controls; the first startup pass
lists them in one INFO line. The check reads `system_auth.role_permissions` and the schema at startup in `DSE_6` mode.
It refuses when any row has a non-empty `restricted` set or a `rows/…` resource granted permissions, or when a table
carries `DSE_RLACA`. The refusal lists the roles, resources and tables (with the tenant column) and the statements to
run on DSE, for example:
```
internode_compatibility_mode DSE_6 refused: DSE access controls that this software does not enforce would grant access if ignored (found by this node's replica): row-level restricted tables in the schema [ks.t (tenant column tenant)]. Roll this node back to DSE, remove them there (UNRESTRICT ROWS ON ks.t) and upgrade it again; or start with -Dcassandra.internode_compatibility_mode.ignore_dse_restrictions=true to drop them knowingly
```
The check runs twice at startup: on this node's replica of `system_auth.role_permissions` (source `this node's
replica`), then, once the node has joined the ring, on a read of the whole table at `auth_read_consistency_level`
(`LOCAL_QUORUM` by default; source `a LOCAL_QUORUM read`), retried for up to
`cassandra.internode_compatibility_mode.dse_restrictions_check_wait_seconds` (120 s) while the replicas are not
reachable (runbook section 3, "Starting an upgraded node while other replicas are down"). The same list appears in
`nodetool compatstatus` (`DSE access controls ignored`) and, from the check repeated every 10 minutes, at ERROR. The
override is logged at `WARN` at every start, and again for what it drops (at startup, then for each control the
periodic check finds added). **Limitation:** the tables come from each upgraded node's own schema, which DSE DDL does
not reach during the upgrade: a `RESTRICT ROWS` run on DSE after a node was upgraded is not detected, so `GRANT`,
`REVOKE`, `RESTRICT`, `UNRESTRICT`, `RESTRICT ROWS` and `UNRESTRICT ROWS` must not be run on DSE nodes during the
upgrade.

## 2. Controls that are dropped and remove access (fail closed)

These do not open access, but users can lose access they had. The node starts and logs a warning; the preflight
warns with the code in brackets (it blocks on LDAP, Kerberos and LDAP role management, `AUTH_SCHEME`, last row).

| DSE control | Effect after the upgrade | What to do |
|---|---|---|
| `AUTHORIZE FOR` delegations (`system_auth.role_permissions.grantables`) [`DELEGATION`] | The delegate can no longer grant the permission (WARN `DSE AUTHORIZE FOR delegation in system_auth.role_permissions is not enforced by this software and is dropped …`). | Re-grant `AUTHORIZE` where needed, once the upgrade is complete. |
| The pair `UPDATE` + `TRUNCATE` on one **data** resource (table, keyspace, all keyspaces): how DSE 6.8 and 6.9 store the permissions of a table's creator and a `GRANT ALL` on a table (tested on DSE 6.8.65 and 6.9.26; DSE 6.8.65 also stores the creator's permissions on a keyspace as the pair), and what a `GRANT UPDATE` followed by a `GRANT TRUNCATE` leaves. DSE's `GRANT MODIFY` stores `MODIFY` itself (with a deprecation warning), read as is. *(From a test on DSE 6.8.65 and 6.9.26.)* | Read as this software's `MODIFY`, which covers exactly inserts, updates, deletes and truncate: no access change (INFO, once). In `DSE_6`, `GRANT MODIFY` through an upgraded node writes the pair, no literal `MODIFY`, so DSE nodes and a rolled-back node honour it. An upgraded node's `REVOKE MODIFY` removes `MODIFY` and the pair; DSE's `REVOKE MODIFY` removes only a literal `MODIFY` and leaves the pair, so after it the role can still write and truncate. On non-data resources (e.g. MBeans) the two names stay unknown. | Nothing during the upgrade, when no `GRANT` or `REVOKE` is run (runbook, "Security first", item 4). If a `GRANT MODIFY` was run on an upgraded node all the same, it is stored as the pair: once the upgrade is complete, `REVOKE MODIFY` removes it; after a rollback to DSE, `REVOKE UPDATE` and `REVOKE TRUNCATE` do (and `REVOKE MODIFY` if the role also held a `MODIFY` granted on DSE), since DSE's `REVOKE MODIFY` leaves the pair. Before the upgrade, on DSE, `REVOKE UPDATE` or `REVOKE TRUNCATE` does not touch a literal `MODIFY`: to narrow it, `REVOKE MODIFY`, then grant the one to keep (this software then drops it, next row). |
| `UPDATE` alone, or `TRUNCATE` alone, on a data resource [`LONE_WRITE_GRANT`] | Dropped: mapping either to `MODIFY` would also grant the other and widen access. The role loses what it gave (writes, or truncate). One warning per role and resource; in `DSE_6` the access-control check lists them (one warning from its second startup pass, new ones from the periodic check); `nodetool compatstatus` shows them in the line `DSE UPDATE/TRUNCATE alone`, in every mode. | Before upgrading, `GRANT MODIFY` on DSE where both are wanted, or accept the loss. |
| `GRANT ALL` on a keyspace or table in `DSE_6` mode (and the creator grant with `allow_ddl`) | `UNMASK` and `SELECT_MASKED` are not written, because DSE, which reads the same table, does not have them; the client is warned. An explicit `GRANT UNMASK`/`SELECT_MASKED` is refused (runbook appendix A). | Grant them once every node runs `internode_compatibility_mode: NONE`. |
| Other DSE-only permissions (for example `PROXY.LOGIN`, `PROXY.EXECUTE`, the `SEARCH` permissions) [`PROXY_GRANT`, `DSE_ONLY_PERMISSION`] | Ignored, with one WARN per permission name (`Ignoring the unknown permission '<name>' granted to role …`): the role loses what they gave. DSE grants the proxy permissions to the creator of every role, so most clusters have them (hence a warning). This software has no proxy login or proxy execution. | Make every application that uses proxy login or execution log in as the role it needs before the upgrade; grant the nearest equivalent of the others. |
| DSE-only resources (for example `search/…`, `rpc/…`, `authentication_schemes/…`) [`DSE_ONLY_RESOURCE`] | Ignored, with one WARN per resource type (`Ignoring the grants to role '<role>' on resource '<resource>' found in system_auth.role_permissions: resources of type …`). | Check that no application relies on them. |
| DSE authentication and role management: `DseAuthenticator`, `DseRoleManager`, `DseAuthorizer` [`AUTH_CONFIG_CHANGE`, `AUTH_SCHEME`, `TRANSITIONAL_MODE`] | Not in this software. With the internal scheme only, the roles and grants in `system_auth` are kept and the classes change: `PasswordAuthenticator`, `CassandraRoleManager`, `CassandraAuthorizer` (the preflight's "Settings to carry"). LDAP or Kerberos schemes, or `role_management_options.mode: ldap`, block the upgrade. `authentication_options.transitional_mode` other than `disabled` warns (`TRANSITIONAL_MODE`): HCD 2.0 lets no client in without a valid login. | Move every role to internal authentication on DSE before the upgrade; set the classes of the preflight report in HCD 2.0's `cassandra.yaml`. |

The rule: the `UPDATE` + `TRUNCATE` pair becomes `MODIFY`; a single one is never widened to `MODIFY`
(security first: no access is granted that DSE did not grant).

## 3. During the mixed phase

While DSE and upgraded nodes coexist, each node enforces its own rules: the same request can be allowed by one
coordinator and refused by another. The fail-fast check of section 1 removes the dangerous case (a DSE restriction
ignored by an upgraded coordinator); the fail-closed cases of section 2 can still make requests coordinated by
upgraded nodes fail where DSE nodes accept them (an application that logs in by proxy, for example).

## 4. Internode encryption: no plain-text retry towards DSE nodes

Upstream, with optional internode encryption, a node retries a failed TLS connection with each fallback in turn, the
last one in plain text, for its message connections and its streams. `internode_encryption: dc` and `rack` make the
encryption optional whatever `optional` in `server_encryption_options` says, so under `dc` or `rack` a failed TLS
connection to a node of another data center or rack could be retried unencrypted. In `internode_compatibility_mode:
DSE_6` an upgraded node never retries in plain text a connection its configuration encrypts (`all`, or `dc` and `rack`
towards another data center or rack) to a peer it does not know at messaging version 4.0 or later: a DSE node, or a
node whose version it has not learned yet. It retries messages with TLS, and stops a stream after its encrypted
attempts. Nodes known at 4.0 or later (the other upgraded nodes), the connections the configuration does not encrypt,
and a node in `NONE` keep the upstream behaviour.

- TLS must work between the upgraded nodes and every DSE node they encrypt to: with `all` every DSE node, with `dc`
  those of the other data centers, with `rack` those of the other racks (runbook section 1, "Internode TLS: no
  plain-text retry towards DSE nodes").
- With `dc` or `rack`, setting `optional: false` changes nothing: the encryption stays optional.
- A peer that accepts only plain text where the upgraded node encrypts stays unreachable from the upgraded nodes until
  it accepts TLS. The case to look for: a cluster set to `all` without `optional: false` (unset counts as `true` when a
  keystore is set) in which a DSE node has no internode TLS yet; upstream an upgraded node would reach it in plain text,
  in `DSE_6` it does not. Turning internode TLS on is finished on DSE before the upgrade starts.
- The upgraded node logs, once per peer: `DSE 6 compatibility mode: refusing the unencrypted fallback of the internode
  connections to <peer>: internode_encryption (<setting>) encrypts the connections to this peer and <reason>, so a
  failed TLS connection is not retried in plain text: the connections to it keep failing while the peer does not accept
  TLS on port <port>; check the peer's internode_encryption and ssl_storage_port, and the certificates and truststores
  of both nodes`.

*(Not validated: unit tests only.)*

## 5. The upgrade from DSE 5.1

In `internode_compatibility_mode: DSE_5_1` the same rule holds: a node refuses to start while DSE 5.1 access controls
it does not enforce exist, and drops, with a warning, those whose loss only removes access. What differs from DSE 6:

| DSE 5.1 control | What this software does | Remedy on DSE 5.1, before the first node |
|---|---|---|
| Row-level access control on a **table** (`RESTRICT ROWS`: the `DSE_RLACA` extension) and `rows/<target>/data/<ks>/<table>` grants | Refused at start, whether or not `allow_row_level_security` is on in `dse.yaml`, and the `rows/` grants whether or not their table is still restricted; the preflight blocks (`ROW_LEVEL_ACCESS`) | `UNRESTRICT ROWS ON <ks>.<table>`, then `REVOKE` each `rows/` grant, which `UNRESTRICT ROWS` leaves; then `allow_row_level_security: false` |
| Row-level access control on a **materialized view** | Refused at start, from the schema of the node (`RESTRICTED_VIEW` in the preflight, its aggregate naming the nodes whose schema holds it: DSE 5.1 keeps a view's `RESTRICT ROWS` in the schema of the node that ran it only, until that node restarts, then every node takes it) | `REVOKE` the view's `rows/` grants, drop the view and create it again without the restriction (validated on DSE 5.1.51). Never `UNRESTRICT ROWS` on a view: on DSE 5.1.51 it fails and leaves on that node a row of `system_schema.tables` named after the view, with which neither DSE 5.1.51 nor HCD 2.0 starts the node (`RUNBOOK-DSE51.md` section 6 repairs it) |
| A read coordinated by a DSE 5.1 node that carries DSE's row-level access control expression | Refused on an upgraded replica, and counted in `nodetool compatstatus` | None: the controls are removed before the first node. If a read is refused (a `RESTRICT ROWS` run during the upgrade, or a restriction an upgraded node dropped knowingly), upgrade no further node and report it: the restriction is removed on DSE 5.1, as the two rows above say (never `UNRESTRICT ROWS` on a view), only once every upgraded node is rolled back |
| Proxy login and proxy execution (`PROXY.LOGIN`, `PROXY.EXECUTE`) | Read as no grant, with the WARN `Ignoring the unknown permission '<name>' granted to role …` (removes access) | Applications log in as the role they need (`PROXY_GRANT`) |
| `MODIFY` | DSE 5.1 stores `MODIFY` as one name (it has no `UPDATE` or `TRUNCATE` permission), read as is; in the mode a `GRANT MODIFY` writes `MODIFY`, as DSE 5.1 does | Nothing |
| `GRANT ALL`, `UNMASK`, `SELECT_MASKED` | As section 2 says for DSE 6: not written by `GRANT ALL`, an explicit grant refused, until the flag drop | Grant them after the flag drop |
| `RESTRICT`, `AUTHORIZE FOR` | DSE 5.1 has neither | Nothing |
| Scheme permissions, LDAP, Kerberos, LDAP role management, the DSE security classes | As sections 1 and 2 say (`SCHEME_PERMISSIONS`, `AUTH_SCHEME`, `AUTH_CONFIG_CHANGE`, `TRANSITIONAL_MODE`) | As sections 1 and 2 say |

**The check at startup.** In `DSE_5_1` the check reads the raw rows of `system_schema` before the schema is loaded, then
`system_auth.role_permissions` and the schema at startup, and again every 10 minutes. When the read of the whole table
at `auth_read_consistency_level` still fails after `dse_restrictions_check_wait_seconds` (120 s) on the request
(replicas unavailable, a timeout, a read failure), a node in `DSE_5_1` starts on the check of its own replica, with a
WARN, when that replica holds DSE 5.1's table and nothing it finds is refused: DSE 5.1 has no `RESTRICT` and no
delegation, and the restrictions it applies on the node are in the schema the node checked. With DSE 5.1's default of
one replica of `system_auth` that wait comes at every start while another node is down. The override
`-Dcassandra.internode_compatibility_mode.ignore_dse_restrictions=true` drops the controls knowingly, as for DSE 6.

**Settings that need tables DSE 5.1 lacks** are refused at start in the mode: `network_authorizer`
`CassandraNetworkAuthorizer` (use `AllowAllNetworkAuthorizer` until the mode is dropped), `cidr_authorizer`
`CassandraCIDRAuthorizer` and the mutual TLS authenticators (this software creates their tables only with
`storage_compatibility_mode` `UPGRADING` or `NONE`, after the upgrade). The reverse index of the permissions
(`system_auth.resource_role_permissons_index`), which a DSE 5.1.51 install does not have, is not maintained in the mode
while DSE's `system_auth` lacks it; the first start in `NONE` adds it, empty, and the flag drop rebuilds it before any
`DROP` (`RUNBOOK-DSE51.md` section 5, F4).

**Internode encryption.** Section 4 holds in `DSE_5_1`: an upgraded node never retries in plain text a connection it
encrypts to a DSE 5.1 node, or to a node whose version it does not know yet (the WARN starts `DSE 5.1 compatibility
mode: refusing the unencrypted fallback …`). It dials every DSE 5.1 node it encrypts to on `ssl_storage_port`, the only
port on which DSE 5.1 accepts TLS, once it knows that node speaks the pre-4.0 dialect, and listens there for the DSE 5.1
nodes too; keep `legacy_ssl_storage_port_enabled: false`. DSE 5.1 runs on Java 8, HCD 2.0 on JDK 17, which no longer
enables some older protocols and cipher suites: the preflight reports the TLS settings (`TLS`, a note) and never blocks
on them; check that the two negotiate before the first node. *(Not validated: unit tests only; no TLS connection between
DSE 5.1 and HCD 2.0 has run.)*

## Open items

- Known: `RESTRICT` lives only in `role_permissions.restricted`; RLAC is the `DSE_RLACA` table extension plus
  `rows/…` grants. Still open, without effect on the refusal: how DSE treats a role with table-level `SELECT` on a
  table restricted to rows while RLAC is on.
- Known: DSE 6.8.65 and 6.9.26 store `GRANT MODIFY` as `MODIFY`, with a deprecation warning naming `UPDATE` and
  `TRUNCATE`; they store the permissions of a table's creator and a `GRANT ALL` on a table as the pair `UPDATE` +
  `TRUNCATE`; `GRANT UPDATE` and `GRANT TRUNCATE` exist on both and store the name alone (a lone `UPDATE` allows writes,
  that is inserts, updates and deletes, but not truncation; a lone `TRUNCATE` truncation but not writes); DSE's
  `REVOKE MODIFY` removes only a literal `MODIFY`, and its `REVOKE UPDATE` and `REVOKE TRUNCATE` never touch one.
- Override `ignore_dse_peer_workloads`: skips only DSE's `nodes/peers` workload records (stale entries
  or an unreadable file); it opens no access and cannot hide a search index, a non-core keyspace, `solr.data` or this
  node's own workload.
