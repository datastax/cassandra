-- SAI JSON path indexing POC demo. Run: bin/cqlsh -f docs/superpowers/poc/sai-json-path-demo.cql
DROP KEYSPACE IF EXISTS json_poc;
CREATE KEYSPACE json_poc WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
CREATE TABLE json_poc.users (id int PRIMARY KEY, doc text);

CREATE CUSTOM INDEX users_age ON json_poc.users(doc) USING 'StorageAttachedIndex'
  WITH OPTIONS = {'json_path': '$.profile.age', 'json_type': 'int'};
CREATE CUSTOM INDEX users_tags ON json_poc.users(doc) USING 'StorageAttachedIndex'
  WITH OPTIONS = {'json_path': '$.tags', 'json_type': 'text'};
CREATE CUSTOM INDEX users_city ON json_poc.users(doc) USING 'StorageAttachedIndex'
  WITH OPTIONS = {'json_path': '$.address.city', 'json_type': 'text'};

-- 1: tags array, age 25, stockholm
INSERT INTO json_poc.users (id, doc) VALUES (1, '{"profile": {"age": 25}, "tags": ["admin", "dev"], "address": {"city": "stockholm"}}');
-- 2: tags scalar, age 40, warsaw
INSERT INTO json_poc.users (id, doc) VALUES (2, '{"profile": {"age": 40}, "tags": "admin", "address": {"city": "warsaw"}}');
-- 3: age 30.0 (counts as 30), stockholm
INSERT INTO json_poc.users (id, doc) VALUES (3, '{"profile": {"age": 30.0}, "tags": ["user"], "address": {"city": "stockholm"}}');
-- 4: no profile
INSERT INTO json_poc.users (id, doc) VALUES (4, '{"tags": ["admin", "ops"], "address": {"city": "paris"}}');
-- 5: no hit
INSERT INTO json_poc.users (id, doc) VALUES (5, '{"tags": ["ops"], "address": {"city": "wroclaw"}}');

-- expected: 2
SELECT id FROM json_poc.users WHERE doc['$.profile.age'] > 30;
-- expected: 1, 3
SELECT id FROM json_poc.users WHERE doc['$.address.city'] = 'stockholm';
-- expected: 1, 2, 4
SELECT id FROM json_poc.users WHERE doc['$.tags'] = 'admin';
-- expected: 2
SELECT id FROM json_poc.users WHERE doc['$.profile.age'] > 30 AND doc['$.tags'] = 'admin';
