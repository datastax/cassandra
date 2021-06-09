# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import random
from cassandra.cluster import Cluster
from cassandra.policies import WhiteListRoundRobinPolicy, RoundRobinPolicy
from cassandra.pool import HostDistance


class EndpointWhiteListRoundRobinPolicy(WhiteListRoundRobinPolicy):
    def __init__(self, hosts):
        self._allowed_hosts = self._allowed_hosts_resolved = tuple(hosts)
        RoundRobinPolicy.__init__(self)

    def populate(self, cluster, hosts):
        self._live_hosts = frozenset(h for h in hosts if h.endpoint in self._allowed_hosts_resolved)

        if len(hosts) <= 1:
            self._position = 0
        else:
            self._position = random.randint(0, len(hosts) - 1)

    def distance(self, host):
        if host.endpoint in self._allowed_hosts_resolved:
            return HostDistance.LOCAL
        else:
            return HostDistance.IGNORED

    def on_up(self, host):
        if host.endpoint in self._allowed_hosts_resolved:
            RoundRobinPolicy.on_up(self, host)

    def on_add(self, host):
        if host.endpoint in self._allowed_hosts_resolved:
            RoundRobinPolicy.on_add(self, host)


def create_cloud_cluster(secure_connect_bundle_path, cql_version, auth_provider, connect_timeout, **kwargs):
    """
    Create cluster connected to the cloud C* instance using provided secure connect bundle.
    
    See https://docs.datastax.com/en/developer/python-driver/3.25/cloud/
    """
    cloud_config = {
        'secure_connect_bundle': secure_connect_bundle_path,
        'use_default_tempdir': True,
    }
    options = kwargs.copy()
    cluster = Cluster(cloud=cloud_config, 
                   auth_provider=auth_provider,
                   connect_timeout=connect_timeout,
                   control_connection_timeout=connect_timeout,
                   **options)
    # applying load balancing policy as we know now the contact points
    # we need to randomly select one endpoint as it may be behind ingress
    endpoints = [random.choice(cluster.contact_points)]
    cluster.load_balancing_policy = EndpointWhiteListRoundRobinPolicy(endpoints)
    for ep in cluster.profile_manager.profiles.values():
        ep.load_balancing_policy = EndpointWhiteListRoundRobinPolicy(endpoints)
    cluster.contact_points_resolved = endpoints
    return cluster
