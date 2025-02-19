// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.flink.sink;

import org.apache.doris.flink.exception.DorisRuntimeException;
import org.apache.doris.flink.rest.models.BackendV2;
import org.apache.doris.flink.rest.models.BackendV2.BackendRowV2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BackendUtil {
    private static final Logger LOG = LoggerFactory.getLogger(BackendUtil.class);
    private final List<BackendV2.BackendRowV2> backends;
    private long pos;

    public BackendUtil(List<BackendV2.BackendRowV2> backends) {
        this.backends = backends;
        this.pos = 0;
    }

    public BackendUtil(String beNodes) {
        this.backends = initBackends(beNodes);
        this.pos = 0;
    }

    private List<BackendV2.BackendRowV2> initBackends(String beNodes) {
        List<BackendV2.BackendRowV2> backends = new ArrayList<>();
        List<String> nodes = Arrays.asList(beNodes.split(","));
        nodes.forEach(node -> {
            if (tryHttpConnection(node)) {
                node = node.trim();
                String[] ipAndPort = node.split(":");
                BackendRowV2 backendRowV2 = new BackendRowV2();
                backendRowV2.setIp(ipAndPort[0]);
                backendRowV2.setHttpPort(Integer.parseInt(ipAndPort[1]));
                backendRowV2.setAlive(true);
                backends.add(backendRowV2);
            }
        });
        return backends;
    }

    public String getAvailableBackend() {
        long tmp = pos + backends.size();
        while (pos < tmp) {
            BackendV2.BackendRowV2 backend = backends.get((int) (pos++ % backends.size()));
            String res = backend.toBackendString();
            if (tryHttpConnection(res)) {
                return res;
            }
        }
        throw new DorisRuntimeException("no available backend.");
    }

    public boolean tryHttpConnection(String backend) {
        try {
            backend = "http://" + backend;
            URL url = new URL(backend);
            HttpURLConnection co =  (HttpURLConnection) url.openConnection();
            co.setConnectTimeout(60000);
            co.connect();
            co.disconnect();
            return true;
        } catch (Exception ex) {
            LOG.warn("Failed to connect to backend:{}", backend, ex);
            return false;
        }
    }
}
