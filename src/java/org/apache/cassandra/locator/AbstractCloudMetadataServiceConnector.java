/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.locator;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import static java.nio.charset.StandardCharsets.UTF_8;

abstract class AbstractCloudMetadataServiceConnector
{
    protected final String metadataServiceUrl;

    protected AbstractCloudMetadataServiceConnector(String metadataServiceUrl)
    {
        this.metadataServiceUrl = metadataServiceUrl;
    }

    public String apiCall(String query) throws IOException
    {
        return apiCall(metadataServiceUrl, query, 200);
    }

    public String apiCall(String url, String query, int expectedResponseCode) throws IOException
    {
        return apiCall(url, query, "GET", ImmutableMap.of(), expectedResponseCode);
    }

    public String apiCall(String url,
                          String query,
                          String method,
                          Map<String, String> extraHeaders,
                          int expectedResponseCode) throws IOException
    {
        HttpURLConnection conn = null;
        try
        {
            // Populate the region and zone by introspection, fail if 404 on metadata
            conn = (HttpURLConnection) new URL(url + query).openConnection();
            extraHeaders.forEach(conn::setRequestProperty);
            conn.setRequestMethod(method);
            if (conn.getResponseCode() != expectedResponseCode)
                throw new HttpException(conn.getResponseCode(), conn.getResponseMessage());

            // Read the information. I wish I could say (String) conn.getContent() here...
            int cl = conn.getContentLength();

            if (cl == -1)
                return null;

            byte[] b = new byte[cl];
            try (DataInputStream d = new DataInputStream((InputStream) conn.getContent()))
            {
                d.readFully(b);
            }
            return new String(b, UTF_8);
        }
        finally
        {
            if (conn != null)
                conn.disconnect();
        }
    }

    public static final class HttpException extends IOException
    {
        public final int responseCode;
        public final String responseMessage;

        public HttpException(int responseCode, String responseMessage)
        {
            super("HTTP response code: " + responseCode + " (" + responseMessage + ')');
            this.responseCode = responseCode;
            this.responseMessage = responseMessage;
        }
    }
}
