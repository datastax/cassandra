/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.tools.dsepreflight;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * One finding of the preflight inspection: what was found, how serious it is, and what to do on DSE before the upgrade.
 * Serialized as is in the JSON report; the codes are stable, the texts are for people.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class Finding
{
    public enum Severity
    {
        /** the node cannot be upgraded until it is resolved: the tool exits with a non-zero code */
        BLOCKING,
        /** the upgrade can go ahead, but something is lost or must be done */
        WARNING,
        /** for the record */
        INFO
    }

    @JsonProperty("severity")
    public Severity severity;
    /** a stable identifier of the kind of finding, such as {@code SOLR_DATA} */
    @JsonProperty("code")
    public String code;
    @JsonProperty("message")
    public String message;
    /** what to do about it; null when nothing is to be done */
    @JsonProperty("remedy")
    public String remedy;

    /** For the JSON reader. */
    public Finding()
    {
    }

    public Finding(Severity severity, String code, String message, String remedy)
    {
        this.severity = severity;
        this.code = code;
        this.message = message;
        this.remedy = remedy;
    }

    static Finding blocking(String code, String message, String remedy)
    {
        return new Finding(Severity.BLOCKING, code, message, remedy);
    }

    static Finding warning(String code, String message, String remedy)
    {
        return new Finding(Severity.WARNING, code, message, remedy);
    }

    static Finding info(String code, String message)
    {
        return new Finding(Severity.INFO, code, message, null);
    }

    @Override
    public String toString()
    {
        return severity + " " + code + ": " + message + (remedy == null ? "" : " Remedy: " + remedy);
    }
}
