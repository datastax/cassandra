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

package org.apache.cassandra.db.marshal;

/**
 * Named boolan to express whether some sensistive data should be presented in clear or redacted.
 * This is generally applied to column values, or things containing column values.
 * Column values should be redacted when printed in logs.
 * They shouldn't be redacted when used in user-facing error messages or query tracing.
 */
public enum Privacy
{
    NONE, REDACT
}
