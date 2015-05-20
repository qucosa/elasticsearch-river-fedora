/*
 * Copyright 2015 SLUB Dresden
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

package de.slub.fedora.oai;

import java.util.Date;

public class OaiRunResult {
    private final Date expirationDate;
    private final String resumptionToken;
    private final Date timestamp;

    public OaiRunResult(Date timestamp, Date expirationDate, String resumptionToken) {
        this.timestamp = timestamp;
        this.expirationDate = expirationDate;
        this.resumptionToken = resumptionToken;
    }

    public OaiRunResult() {
        this(null, null, null);
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public Date getExpirationDate() {
        return expirationDate;
    }

    public String getResumptionToken() {
        return resumptionToken;
    }

    public boolean isInFuture(Date date) {
        return timestamp != null && timestamp.after(date);
    }

    public boolean hasTimestamp() {
        return timestamp != null;
    }

    public boolean hasResumptionToken() {
        return resumptionToken != null && !resumptionToken.isEmpty();
    }

    public boolean isValidResumptionToken(Date now) {
        return resumptionToken != null
                && !resumptionToken.isEmpty()
                && (expirationDate == null || expirationDate.after(now));
    }

}
