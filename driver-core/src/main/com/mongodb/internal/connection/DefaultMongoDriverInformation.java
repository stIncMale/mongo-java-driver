/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.internal.connection;

import com.mongodb.MongoDriverInformation;
import com.mongodb.internal.client.DriverInformation;
import com.mongodb.internal.client.DriverInformationHelper;

import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * The MongoDriverInformation class allows driver and library authors to add extra information about their library. This information is
 * then available in the MongoD/MongoS logs.
 *
 * <p>
 *     The following metadata can be included when creating a {@code MongoClient}.
 * </p>
 * <ul>
 *     <li>The driver name. Eg: {@code mongo-scala-driver}</li>
 *     <li>The driver version. Eg: {@code 1.2.0}</li>
 *     <li>Extra platform information. Eg: {@code Scala 2.11}</li>
 * </ul>
 * <p>
 *     Note: Library authors are responsible for accepting {@code MongoDriverInformation} from external libraries using their library.
 *     Also all the meta data is limited to 512 bytes and any excess data will be truncated.
 * </p>
 *
 * @since 3.4
 * @mongodb.server.release 3.4
 */
public final class DefaultMongoDriverInformation implements MongoDriverInformation {
    private final List<DriverInformation> driverInformationList;

    /**
     * Returns the driverNames
     *
     * @return the driverNames
     */
    @Override
    public List<String> getDriverNames() {
        return DriverInformationHelper.getNames(driverInformationList);
    }

    /**
     * Returns the driverVersions
     *
     * @return the driverVersions
     */
    @Override
    public List<String> getDriverVersions() {
        return DriverInformationHelper.getVersions(driverInformationList);
    }

    /**
     * Returns the driverPlatforms
     *
     * @return the driverPlatforms
     */
    @Override
    public List<String> getDriverPlatforms() {
        return DriverInformationHelper.getPlatforms(driverInformationList);
    }

    public List<DriverInformation> getDriverInformationList() {
        return driverInformationList;
    }

    public DefaultMongoDriverInformation(final List<DriverInformation> driverInformation) {
        this.driverInformationList = notNull("driverInformation", driverInformation);
    }
}
