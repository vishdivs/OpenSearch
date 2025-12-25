/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.throttling;

public class AwsThrottlingSupplier {
    private static AwsThrottlingService awsThrottlingService = null;

    public static AwsThrottlingService getAwsThrottlingService() {
        return awsThrottlingService;
    }

    public static void setAwsThrottlingService(AwsThrottlingService service) {
        awsThrottlingService = service;
    }

}
