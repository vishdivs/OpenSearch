/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.throttling;

import com.amazonaws.throttling.client.ThrottlingClient;
import com.amazonaws.throttling.client.ThrottlingClientConfig;
import com.amazonaws.throttling.client.ThrottlingDimensions;
import com.amazonaws.throttling.client.ThrottlingHandle;
import com.amazonaws.throttling.client.SyncFailBehavior;
import com.amazonaws.throttling.client.NativeCache;
import com.amazonaws.throttling.client.NativeQuerylog;
import com.amazonaws.throttling.client.QuerylogConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.rest.RestRequest;

import java.io.IOException;

/**
 * AWS Throttling service integration for OpenSearch rate limiting
 */
public class AwsThrottlingService extends AbstractLifecycleComponent {
    private static final Logger logger = LogManager.getLogger(AwsThrottlingService.class);
    private static final String RULESET_NAME = "JunoAwsThrottlingRuleSet-gamma-eu-north-1";
    private static final int CACHE_SIZE = 10000;
    private static final String ASSUME_ROLE_ARN = "arn:aws:iam::961814724801:role/MultiTenantRole";
    private final ThrottlingClient throttlingClient;
    private final boolean enabled;

    public AwsThrottlingService(boolean enabled) {
        this.enabled = enabled;
        this.throttlingClient = enabled ? initializeClient() : null;
        logger.info("AWS Throttling Service initialized with enabled={}",this.throttlingClient);

    }

    private ThrottlingClient initializeClient() {
        try {
            final NativeCache cache = new NativeCache(CACHE_SIZE);
            final ThrottlingClientConfig config = ThrottlingClientConfig.builder()
                .ruleSetName(RULESET_NAME)
                .cache(cache)
                .region("eu-north-1")
                .clientId(getHostname())
                .cache(cache)
                .build();

            return ThrottlingClient.newNativeClient(config);
        } catch (UnsatisfiedLinkError e) {
            logger.warn("Native throttling library unavailable, using fallback: {}", e.getMessage());
            return null;
        } catch (Exception e) {
            logger.error("Failed to initialize AWS Throttling client {}", e.getMessage());
            return null;
        }
    }

    /**
     * Check if request should be throttled
     */
    public boolean shouldThrottle(RestRequest request) {
        if (!enabled || throttlingClient == null) {
            return false;
        }

        try (ThrottlingHandle handle = throttlingClient.createHandle(getRequestId(request))) {
            final ThrottlingDimensions dimensions = buildDimensions(request);
            if(dimensions==null) {
                logger.warn("Could not build throttling dimensions for host: {}, allowing request ",
                        this.extractHostFromHeaders(request));
                return false;
            }
            float estimatedConsumption = 1.0;
            if(isLargeContentSize(request))
            {
                estimatedConsumption = 3.0;
            }
            final boolean throttled =  handle.shouldThrottle(
                dimensions,
                estimatedConsumption, // estimatedConsumption
                0.0, // unconditionalConsumption
                SyncFailBehavior.LAST_KNOWN_RATE_OR_FAIL_OPEN
            );
            handle.reconcile();
            return throttled;
        } catch (Exception e) {
            logger.warn("Throttling check failed, allowing request", e);
            return false; // Fail open
        }
    }

    private ThrottlingDimensions buildDimensions(RestRequest request) {
        final String hostHeader = extractHostFromHeaders(request);
        final String accountId = extractAccountIdFromHost(hostHeader);
        if(accountId==null) {
            return null;
        }
        return ThrottlingDimensions.builder()
            .addDimension("aws-account", accountId)
            .build();
    }

    public String extractHostFromHeaders(RestRequest request) {
        final String hostHeader = request.header("Host");
        if (hostHeader != null && !hostHeader.isEmpty()) {
            return hostHeader;
        }
        return null;
    }

    private String extractAccountIdFromHost(String hostHeader) {
        if (hostHeader == null || hostHeader.isEmpty()) {
            return null;
        }

        // Extract account ID from host pattern like: 100000000001.aoss-idx-partitions.eu-north-1.test:9200
        final String[] parts = hostHeader.split("\\.");
        if (parts.length > 0 && parts[0].matches("\\d{12}")) {
            return parts[0];
        }
        logger.error("Could not extract account ID from host: {}, using default", hostHeader);
        return null;
    }

    private boolean isLargeContentSize(RestRequest request) {
       return  (request.content().length()/1024)>100;
    }


    private String getRequestId(RestRequest request) {
        return String.valueOf(request.getRequestId());
    }

    private String getHostname() {
        try {
            return java.net.InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "opensearch-node";
        }
    }

    @Override
    protected void doStart() {

    }

    @Override
    protected void doStop() {
        if (throttlingClient != null) {
            throttlingClient.close();
        }
    }

    @Override
    protected void doClose() throws IOException {
        if (throttlingClient != null) {
            throttlingClient.close();
        }
    }
}
