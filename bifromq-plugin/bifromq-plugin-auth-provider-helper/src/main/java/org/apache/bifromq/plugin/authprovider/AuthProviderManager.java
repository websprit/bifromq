/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.bifromq.plugin.authprovider;

import static org.apache.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;
import static org.apache.bifromq.plugin.settingprovider.Setting.ByPassPermCheckError;

import io.micrometer.core.instrument.Timer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.plugin.authprovider.type.CheckResult;
import org.apache.bifromq.plugin.authprovider.type.Error;
import org.apache.bifromq.plugin.authprovider.type.Failed;
import org.apache.bifromq.plugin.authprovider.type.Granted;
import org.apache.bifromq.plugin.authprovider.type.MQTT3AuthData;
import org.apache.bifromq.plugin.authprovider.type.MQTT3AuthResult;
import org.apache.bifromq.plugin.authprovider.type.MQTT5AuthData;
import org.apache.bifromq.plugin.authprovider.type.MQTT5AuthResult;
import org.apache.bifromq.plugin.authprovider.type.MQTT5ExtendedAuthData;
import org.apache.bifromq.plugin.authprovider.type.MQTT5ExtendedAuthResult;
import org.apache.bifromq.plugin.authprovider.type.MQTTAction;
import org.apache.bifromq.plugin.authprovider.type.Reject;
import org.apache.bifromq.plugin.eventcollector.IEventCollector;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.accessctrl.AccessControlError;
import org.apache.bifromq.plugin.settingprovider.ISettingProvider;
import org.apache.bifromq.type.ClientInfo;
import org.pf4j.PluginManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slf4j
public class AuthProviderManager implements IAuthProvider, AutoCloseable {
    private static final Logger pluginLog = LoggerFactory.getLogger("plugin.manager");
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final IAuthProvider delegate;
    private final Map<String, IAuthProvider> listenerDelegates = new TreeMap<>();
    private final Map<String, IAuthProvider> scopedProviders = new TreeMap<>();
    private final Map<String, MetricManager> scopedMetricMgrs = new HashMap<>();
    private final ISettingProvider settingProvider;
    private final IEventCollector eventCollector;
    private MetricManager metricMgr;

    public AuthProviderManager(String authProviderFQN,
                               PluginManager pluginMgr,
                               ISettingProvider settingProvider,
                               IEventCollector eventCollector) {
        this(authProviderFQN, Map.of(), pluginMgr, settingProvider, eventCollector);
    }

    public AuthProviderManager(String authProviderFQN,
                               Map<String, String> listenerAuthProviderFQNs,
                               PluginManager pluginMgr,
                               ISettingProvider settingProvider,
                               IEventCollector eventCollector) {
        this.settingProvider = settingProvider;
        this.eventCollector = eventCollector;
        Map<String, IAuthProvider> availAuthProviders = pluginMgr.getExtensions(IAuthProvider.class)
            .stream().collect(Collectors.toMap(e -> e.getClass().getName(), e -> e,
                (k, v) -> v, TreeMap::new));
        if (availAuthProviders.isEmpty()) {
            pluginLog.warn("No auth provider plugin available, use DEV ONLY one instead");
            delegate = new DevOnlyAuthProvider();
        } else {
            if (authProviderFQN == null) {
                if (availAuthProviders.size() > 1) {
                    pluginLog.info("Auth provider plugin type not specified, use the first found");
                }
                String firstAuthProviderFQN = availAuthProviders.keySet().iterator().next();
                pluginLog.info("Auth provider plugin loaded: {}", firstAuthProviderFQN);
                delegate = availAuthProviders.get(firstAuthProviderFQN);
            } else if (!availAuthProviders.containsKey(authProviderFQN)) {
                pluginLog.warn("Auth provider plugin type '{}' not found, so the system will shut down.",
                    authProviderFQN);
                throw new AuthProviderPluginException("Auth provider plugin type '%s' not found, so the system will "
                    + "shut down.", authProviderFQN);
            } else {
                pluginLog.info("Auth provider plugin type: {}", authProviderFQN);
                delegate = availAuthProviders.get(authProviderFQN);
            }
        }
        listenerAuthProviderFQNs.forEach((listenerKey, listenerAuthProviderFQN) -> {
            if (!availAuthProviders.containsKey(listenerAuthProviderFQN)) {
                pluginLog.warn("Auth provider plugin type '{}' for MQTT listener '{}' not found, so the system will "
                        + "shut down.",
                    listenerAuthProviderFQN, listenerKey);
                throw new AuthProviderPluginException("Auth provider plugin type '%s' for MQTT listener '%s' not "
                    + "found, so the system will shut down.", listenerAuthProviderFQN, listenerKey);
            }
            IAuthProvider listenerDelegate = availAuthProviders.get(listenerAuthProviderFQN);
            pluginLog.info("Auth provider plugin type for MQTT listener '{}': {}",
                listenerKey, listenerAuthProviderFQN);
            listenerDelegates.put(listenerKey, listenerDelegate);
        });
        init();
    }

    private void init() {
        metricMgr = new MetricManager(delegate.getClass().getName());
        listenerDelegates.forEach((listenerId, listenerDelegate) -> {
            MetricManager scopedMetricMgr = new MetricManager(listenerDelegate.getClass().getName());
            scopedMetricMgrs.put(listenerId, scopedMetricMgr);
            scopedProviders.put(listenerId, new ScopedAuthProvider(listenerDelegate, scopedMetricMgr));
        });
    }

    @Override
    public IAuthProvider forListener(String listenerId) {
        return scopedProviders.getOrDefault(listenerId, this);
    }

    @Override
    public IAuthProvider forListener(String listenerId, String transportType) {
        return scopedProviders.getOrDefault(listenerKey(transportType, listenerId), forListener(listenerId));
    }

    @Override
    public CompletableFuture<MQTT3AuthResult> auth(MQTT3AuthData authData) {
        return auth(delegate, metricMgr, authData);
    }

    private CompletableFuture<MQTT3AuthResult> auth(IAuthProvider selectedDelegate,
                                                    MetricManager selectedMetricMgr,
                                                    MQTT3AuthData authData) {
        assert !stopped.get();
        Timer.Sample start = Timer.start();
        try {
            return selectedDelegate.auth(authData)
                .handle((v, e) -> {
                    if (e != null) {
                        selectedMetricMgr.authCallErrorCounter.increment();
                        return MQTT3AuthResult.newBuilder()
                            .setReject(Reject.newBuilder()
                                .setCode(Reject.Code.Error)
                                .setReason(e.getMessage() != null ? e.getMessage() : e.toString())
                                .build())
                            .build();
                    } else {
                        start.stop(selectedMetricMgr.authCallTimer);
                        return v;
                    }
                });
        } catch (Throwable e) {
            selectedMetricMgr.authCallErrorCounter.increment();
            pluginLog.error("AuthProvider auth3 throws exception", e);
            Reject.Builder rb = Reject.newBuilder().setCode(Reject.Code.Error);
            if (e.getMessage() != null) {
                rb.setReason(e.getMessage());
            }
            return CompletableFuture.completedFuture(MQTT3AuthResult.newBuilder()
                .setReject(Reject.newBuilder()
                    .setCode(Reject.Code.Error)
                    .setReason(e.getMessage() != null ? e.getMessage() : e.toString())
                    .build())
                .build());
        }
    }

    @Override
    public CompletableFuture<MQTT5AuthResult> auth(MQTT5AuthData authData) {
        return auth(delegate, metricMgr, authData);
    }

    private CompletableFuture<MQTT5AuthResult> auth(IAuthProvider selectedDelegate,
                                                    MetricManager selectedMetricMgr,
                                                    MQTT5AuthData authData) {
        assert !stopped.get();
        Timer.Sample start = Timer.start();
        try {
            return selectedDelegate.auth(authData)
                .handle((v, e) -> {
                    if (e != null) {
                        selectedMetricMgr.authCallErrorCounter.increment();
                        return MQTT5AuthResult.newBuilder()
                            .setFailed(Failed.newBuilder()
                                .setCode(Failed.Code.Error)
                                .setReason(e.getMessage() != null ? e.getMessage() : e.toString())
                                .build())
                            .build();
                    } else {
                        start.stop(selectedMetricMgr.authCallTimer);
                        return v;
                    }
                });
        } catch (Throwable e) {
            selectedMetricMgr.authCallErrorCounter.increment();
            pluginLog.error("AuthProvider auth5 throws exception", e);
            Failed.Builder rb = Failed.newBuilder().setCode(Failed.Code.Error);
            if (e.getMessage() != null) {
                rb.setReason(e.getMessage());
            }
            return CompletableFuture.completedFuture(MQTT5AuthResult.newBuilder()
                .setFailed(Failed.newBuilder()
                    .setCode(Failed.Code.Error)
                    .setReason(e.getMessage() != null ? e.getMessage() : e.toString())
                    .build())
                .build());
        }
    }

    @Override
    public CompletableFuture<MQTT5ExtendedAuthResult> extendedAuth(MQTT5ExtendedAuthData authData) {
        return extendedAuth(delegate, metricMgr, authData);
    }

    private CompletableFuture<MQTT5ExtendedAuthResult> extendedAuth(IAuthProvider selectedDelegate,
                                                                    MetricManager selectedMetricMgr,
                                                                    MQTT5ExtendedAuthData authData) {
        assert !stopped.get();
        Timer.Sample start = Timer.start();
        try {
            return selectedDelegate.extendedAuth(authData)
                .handle((v, e) -> {
                    if (e != null) {
                        selectedMetricMgr.extAuthCallErrorCounter.increment();
                        return MQTT5ExtendedAuthResult.newBuilder()
                            .setFailed(Failed.newBuilder()
                                .setCode(Failed.Code.Error)
                                .setReason(e.getMessage() != null ? e.getMessage() : e.toString())
                                .build())
                            .build();
                    } else {
                        start.stop(selectedMetricMgr.extAuthCallTimer);
                        return v;
                    }
                });
        } catch (Throwable e) {
            selectedMetricMgr.extAuthCallErrorCounter.increment();
            pluginLog.error("AuthProvider extendedAuth throws exception", e);
            Failed.Builder rb = Failed.newBuilder().setCode(Failed.Code.Error);
            if (e.getMessage() != null) {
                rb.setReason(e.getMessage());
            }
            return CompletableFuture.completedFuture(MQTT5ExtendedAuthResult.newBuilder()
                .setFailed(Failed.newBuilder()
                    .setCode(Failed.Code.Error)
                    .setReason(e.getMessage() != null ? e.getMessage() : e.toString())
                    .build())
                .build());
        }
    }

    @Override
    public CompletableFuture<Boolean> check(ClientInfo client, MQTTAction action) {
        pluginLog.warn(
            "IAuthProvider/check method has been deprecated and will be removed in later release, please implement checkPermission instead");
        return delegate.check(client, action);
    }

    @Override
    public CompletableFuture<CheckResult> checkPermission(ClientInfo client, MQTTAction action) {
        return checkPermission(delegate, metricMgr, client, action);
    }

    private CompletableFuture<CheckResult> checkPermission(IAuthProvider selectedDelegate,
                                                           MetricManager selectedMetricMgr,
                                                           ClientInfo client,
                                                           MQTTAction action) {
        assert !stopped.get();
        Timer.Sample start = Timer.start();
        try {
            return selectedDelegate.checkPermission(client, action)
                .thenApply(v -> {
                    start.stop(selectedMetricMgr.checkCallTimer);
                    if (v.getTypeCase() == CheckResult.TypeCase.ERROR
                        && (boolean) settingProvider.provide(ByPassPermCheckError, client.getTenantId())) {
                        eventCollector.report(
                            getLocal(AccessControlError.class).clientInfo(client).cause(v.getError().getReason()));
                        return CheckResult.newBuilder()
                            .setGranted(Granted.getDefaultInstance())
                            .build();
                    }
                    return v;
                })
                .exceptionally(e -> {
                    selectedMetricMgr.checkCallErrorCounter.increment();
                    eventCollector.report(getLocal(AccessControlError.class).clientInfo(client).cause(e.getMessage()));
                    boolean byPass = settingProvider.provide(ByPassPermCheckError, client.getTenantId());
                    if (byPass) {
                        return CheckResult.newBuilder()
                            .setGranted(Granted.getDefaultInstance())
                            .build();
                    } else {
                        pluginLog.error("AuthProvider permission check error", e);
                        return CheckResult.newBuilder()
                            .setError(Error.newBuilder()
                                .setReason("Permission check error")
                                .build())
                            .build();
                    }
                });
        } catch (Throwable e) {
            selectedMetricMgr.checkCallErrorCounter.increment();
            eventCollector.report(getLocal(AccessControlError.class).clientInfo(client).cause(e.getMessage()));
            boolean byPass = settingProvider.provide(ByPassPermCheckError, client.getTenantId());
            if (byPass) {
                return CompletableFuture.completedFuture(CheckResult.newBuilder()
                    .setGranted(Granted.getDefaultInstance())
                    .build());
            } else {
                pluginLog.error("AuthProvider permission check error", e);
                return CompletableFuture.completedFuture(CheckResult.newBuilder()
                    .setError(Error.newBuilder().setReason("Permission check error").build())
                    .build());
            }
        }
    }

    @Override
    public void close() {
        if (stopped.compareAndSet(false, true)) {
            log.debug("Closing auth provider manager");
            try {
                Set<IAuthProvider> uniqueDelegates = new HashSet<>();
                uniqueDelegates.add(delegate);
                uniqueDelegates.addAll(listenerDelegates.values());
                for (IAuthProvider authProvider : uniqueDelegates) {
                    authProvider.close();
                }
            } catch (Throwable e) {
                pluginLog.error("AuthProvider close throws exception", e);
            }
            metricMgr.close();
            scopedMetricMgrs.values().forEach(MetricManager::close);
            log.debug("Auth provider manager stopped");
        }
    }

    private class ScopedAuthProvider implements IAuthProvider {
        private final IAuthProvider selectedDelegate;
        private final MetricManager selectedMetricMgr;

        ScopedAuthProvider(IAuthProvider selectedDelegate, MetricManager selectedMetricMgr) {
            this.selectedDelegate = selectedDelegate;
            this.selectedMetricMgr = selectedMetricMgr;
        }

        @Override
        public CompletableFuture<MQTT3AuthResult> auth(MQTT3AuthData authData) {
            return AuthProviderManager.this.auth(selectedDelegate, selectedMetricMgr, authData);
        }

        @Override
        public CompletableFuture<MQTT5AuthResult> auth(MQTT5AuthData authData) {
            return AuthProviderManager.this.auth(selectedDelegate, selectedMetricMgr, authData);
        }

        @Override
        public CompletableFuture<MQTT5ExtendedAuthResult> extendedAuth(MQTT5ExtendedAuthData authData) {
            return AuthProviderManager.this.extendedAuth(selectedDelegate, selectedMetricMgr, authData);
        }

        @Override
        public CompletableFuture<Boolean> check(ClientInfo client, MQTTAction action) {
            return selectedDelegate.check(client, action);
        }

        @Override
        public CompletableFuture<CheckResult> checkPermission(ClientInfo client, MQTTAction action) {
            return AuthProviderManager.this.checkPermission(selectedDelegate, selectedMetricMgr, client, action);
        }
    }

    public static String listenerKey(String transportType, String listenerId) {
        return transportType + ":" + listenerId;
    }
}
