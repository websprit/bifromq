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

package org.apache.bifromq.starter.module;

import static org.apache.bifromq.starter.utils.ResourceUtil.loadFile;

import com.google.common.base.Strings;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.incubator.codec.quic.QuicSslContext;
import io.netty.incubator.codec.quic.QuicSslContextBuilder;
import java.io.File;
import java.security.Provider;
import java.security.Security;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Collection;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.starter.config.model.ServerSSLContextConfig;

@Slf4j
public class SSLUtil {
    public static SslProvider defaultSslProvider() {
        if (OpenSsl.isAvailable()) {
            return SslProvider.OPENSSL;
        }
        Provider jdkProvider = findJdkProvider();
        if (jdkProvider != null) {
            return SslProvider.JDK;
        }
        throw new IllegalStateException("Could not find TLS provider");
    }

    public static Provider findJdkProvider() {
        Provider[] providers = Security.getProviders("SSLContext.TLS");
        if (providers.length > 0) {
            return providers[0];
        }
        return null;
    }

    public static SslContext buildServerSslContext(ServerSSLContextConfig config) {
        try {
            SslProvider sslProvider = defaultSslProvider();
            SslContextBuilder sslCtxBuilder = SslContextBuilder
                    .forServer(loadFile(config.getCertFile()), loadFile(config.getKeyFile()))
                    .clientAuth(config.getClientAuth())
                    .sslProvider(sslProvider);
            if (Strings.isNullOrEmpty(config.getTrustCertsFile())) {
                sslCtxBuilder.trustManager(InsecureTrustManagerFactory.INSTANCE);
            } else {
                sslCtxBuilder.trustManager(loadFile(config.getTrustCertsFile()));
            }
            if (sslProvider == SslProvider.JDK) {
                sslCtxBuilder.sslContextProvider(findJdkProvider());
            }
            return sslCtxBuilder.build();
        } catch (Throwable e) {
            throw new RuntimeException("Fail to initialize server SSLContext", e);
        }
    }

    /**
     * Build a QuicSslContext for QUIC transport.
     * QUIC mandates TLS 1.3 and uses ALPN protocol "mqtt".
     */
    public static QuicSslContext buildQuicSslContext(ServerSSLContextConfig config) {
        try {
            File certFile = loadFile(config.getCertFile());
            File keyFile = loadFile(config.getKeyFile());
            log.info("Building QUIC SSL context: certFile={}, keyFile={}, trustCertsFile={}, clientAuth={}",
                certFile.getAbsolutePath(), keyFile.getAbsolutePath(), config.getTrustCertsFile(), config.getClientAuth());
            logQuicCertificateDetails(certFile);
            QuicSslContextBuilder quicSslCtxBuilder = QuicSslContextBuilder
                    // forServer(keyFile, keyPassword, certChainFile)
                    .forServer(keyFile, null, certFile)
                    .applicationProtocols("mqtt")
                    .clientAuth(config.getClientAuth());
            if (!Strings.isNullOrEmpty(config.getTrustCertsFile())) {
                quicSslCtxBuilder.trustManager(loadFile(config.getTrustCertsFile()));
            }
            QuicSslContext quicSslContext = quicSslCtxBuilder.build();
            log.info("Built QUIC SSL context successfully: applicationProtocols=[mqtt], clientAuth={}",
                config.getClientAuth());
            return quicSslContext;
        } catch (Throwable e) {
            throw new RuntimeException("Fail to initialize QUIC SSLContext", e);
        }
    }

    private static void logQuicCertificateDetails(File certFile) {
        try {
            CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
            Collection<? extends java.security.cert.Certificate> certificates =
                certificateFactory.generateCertificates(new java.io.FileInputStream(certFile));
            int index = 0;
            for (java.security.cert.Certificate certificate : certificates) {
                if (certificate instanceof X509Certificate x509Certificate) {
                    log.info("QUIC certificate[{}]: subject={}, issuer={}, notBefore={}, notAfter={}, san={}",
                        index++, x509Certificate.getSubjectX500Principal(), x509Certificate.getIssuerX500Principal(),
                        x509Certificate.getNotBefore(), x509Certificate.getNotAfter(),
                        x509Certificate.getSubjectAlternativeNames());
                }
            }
        } catch (Throwable e) {
            log.warn("Failed to inspect QUIC certificate metadata: certFile={}", certFile.getAbsolutePath(), e);
        }
    }
}
