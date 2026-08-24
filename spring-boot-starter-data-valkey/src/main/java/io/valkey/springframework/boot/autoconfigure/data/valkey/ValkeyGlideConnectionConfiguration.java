/*
 * Copyright 2025-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.valkey.springframework.boot.autoconfigure.data.valkey;

import glide.api.models.configuration.ReadFrom;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.thread.Threading;
import org.springframework.boot.autoconfigure.condition.ConditionalOnThreading;
import org.springframework.boot.ssl.SslBundle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.util.StringUtils;
import org.springframework.util.Assert;

import java.io.ByteArrayOutputStream;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.Base64;
import java.util.Enumeration;

import io.valkey.springframework.data.valkey.connection.ValkeyClusterConfiguration;
import io.valkey.springframework.data.valkey.connection.ValkeyStaticMasterReplicaConfiguration;
import io.valkey.springframework.data.valkey.connection.ValkeyConnectionFactory;
import io.valkey.springframework.data.valkey.connection.ValkeySentinelConfiguration;
import io.valkey.springframework.data.valkey.connection.ValkeyStandaloneConfiguration;
import io.valkey.springframework.data.valkey.connection.valkeyglide.ValkeyGlideClientConfiguration;
import io.valkey.springframework.data.valkey.connection.valkeyglide.ValkeyGlideConnectionFactory;

/**
 * Valkey GLIDE connection configuration.
 *
 * @author Jeremy Parr-Pearson
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass({ ValkeyGlideConnectionFactory.class, glide.api.GlideClient.class })
@ConditionalOnProperty(name = "spring.data.valkey.client-type", havingValue = "valkeyglide", matchIfMissing = true)
class ValkeyGlideConnectionConfiguration extends ValkeyConnectionConfiguration {

	ValkeyGlideConnectionConfiguration(ValkeyProperties properties, ValkeyConnectionDetails connectionDetails,
			ObjectProvider<ValkeyStandaloneConfiguration> standaloneConfigurationProvider,
			ObjectProvider<ValkeySentinelConfiguration> sentinelConfigurationProvider,
			ObjectProvider<ValkeyClusterConfiguration> clusterConfigurationProvider,
			ObjectProvider<ValkeyStaticMasterReplicaConfiguration> masterReplicaConfiguration) {
		super(properties, connectionDetails, standaloneConfigurationProvider, sentinelConfigurationProvider,
				clusterConfigurationProvider, masterReplicaConfiguration);
	}

	@Bean
	@ConditionalOnMissingBean(ValkeyConnectionFactory.class)
	@ConditionalOnThreading(Threading.PLATFORM)
	ValkeyGlideConnectionFactory valkeyConnectionFactory(
			ObjectProvider<ValkeyGlideClientConfigurationBuilderCustomizer> builderCustomizers) {
		return createValkeyConnectionFactory(builderCustomizers);
	}

	@Bean
	@ConditionalOnMissingBean(ValkeyConnectionFactory.class)
	@ConditionalOnThreading(Threading.VIRTUAL)
	ValkeyGlideConnectionFactory valkeyConnectionFactoryVirtualThreads(
			ObjectProvider<ValkeyGlideClientConfigurationBuilderCustomizer> builderCustomizers) {
		ValkeyGlideConnectionFactory factory = createValkeyConnectionFactory(builderCustomizers);
		SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor("valkey-");
		executor.setVirtualThreads(true);
		factory.setExecutor(executor);
		return factory;
	}

	private ValkeyGlideConnectionFactory createValkeyConnectionFactory(
			ObjectProvider<ValkeyGlideClientConfigurationBuilderCustomizer> builderCustomizers) {
		ValkeyGlideClientConfiguration clientConfiguration = getValkeyGlideClientConfiguration(builderCustomizers);
		ValkeyGlideConnectionFactory factory = switch (this.mode) {
			case STANDALONE -> new ValkeyGlideConnectionFactory(getStandaloneConfig(), clientConfiguration);
			case CLUSTER -> {
				ValkeyClusterConfiguration clusterConfiguration = getClusterConfiguration();
				Assert.state(clusterConfiguration != null, "'clusterConfiguration' must not be null");
				yield new ValkeyGlideConnectionFactory(clusterConfiguration, clientConfiguration);
			}
			case SENTINEL ->
				throw new IllegalStateException("Valkey GLIDE does not support Sentinel. Use Lettuce or Jedis.");
			case MASTER_REPLICA -> throw new IllegalStateException(
					"Valkey GLIDE does not support Master/Replica topology configuration. Use Lettuce.");
		};

		// Disable early startup for Spring Boot to avoid connection attempts during bean
		// creation
		factory.setEarlyStartup(false);
		return factory;
	}

	private ValkeyGlideClientConfiguration getValkeyGlideClientConfiguration(
			ObjectProvider<ValkeyGlideClientConfigurationBuilderCustomizer> builderCustomizers) {
		ValkeyGlideClientConfiguration.ValkeyGlideClientConfigurationBuilder builder = ValkeyGlideClientConfiguration
			.builder();

		if (getProperties().getTimeout() != null) {
			builder.commandTimeout(getProperties().getTimeout());
		}
		if (getProperties().getSsl().isEnabled() || getSslBundle() != null) {
			builder.useSsl();
			applySslBundle(builder, getSslBundle());
		}
		if (StringUtils.hasText(getProperties().getUrl())) {
			customizeConfigurationFromUrl(builder);
		}

		ValkeyProperties.ValkeyGlide valkeyGlideProperties = getProperties().getValkeyGlide();
		if (valkeyGlideProperties.getConnectionTimeout() != null) {
			builder.connectionTimeout(valkeyGlideProperties.getConnectionTimeout());
		}
		String readFrom = valkeyGlideProperties.getReadFrom();
		if (StringUtils.hasText(readFrom)) {
			builder.readFrom(getReadFrom(readFrom));
		}
		if (valkeyGlideProperties.getInflightRequestsLimit() != null) {
			builder.inflightRequestsLimit(valkeyGlideProperties.getInflightRequestsLimit());
		}
		if (valkeyGlideProperties.getClientAZ() != null) {
			builder.clientAZ(valkeyGlideProperties.getClientAZ());
		}
		if (valkeyGlideProperties.getMaxPoolSize() != null) {
			builder.maxPoolSize(valkeyGlideProperties.getMaxPoolSize());
		}

		// Apply OpenTelemetry configuration if enabled
		ValkeyProperties.ValkeyGlide.OpenTelemetry otelProperties = valkeyGlideProperties.getOpenTelemetry();
		if (otelProperties != null && otelProperties.isEnabled()) {
			ValkeyGlideClientConfiguration.OpenTelemetryForGlide otelConfig = new ValkeyGlideClientConfiguration.OpenTelemetryForGlide(
					otelProperties.getTracesEndpoint(), otelProperties.getMetricsEndpoint(),
					otelProperties.getSamplePercentage(), otelProperties.getFlushIntervalMs());
			builder.useOpenTelemetry(otelConfig);
		}

		// Apply IAM authentication configuration if configured
		ValkeyProperties.ValkeyGlide.IamAuthentication iamProperties = valkeyGlideProperties.getIamAuthentication();
		if (iamProperties != null) {
			if (!isSslEnabled()) {
				throw new IllegalArgumentException("IAM authentication requires TLS/SSL to be enabled. "
						+ "Please set spring.data.valkey.ssl.enabled=true or use a valkeys:// URL.");
			}
			if (!StringUtils.hasText(iamProperties.getClusterName()) || !StringUtils.hasText(iamProperties.getService())
					|| !StringUtils.hasText(iamProperties.getRegion())) {
				throw new IllegalArgumentException(
						"IAM authentication requires all of: cluster-name, service, and region. "
								+ "Please set spring.data.valkey.valkey-glide.iam-authentication.cluster-name, "
								+ "spring.data.valkey.valkey-glide.iam-authentication.service, and "
								+ "spring.data.valkey.valkey-glide.iam-authentication.region");
			}
			ValkeyGlideClientConfiguration.AwsServiceType serviceType = ValkeyGlideClientConfiguration.AwsServiceType
				.valueOf(iamProperties.getService().toUpperCase(java.util.Locale.ROOT));
			ValkeyGlideClientConfiguration.IamAuthenticationForGlide iamConfig = new ValkeyGlideClientConfiguration.IamAuthenticationForGlide(
					iamProperties.getClusterName(), serviceType, iamProperties.getRegion(),
					iamProperties.getRefreshIntervalSeconds());
			builder.useIamAuthentication(iamConfig);
		}

		builderCustomizers.orderedStream().forEach((customizer) -> customizer.customize(builder));
		return builder.build();
	}

	private void customizeConfigurationFromUrl(
			ValkeyGlideClientConfiguration.ValkeyGlideClientConfigurationBuilder builder) {
		if (urlUsesSsl(getProperties().getUrl())) {
			builder.useSsl();
		}
	}

	private ReadFrom getReadFrom(String readFrom) {
		try {
			return ReadFrom.valueOf(readFrom.toUpperCase());
		}
		catch (IllegalArgumentException ex) {
			throw new IllegalArgumentException("Invalid readFrom value: " + readFrom, ex);
		}
	}

	/**
	 * Apply an {@link SslBundle} to the GLIDE client configuration.
	 * <p>
	 * The GLIDE client only supports supplying custom root (CA) certificates for server verification. Client key
	 * material (mutual TLS), custom cipher suites, and explicit protocol selection are not supported by the GLIDE
	 * driver. Rather than silently ignoring configured material, this method fails fast with a clear error so that a
	 * misconfiguration cannot lead to a connection that trusts material the operator did not intend.
	 * @param builder the GLIDE client configuration builder.
	 * @param sslBundle the SSL bundle, may be {@literal null}.
	 */
	private void applySslBundle(ValkeyGlideClientConfiguration.ValkeyGlideClientConfigurationBuilder builder,
			@org.jspecify.annotations.Nullable SslBundle sslBundle) {
		if (sslBundle == null) {
			return;
		}

		// Fail fast: the GLIDE version currently in use cannot present a client certificate (no mutual TLS
		// support). GLIDE added mTLS in a later release; when the bundled GLIDE version is upgraded, this
		// guard can be replaced by wiring the client key material through TlsAdvancedConfiguration#useMutualTls.
		if (hasClientKeyMaterial(sslBundle)) {
			throw new IllegalStateException(
					"The Valkey GLIDE driver does not support mutual TLS (client certificates). "
							+ "The configured SSL bundle contains client key material that would be silently ignored. "
							+ "Remove the key store from the SSL bundle, or use the Lettuce driver for mutual TLS.");
		}

		// Fail fast: GLIDE does not support custom cipher suites or explicit protocol selection.
		if (sslBundle.getOptions() != null) {
			if (sslBundle.getOptions().getCiphers() != null) {
				throw new IllegalStateException(
						"The Valkey GLIDE driver does not support configuring TLS cipher suites via an SSL bundle. "
								+ "Remove 'ciphers' from the SSL bundle options, or use the Lettuce driver.");
			}
			if (sslBundle.getOptions().getEnabledProtocols() != null) {
				throw new IllegalStateException(
						"The Valkey GLIDE driver does not support configuring TLS protocols via an SSL bundle. "
								+ "Remove 'enabled-protocols' from the SSL bundle options, or use the Lettuce driver.");
			}
		}

		// Supported: custom root (CA) certificates for server verification.
		byte[] trustCertificates = extractTrustCertificates(sslBundle);
		if (trustCertificates != null) {
			builder.tlsTrustCertificates(trustCertificates);
		}
	}

	/**
	 * Determine whether the bundle's key store contains a private key entry (i.e. client key material for mutual TLS).
	 * A key store that holds only certificate entries is not considered client key material.
	 * @param sslBundle the SSL bundle.
	 * @return {@literal true} if a private key entry is present.
	 */
	private boolean hasClientKeyMaterial(SslBundle sslBundle) {
		try {
			KeyStore keyStore = sslBundle.getStores().getKeyStore();
			if (keyStore == null) {
				return false;
			}
			for (Enumeration<String> aliases = keyStore.aliases(); aliases.hasMoreElements();) {
				if (keyStore.isKeyEntry(aliases.nextElement())) {
					return true;
				}
			}
			return false;
		}
		catch (Exception ex) {
			throw new IllegalStateException("Failed to inspect the configured SSL bundle key store", ex);
		}
	}

	/**
	 * Extract the trusted (CA) certificates from the bundle's trust store and encode them as a PEM byte array suitable
	 * for GLIDE's {@code rootCertificates} option.
	 * @param sslBundle the SSL bundle.
	 * @return the PEM-encoded certificates, or {@literal null} if the bundle has no trust store.
	 */
	private byte @org.jspecify.annotations.Nullable [] extractTrustCertificates(SslBundle sslBundle) {
		try {
			KeyStore trustStore = sslBundle.getStores().getTrustStore();
			if (trustStore == null) {
				return null;
			}
			ByteArrayOutputStream pem = new ByteArrayOutputStream();
			Base64.Encoder encoder = Base64.getMimeEncoder(64, "\n".getBytes());
			boolean found = false;
			for (Enumeration<String> aliases = trustStore.aliases(); aliases.hasMoreElements();) {
				String alias = aliases.nextElement();
				Certificate certificate = trustStore.getCertificate(alias);
				if (certificate == null) {
					continue;
				}
				found = true;
				pem.write("-----BEGIN CERTIFICATE-----\n".getBytes());
				pem.write(encoder.encode(certificate.getEncoded()));
				pem.write("\n-----END CERTIFICATE-----\n".getBytes());
			}
			return found ? pem.toByteArray() : null;
		}
		catch (Exception ex) {
			throw new IllegalStateException("Failed to extract trust certificates from the configured SSL bundle", ex);
		}
	}

}
