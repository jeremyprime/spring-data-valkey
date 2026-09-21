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
package io.valkey.springframework.data.valkey.connection.valkeyglide;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import glide.api.models.GlideString;

import org.junit.jupiter.api.Test;

import io.valkey.springframework.data.valkey.core.Cursor;
import io.valkey.springframework.data.valkey.core.ScanOptions;

/**
 * Unit tests for {@link ValkeyGlideKeyCommands}.
 *
 * @author Mantas Aleknavičius
 */
class ValkeyGlideKeyCommandsUnitTests {

	@Test // GH-108
	void scanContinuesAfterEmptyIntermediateBatch() throws Exception {

		UnifiedGlideClient client = mock(UnifiedGlideClient.class);
		when(client.customCommand(any(GlideString[].class))).thenReturn(
				new Object[] { GlideString.of("17"), new Object[] { GlideString.of("key:1") } },
				new Object[] { GlideString.of("29"), new Object[] {} },
				new Object[] { GlideString.of("0"), new Object[] { GlideString.of("key:2") } });
		ValkeyGlideConnection connection = new ValkeyGlideConnection(client, null);
		ValkeyGlideKeyCommands commands = new ValkeyGlideKeyCommands(connection);

		try (Cursor<byte[]> cursor = commands.scan(ScanOptions.scanOptions().match("key:*").count(1).build())) {
			assertThat(cursor.hasNext()).isTrue();
			assertThat(cursor.next()).isEqualTo("key:1".getBytes());
			assertThat(cursor.hasNext()).isTrue();
			assertThat(cursor.next()).isEqualTo("key:2".getBytes());
			assertThat(cursor.hasNext()).isFalse();
		}

		verify(client, times(3)).customCommand(any(GlideString[].class));
	}

}
