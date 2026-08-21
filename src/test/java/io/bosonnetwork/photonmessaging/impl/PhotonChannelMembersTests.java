/*
 * Copyright (c) 2023 -      bosonnetwork.io
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package io.bosonnetwork.photonmessaging.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.junit.jupiter.api.Test;

import io.bosonnetwork.Id;
import io.bosonnetwork.photonmessaging.Channel;

public class PhotonChannelMembersTests {
	private final Id ownerId = Id.random();

	private PhotonChannel newChannel() {
		return new PhotonChannel(Id.random(), new byte[PhotonContact.ENCRYPTED_SESSION_KEY_BYTES], ownerId, Channel.Permission.PUBLIC,
				"Test", null, false, System.currentTimeMillis(), System.currentTimeMillis());
	}

	private List<ChannelMember> members() {
		return List.of(new ChannelMember(ownerId, Channel.Role.OWNER, System.currentTimeMillis()));
	}

	private List<ChannelMember> membersWithJoiner() {
		return List.of(new ChannelMember(ownerId, Channel.Role.OWNER, System.currentTimeMillis()),
				new ChannelMember(Id.random(), Channel.Role.MEMBER, System.currentTimeMillis()));
	}

	// The loader completes before tryLoadMembers() publishes the in-flight future.
	@Test
	void reloadsAfterInvalidateWhenLoaderCompletesInline() {
		AtomicInteger loads = new AtomicInteger();
		PhotonChannel channel = newChannel();
		channel.setMembersLoader(id -> {
			loads.incrementAndGet();
			return Future.succeededFuture(members());
		});

		assertTrue(channel.tryLoadMembers().succeeded());
		assertEquals(1, loads.get());
		assertEquals(1, channel.size());

		channel.invalidateMembers();

		assertTrue(channel.tryLoadMembers().succeeded());
		assertEquals(2, loads.get(), "the invalidated members must be loaded again");
		assertEquals(1, channel.size());
	}

	// The loader completes asynchronously, which is the normal (working) case.
	@Test
	void reloadsAfterInvalidateWhenLoaderCompletesLater() {
		AtomicInteger loads = new AtomicInteger();
		PhotonChannel channel = newChannel();
		Promise<List<ChannelMember>> pending = Promise.promise();
		channel.setMembersLoader(id -> {
			loads.incrementAndGet();
			return pending.future();
		});

		Future<Void> loading = channel.tryLoadMembers();
		pending.complete(members());
		assertTrue(loading.succeeded());
		assertEquals(1, channel.size());

		channel.invalidateMembers();

		channel.setMembersLoader(id -> {
			loads.incrementAndGet();
			return Future.succeededFuture(members());
		});
		assertTrue(channel.tryLoadMembers().succeeded());
		assertEquals(2, loads.get(), "the invalidated members must be loaded again");
		assertEquals(1, channel.size());
	}

	// The members are invalidated while a load is running: the snapshot that load read may predate
	// the change that triggered the invalidation, so it must not be installed.
	@Test
	void reloadsWhenInvalidatedWhileLoading() {
		AtomicInteger loads = new AtomicInteger();
		PhotonChannel channel = newChannel();
		Promise<List<ChannelMember>> stale = Promise.promise();
		channel.setMembersLoader(id -> loads.incrementAndGet() == 1 ?
				stale.future() : Future.succeededFuture(membersWithJoiner()));

		Future<Void> loading = channel.tryLoadMembers();
		channel.invalidateMembers();
		stale.complete(members());

		assertTrue(loading.succeeded());
		assertEquals(2, loads.get(), "a snapshot read before the invalidation must not be installed");
		assertEquals(2, channel.size());
	}
}
