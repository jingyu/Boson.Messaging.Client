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
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.bosonnetwork.Id;
import io.bosonnetwork.photonmessaging.Channel;

public class ChannelMembersNotificationTests {
	private static ChannelMember member(Id id, Channel.Role role) {
		return new ChannelMember(id, role, System.currentTimeMillis());
	}

	@Test
	void keepsTheKnownMembersWhenAllAreKnown() {
		Id alice = Id.random();
		Id bob = Id.random();
		List<Channel.Member> known = List.of(member(alice, Channel.Role.MODERATOR), member(bob, Channel.Role.MEMBER));

		List<Channel.Member> resolved = PhotonMessagingClient.resolveNotifiedMembers(known, List.of(alice, bob));

		assertSame(known, resolved);
	}

	// A membership notification can arrive before the member join it refers to has been applied.
	@Test
	void synthesizesTheMembersThatAreNotKnownLocally() {
		Id known = Id.random();
		Id unknown = Id.random();
		List<Channel.Member> resolved = PhotonMessagingClient.resolveNotifiedMembers(
				List.of(member(known, Channel.Role.MODERATOR)), List.of(known, unknown));

		assertEquals(2, resolved.size());
		assertEquals(known, resolved.get(0).getId());
		assertEquals(Channel.Role.MODERATOR, resolved.get(0).getRole());
		assertEquals(unknown, resolved.get(1).getId());
		assertEquals(Channel.Role.MEMBER, resolved.get(1).getRole());
	}

	@Test
	void synthesizesEveryMemberWhenNoneIsKnownLocally() {
		Id removed = Id.random();

		List<Channel.Member> resolved = PhotonMessagingClient.resolveNotifiedMembers(List.of(), List.of(removed));

		assertEquals(1, resolved.size());
		assertEquals(removed, resolved.get(0).getId());
	}
}
