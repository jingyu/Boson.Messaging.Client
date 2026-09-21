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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import io.bosonnetwork.Id;

public class PhotonFriendRequestTests {
	private static final long WEEK = TimeUnit.DAYS.toMillis(7);

	@Test
	void newRequestIsPending() {
		Id userId = Id.random();
		Id initiatorId = Id.random();

		long before = System.currentTimeMillis();
		PhotonFriendRequest fr = new PhotonFriendRequest(userId, initiatorId, "Hello");
		long after = System.currentTimeMillis();

		assertEquals(userId, fr.getUserId());
		assertEquals(initiatorId, fr.getInitiatorId());
		assertEquals("Hello", fr.getHello());
		assertFalse(fr.isAccepted());
		assertFalse(fr.isExpired());
		assertEquals(0, fr.getAcceptedAt());
		assertThat(fr.getCreatedAt()).isBetween(before, after);
		// Never updated yet: the update time starts at the creation time.
		assertEquals(fr.getCreatedAt(), fr.getUpdatedAt());
	}

	@Test
	void helloIsOptional() {
		PhotonFriendRequest fr = new PhotonFriendRequest(Id.random(), Id.random(), null);
		assertNull(fr.getHello());
	}

	@Test
	void zeroUpdateTimeFallsBackToCreationTime() {
		PhotonFriendRequest fr = new PhotonFriendRequest(Id.random(), Id.random(), "Hello", 1000, 0);
		assertEquals(1000, fr.getCreatedAt());
		assertEquals(1000, fr.getUpdatedAt());

		PhotonFriendRequest updated = new PhotonFriendRequest(Id.random(), Id.random(), "Hello", 1000, 2000);
		assertEquals(1000, updated.getCreatedAt());
		assertEquals(2000, updated.getUpdatedAt());
	}

	@Test
	void restoredStateIsKept() {
		PhotonFriendRequest fr = new PhotonFriendRequest(Id.random(), Id.random(), "Hello",
				1000, 3000, true, 3000);
		assertTrue(fr.isAccepted());
		assertEquals(1000, fr.getCreatedAt());
		assertEquals(3000, fr.getUpdatedAt());
		assertEquals(3000, fr.getAcceptedAt());
	}

	@Test
	void acceptMarksNow() {
		long created = System.currentTimeMillis() - 60_000;
		PhotonFriendRequest fr = new PhotonFriendRequest(Id.random(), Id.random(), "Hello", created, created);

		long before = System.currentTimeMillis();
		fr.accept();
		long after = System.currentTimeMillis();

		assertTrue(fr.isAccepted());
		assertThat(fr.getAcceptedAt()).isBetween(before, after);
		assertEquals(fr.getAcceptedAt(), fr.getUpdatedAt());
		assertEquals(created, fr.getCreatedAt());
	}

	@Test
	void acceptAtTakesTheGivenTime() {
		PhotonFriendRequest fr = new PhotonFriendRequest(Id.random(), Id.random(), "Hello", 1000, 1000);
		fr.accept(5000);

		assertTrue(fr.isAccepted());
		assertEquals(5000, fr.getAcceptedAt());
		assertEquals(5000, fr.getUpdatedAt());
		assertEquals(1000, fr.getCreatedAt());
	}

	@Test
	void pendingRequestExpiresAfterAWeek() {
		long now = System.currentTimeMillis();

		PhotonFriendRequest fresh = new PhotonFriendRequest(Id.random(), Id.random(), "Hello",
				now - WEEK + 60_000, now - WEEK + 60_000);
		assertFalse(fresh.isExpired());

		PhotonFriendRequest stale = new PhotonFriendRequest(Id.random(), Id.random(), "Hello",
				now - WEEK - 60_000, now - WEEK - 60_000);
		assertTrue(stale.isExpired());
	}

	@Test
	void expiryCountsFromTheSendTimeNotTheArrival() {
		long now = System.currentTimeMillis();

		// Sent eight days ago, received a minute ago (the recipient was offline): expired, as it is
		// on the sender's side, so both sides agree.
		PhotonFriendRequest late = new PhotonFriendRequest(Id.random(), Id.random(), "Hello",
				now - WEEK - TimeUnit.DAYS.toMillis(1), now - 60_000);
		assertTrue(late.isExpired());

		// Sent a day ago, whenever it arrived: still pending.
		PhotonFriendRequest recent = new PhotonFriendRequest(Id.random(), Id.random(), "Hello",
				now - TimeUnit.DAYS.toMillis(1), now);
		assertFalse(recent.isExpired());
	}

	@Test
	void acceptedRequestNeverExpires() {
		long longAgo = System.currentTimeMillis() - 4 * WEEK;
		PhotonFriendRequest fr = new PhotonFriendRequest(Id.random(), Id.random(), "Hello",
				longAgo, longAgo, true, longAgo);
		assertFalse(fr.isExpired());

		PhotonFriendRequest acceptedLate = new PhotonFriendRequest(Id.random(), Id.random(), "Hello", longAgo, longAgo);
		assertTrue(acceptedLate.isExpired());
		acceptedLate.accept(longAgo + 1);
		assertFalse(acceptedLate.isExpired());
	}

	@Test
	void directionFollowsTheInitiator() {
		Id other = Id.random();
		Id self = Id.random();

		// The record's user is always the other side; the initiator says who sent it.
		PhotonFriendRequest outgoing = new PhotonFriendRequest(other, self, "Hello");
		assertTrue(outgoing.isOutgoing());

		PhotonFriendRequest incoming = new PhotonFriendRequest(other, other, "Hello");
		assertFalse(incoming.isOutgoing());

		// Accepting does not change the direction.
		outgoing.accept();
		incoming.accept();
		assertTrue(outgoing.isOutgoing());
		assertFalse(incoming.isOutgoing());
	}

	@Test
	void expiryAtAGivenTime() {
		PhotonFriendRequest fr = new PhotonFriendRequest(Id.random(), Id.random(), "Hello", 1000, 1000);
		assertFalse(fr.isExpiredAt(1000));
		assertFalse(fr.isExpiredAt(1000 + WEEK - 1));
		assertTrue(fr.isExpiredAt(1000 + WEEK));

		fr.accept(2000);
		assertFalse(fr.isExpiredAt(1000 + 2 * WEEK));
	}

	@Test
	void receivedRequestKeepsTheSendersTimeWhenNotInTheFuture() {
		Id sender = Id.random();
		long now = 1_000_000;

		// Created when sent, updated when received.
		PhotonFriendRequest past = PhotonFriendRequest.received(sender, sender, "Hello", now - 5000, now);
		assertEquals(now - 5000, past.getCreatedAt());
		assertEquals(now, past.getUpdatedAt());
		assertFalse(past.isOutgoing());
		assertFalse(past.isAccepted());

		PhotonFriendRequest exact = PhotonFriendRequest.received(sender, sender, "Hello", now, now);
		assertEquals(now, exact.getCreatedAt());
		assertEquals(now, exact.getUpdatedAt());
	}

	@Test
	void receivedRequestDatedInTheFutureStartsNow() {
		Id sender = Id.random();
		long now = 1_000_000;

		// A sender clock a year ahead must not push the expiry out by a year.
		PhotonFriendRequest future = PhotonFriendRequest.received(sender, sender, "Hello",
				now + TimeUnit.DAYS.toMillis(365), now);
		assertEquals(now, future.getCreatedAt());
		assertEquals(now, future.getUpdatedAt());
		assertFalse(future.isExpiredAt(now + WEEK - 1));
		assertTrue(future.isExpiredAt(now + WEEK));
	}

	@Test
	void notInTheFutureCapsAtNow() {
		assertEquals(10, PhotonFriendRequest.notInTheFuture(10, 20));
		assertEquals(20, PhotonFriendRequest.notInTheFuture(20, 20));
		assertEquals(20, PhotonFriendRequest.notInTheFuture(30, 20));
	}
}
