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

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.bosonnetwork.Id;
import io.bosonnetwork.photonmessaging.ConnectionListener;
import io.bosonnetwork.photonmessaging.FriendRequestListener;

public class PhotonMessagingListenersTests {
	private static class RecordingConnectionListener implements ConnectionListener {
		final List<String> events = new ArrayList<>();

		@Override
		public void onConnecting() {
			events.add("connecting");
		}

		@Override
		public void onConnected() {
			events.add("connected");
		}

		@Override
		public void onContactSynced() {
			events.add("contactSynced");
		}

		@Override
		public void onDisconnected() {
			events.add("disconnected");
		}
	}

	// Regression guard for the onDisconnected dispatch bug: the facade overrides every
	// ConnectionListener method (including the default onDisconnected) so disconnect events
	// reach the registered listener instead of hitting the inherited no-op default.
	@Test
	void dispatchesAllConnectionEventsIncludingDisconnected() {
		PhotonMessagingListeners listeners = new PhotonMessagingListeners();
		RecordingConnectionListener listener = new RecordingConnectionListener();
		listeners.addConnectionListener(listener);

		listeners.onConnecting();
		listeners.onConnected();
		listeners.onContactSynced();
		listeners.onDisconnected();

		assertEquals(List.of("connecting", "connected", "contactSynced", "disconnected"), listener.events);
	}

	@Test
	void removedListenerReceivesNoFurtherEvents() {
		PhotonMessagingListeners listeners = new PhotonMessagingListeners();
		RecordingConnectionListener listener = new RecordingConnectionListener();
		listeners.addConnectionListener(listener);
		listeners.removeConnectionListener(listener);

		listeners.onDisconnected();

		assertEquals(List.of(), listener.events);
	}

	@Test
	void dispatchToMultipleListenersIsExceptionIsolated() {
		PhotonMessagingListeners listeners = new PhotonMessagingListeners();
		ConnectionListener throwing = new ConnectionListener() {
			@Override
			public void onContactSynced() {
			}

			@Override
			public void onDisconnected() {
				throw new RuntimeException("boom");
			}
		};
		RecordingConnectionListener good = new RecordingConnectionListener();
		listeners.addConnectionListener(throwing);
		listeners.addConnectionListener(good);

		// The throwing listener must not prevent the good listener from being notified.
		listeners.onDisconnected();

		assertEquals(List.of("disconnected"), good.events);
	}

	private static class RecordingFriendRequestListener implements FriendRequestListener {
		final List<String> events = new ArrayList<>();

		@Override
		public void onFriendRequest(Id userId, String hello) {
			events.add("request:" + userId + ":" + hello);
		}

		@Override
		public void onFriendRequestAccepted(Id userId) {
			events.add("accepted:" + userId);
		}
	}

	@Test
	void dispatchesFriendRequestEventsToEveryListener() {
		PhotonMessagingListeners listeners = new PhotonMessagingListeners();
		RecordingFriendRequestListener first = new RecordingFriendRequestListener();
		RecordingFriendRequestListener second = new RecordingFriendRequestListener();
		listeners.addFriendRequestListener(first);
		listeners.addFriendRequestListener(second);

		Id alice = Id.random();
		Id bob = Id.random();
		listeners.onFriendRequest(alice, "Hi, I'm Alice");
		listeners.onFriendRequestAccepted(bob);

		List<String> expected = List.of("request:" + alice + ":Hi, I'm Alice", "accepted:" + bob);
		assertEquals(expected, first.events);
		assertEquals(expected, second.events);
	}

	@Test
	void friendRequestEventsWithoutListenersAreDropped() {
		PhotonMessagingListeners listeners = new PhotonMessagingListeners();

		// Nothing registered, or everything removed again: dispatch is a no-op, not an error.
		listeners.onFriendRequest(Id.random(), "Hello");
		listeners.onFriendRequestAccepted(Id.random());

		RecordingFriendRequestListener listener = new RecordingFriendRequestListener();
		listeners.addFriendRequestListener(listener);
		listeners.removeFriendRequestListener(listener);
		listeners.onFriendRequest(Id.random(), "Hello");
		listeners.onFriendRequestAccepted(Id.random());

		assertEquals(List.of(), listener.events);
	}

	@Test
	void removedFriendRequestListenerReceivesNoFurtherEvents() {
		PhotonMessagingListeners listeners = new PhotonMessagingListeners();
		RecordingFriendRequestListener removed = new RecordingFriendRequestListener();
		RecordingFriendRequestListener kept = new RecordingFriendRequestListener();
		listeners.addFriendRequestListener(removed);
		listeners.addFriendRequestListener(kept);

		Id alice = Id.random();
		listeners.onFriendRequest(alice, "first");
		listeners.removeFriendRequestListener(removed);
		listeners.onFriendRequest(alice, "second");
		listeners.onFriendRequestAccepted(alice);

		assertEquals(List.of("request:" + alice + ":first"), removed.events);
		assertEquals(List.of("request:" + alice + ":first", "request:" + alice + ":second", "accepted:" + alice),
				kept.events);

		// Removing a listener that was never added leaves the others registered.
		listeners.removeFriendRequestListener(new RecordingFriendRequestListener());
		listeners.onFriendRequestAccepted(alice);
		assertEquals(4, kept.events.size());
	}

	@Test
	void removeAllListenersDropsFriendRequestListeners() {
		PhotonMessagingListeners listeners = new PhotonMessagingListeners();
		RecordingFriendRequestListener listener = new RecordingFriendRequestListener();
		listeners.addFriendRequestListener(listener);

		listeners.removeAllListeners();
		listeners.onFriendRequest(Id.random(), "Hello");

		assertEquals(List.of(), listener.events);
	}

	@Test
	void friendRequestDispatchIsExceptionIsolated() {
		PhotonMessagingListeners listeners = new PhotonMessagingListeners();
		FriendRequestListener throwing = new FriendRequestListener() {
			@Override
			public void onFriendRequest(Id userId, String hello) {
				throw new RuntimeException("boom");
			}

			@Override
			public void onFriendRequestAccepted(Id userId) {
				throw new RuntimeException("boom");
			}
		};
		RecordingFriendRequestListener good = new RecordingFriendRequestListener();
		listeners.addFriendRequestListener(throwing);
		listeners.addFriendRequestListener(good);

		Id alice = Id.random();
		listeners.onFriendRequest(alice, "Hello");
		listeners.onFriendRequestAccepted(alice);

		assertEquals(List.of("request:" + alice + ":Hello", "accepted:" + alice), good.events);
	}
}
