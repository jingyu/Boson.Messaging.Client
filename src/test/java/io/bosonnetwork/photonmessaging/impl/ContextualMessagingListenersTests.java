/*
 * Copyright (c) 2026 -      bosonnetwork.io
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
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import io.vertx.core.Context;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import io.vertx.junit5.VertxExtension;

import io.bosonnetwork.Id;
import io.bosonnetwork.photonmessaging.Channel;
import io.bosonnetwork.photonmessaging.DeliveryStatus;
import io.bosonnetwork.photonmessaging.MessageDeliveryReceipt;
import io.bosonnetwork.photonmessaging.SessionInfo;

@ExtendWith(VertxExtension.class)
class ContextualMessagingListenersTests {
	// The facade supplies unused callbacks; each representative event records one dispatch.
	private static class RecordingListener extends PhotonMessagingListeners {
		private final Runnable callback;

		RecordingListener(Runnable callback) {
			this.callback = callback;
		}

		@Override
		public void onDisconnected() {
			callback.run();
		}

		@Override
		public void onDeliveryReceipt(MessageDeliveryReceipt receipt) {
			callback.run();
		}

		@Override
		public void onContactsCleared() {
			callback.run();
		}

		@Override
		public void onChannelUpdated(Channel channel) {
			callback.run();
		}

		@Override
		public void onNewSession(SessionInfo session) {
			callback.run();
		}

		@Override
		public void onFriendRequestAccepted(Id userId) {
			callback.run();
		}
	}

	private enum ListenerType {
		CONNECTION(PhotonMessagingListeners::addConnectionListener, PhotonMessagingListeners::removeConnectionListener,
				PhotonMessagingListeners::onDisconnected),
		MESSAGE(PhotonMessagingListeners::addMessageListener, PhotonMessagingListeners::removeMessageListener,
				l -> l.onDeliveryReceipt(new MessageDeliveryReceipt(Id.random(), DeliveryStatus.DELIVERED, null, 1L))),
		CONTACT(PhotonMessagingListeners::addContactListener, PhotonMessagingListeners::removeContactListener,
				PhotonMessagingListeners::onContactsCleared),
		CHANNEL(PhotonMessagingListeners::addChannelListener, PhotonMessagingListeners::removeChannelListener,
				l -> l.onChannelUpdated(new PhotonChannel(Id.random(), new byte[PhotonContact.ENCRYPTED_SESSION_KEY_BYTES],
						Id.random(), Channel.Permission.PUBLIC, "Test", null, false, 1L, 1L))),
		SESSION(PhotonMessagingListeners::addSessionListener, PhotonMessagingListeners::removeSessionListener,
				l -> l.onNewSession(new SessionInfo(Id.random(), true, 1L, "127.0.0.1"))),
		FRIEND_REQUEST(PhotonMessagingListeners::addFriendRequestListener, PhotonMessagingListeners::removeFriendRequestListener,
				l -> l.onFriendRequestAccepted(Id.random()));

		final BiConsumer<PhotonMessagingListeners, RecordingListener> add;
		final BiConsumer<PhotonMessagingListeners, RecordingListener> remove;
		final Consumer<PhotonMessagingListeners> emit;

		ListenerType(BiConsumer<PhotonMessagingListeners, RecordingListener> add,
				BiConsumer<PhotonMessagingListeners, RecordingListener> remove, Consumer<PhotonMessagingListeners> emit) {
			this.add = add;
			this.remove = remove;
			this.emit = emit;
		}
	}

	private void onContext(Context context, Runnable action) throws Exception {
		Promise<Void> done = Promise.promise();
		context.runOnContext(v -> {
			try {
				action.run();
				done.complete();
			} catch (Throwable t) {
				done.fail(t);
			}
		});
		done.future().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
	}

	@ParameterizedTest
	@EnumSource(ListenerType.class)
	void eachListenerUsesItsRegistrationContext(ListenerType type, Vertx vertx) throws Exception {
		Context first = vertx.getOrCreateContext();
		Context second = vertx.getOrCreateContext();
		Context emitter = vertx.getOrCreateContext();
		assertNotSame(first, emitter);
		assertNotSame(first, second);
		PhotonMessagingListeners listeners = new PhotonMessagingListeners();
		AtomicReference<Context> firstCallback = new AtomicReference<>();
		AtomicReference<Context> secondCallback = new AtomicReference<>();
		onContext(first, () -> type.add.accept(listeners, new RecordingListener(() -> firstCallback.set(Vertx.currentContext()))));
		onContext(second, () -> type.add.accept(listeners, new RecordingListener(() -> secondCallback.set(Vertx.currentContext()))));
		onContext(emitter, () -> type.emit.accept(listeners));
		// Drain each registration context after the emitter has scheduled both callbacks.
		onContext(first, () -> assertSame(first, firstCallback.get()));
		onContext(second, () -> assertSame(second, secondCallback.get()));
	}

	@ParameterizedTest
	@EnumSource(ListenerType.class)
	void scheduledCallbackExceptionsStayIsolated(ListenerType type, Vertx vertx) throws Exception {
		Context caller = vertx.getOrCreateContext();
		Context emitter = vertx.getOrCreateContext();
		AtomicReference<Throwable> uncaught = new AtomicReference<>();
		caller.exceptionHandler(uncaught::set);
		AtomicInteger received = new AtomicInteger();
		PhotonMessagingListeners listeners = new PhotonMessagingListeners();
		onContext(caller, () -> {
			type.add.accept(listeners, new RecordingListener(() -> { throw new IllegalStateException("listener failed"); }));
			type.add.accept(listeners, new RecordingListener(received::incrementAndGet));
		});
		onContext(emitter, () -> type.emit.accept(listeners));
		onContext(caller, () -> {
			assertEquals(1, received.get());
			assertNull(uncaught.get());
		});
	}

	@ParameterizedTest
	@EnumSource(ListenerType.class)
	void removalUsesTheOriginalListenerAndRemoveAllClearsTheRest(ListenerType type, Vertx vertx) throws Exception {
		Context caller = vertx.getOrCreateContext();
		Context emitter = vertx.getOrCreateContext();
		AtomicInteger removedEvents = new AtomicInteger();
		AtomicInteger keptEvents = new AtomicInteger();
		RecordingListener removed = new RecordingListener(removedEvents::incrementAndGet);
		RecordingListener kept = new RecordingListener(keptEvents::incrementAndGet);
		PhotonMessagingListeners listeners = new PhotonMessagingListeners();
		onContext(caller, () -> {
			type.add.accept(listeners, removed);
			type.add.accept(listeners, kept);
		});
		type.remove.accept(listeners, removed);
		onContext(emitter, () -> type.emit.accept(listeners));
		onContext(caller, () -> {
			assertEquals(0, removedEvents.get());
			assertEquals(1, keptEvents.get());
		});
		listeners.removeAllListeners();
		onContext(emitter, () -> type.emit.accept(listeners));
		onContext(caller, () -> assertEquals(1, keptEvents.get()));
	}

	@ParameterizedTest
	@EnumSource(ListenerType.class)
	void registrationWithoutAContextUsesTheEmitterContext(ListenerType type, Vertx vertx) throws Exception {
		assertNull(Vertx.currentContext());
		PhotonMessagingListeners listeners = new PhotonMessagingListeners();
		AtomicReference<Context> callback = new AtomicReference<>();
		type.add.accept(listeners, new RecordingListener(() -> callback.set(Vertx.currentContext())));
		Context emitter = vertx.getOrCreateContext();
		onContext(emitter, () -> {
			type.emit.accept(listeners);
			assertSame(emitter, callback.get());
		});
	}
}
