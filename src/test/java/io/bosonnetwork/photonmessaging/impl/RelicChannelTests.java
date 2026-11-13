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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.mqtt.MqttServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import io.vertx.junit5.VertxExtension;

import io.bosonnetwork.CryptoContext;
import io.bosonnetwork.Id;
import io.bosonnetwork.crypto.Signature;
import io.bosonnetwork.photonmessaging.Channel;
import io.bosonnetwork.photonmessaging.ChannelListener;
import io.bosonnetwork.photonmessaging.Configuration;
import io.bosonnetwork.photonmessaging.Contact;
import io.bosonnetwork.photonmessaging.Message;
import io.bosonnetwork.photonmessaging.MessagingStore.StoredContact;
import io.bosonnetwork.photonmessaging.impl.database.SqliteDatabase;
import io.bosonnetwork.vertx.ContextualFuture;

@ExtendWith(VertxExtension.class)
class RelicChannelTests {
	@TempDir Path directory;
	private Vertx vertx;
	private MqttServer server;
	private SqliteDatabase store;
	private PhotonMessagingClient client;
	private MessagingRepository repository;
	private final List<Channel> left = new ArrayList<>();
	private final List<Context> callbackContexts = new ArrayList<>();
	private Promise<Void> removalGate;
	private final Promise<Void> removalPersisted = Promise.promise();
	private boolean foreignCompletion;
	private boolean foreignReadCompletion;
	private Context putContext;
	private boolean failRemoval;

	@BeforeEach
	void setUp(Vertx vertx) throws Exception {
		this.vertx = vertx;
		server = await(MqttServer.create(vertx).endpointHandler(endpoint -> {
			endpoint.subscribeHandler(subscription -> endpoint.subscribeAcknowledge(subscription.messageId(),
					subscription.topicSubscriptions().stream().map(topic -> topic.qualityOfService()).collect(Collectors.toList())));
			endpoint.unsubscribeHandler(subscription -> endpoint.unsubscribeAcknowledge(subscription.messageId()));
			endpoint.accept(false);
		}).listen(0, "127.0.0.1"));
		store = new SqliteDatabase("jdbc:sqlite:" + directory.resolve("messages.db"), 1) {
			@Override
			public Future<List<StoredContact>> getContacts(Collection<Id> ids) {
				Future<List<StoredContact>> loaded = super.getContacts(ids);
				if (!foreignReadCompletion)
					return loaded;
				Promise<List<StoredContact>> result = Promise.promise();
				loaded.onComplete(ar -> CompletableFuture.runAsync(() -> result.handle(ar)));
				return result.future();
			}

			@Override
			public Future<Void> putContacts(int revision, Collection<StoredContact> contacts) {
				putContext = Vertx.currentContext();
				return super.putContacts(revision, contacts);
			}

			@Override
			public Future<Boolean> removeContacts(int revision, Collection<Id> ids) {
				if (failRemoval)
					return Future.failedFuture("Contact removal failed");
				Future<Boolean> removed = super.removeContacts(revision, ids).compose(value -> {
					removalPersisted.tryComplete();
					return removalGate == null ? Future.succeededFuture(value) : removalGate.future().map(value);
				});
				if (!foreignCompletion)
					return removed;
				Promise<Boolean> result = Promise.promise();
				removed.onComplete(ar -> CompletableFuture.runAsync(() -> result.handle(ar)));
				return result.future();
			}
		};
		Configuration config = Configuration.builder().userKey(Signature.KeyPair.random()).deviceKey(Signature.KeyPair.random())
				.servicePeerId(Id.of(Signature.KeyPair.random().publicKey().bytes()))
				.serviceEndpoint("mqtt://127.0.0.1:" + server.actualPort()).dataDir(directory).store(store).build();
		client = new PhotonMessagingClient(vertx, null, config);
		await(client.start());
		repository = (MessagingRepository) field("repository").get(client);
		client.addChannelListener((ChannelListener) Proxy.newProxyInstance(ChannelListener.class.getClassLoader(),
				new Class<?>[] { ChannelListener.class }, (proxy, method, args) -> switch (method.getName()) {
					case "onLeftChannel" -> {
						left.add((Channel) args[0]);
						callbackContexts.add(Vertx.currentContext());
						yield null;
					}
					case "equals" -> proxy == args[0];
					case "hashCode" -> System.identityHashCode(proxy);
					case "toString" -> "Relic channel listener";
					default -> null;
				}));
	}

	@AfterEach
	void tearDown() throws Exception {
		try {
			if (client != null)
				await(client.stop());
		} finally {
			if (server != null)
				await(server.close());
		}
	}

	private static <T> T await(Future<T> future) throws Exception {
		return future.toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
	}

	private static <T> T await(CompletableFuture<T> future) throws Exception {
		return future.get(10, TimeUnit.SECONDS);
	}

	private static Field field(String name) throws Exception {
		Field field = PhotonMessagingClient.class.getDeclaredField(name);
		field.setAccessible(true);
		return field;
	}

	private <T> T onClient(Supplier<Future<T>> operation) throws Exception {
		Context context = Objects.requireNonNull(client.vertxContext());
		return await(ContextualFuture.callOnContext(context, operation));
	}

	private Future<Void> begin(Supplier<Future<Void>> operation) throws Exception {
		return onClient(() -> Future.succeededFuture(operation.get()));
	}

	@SuppressWarnings("unchecked")
	private Future<Void> invoke(String name, Class<?> parameterType, Object argument) {
		try {
			Method method = PhotonMessagingClient.class.getDeclaredMethod(name, parameterType);
			method.setAccessible(true);
			return (Future<Void>) method.invoke(client, argument);
		} catch (ReflectiveOperationException e) {
			return Future.failedFuture(e);
		}
	}

	private PhotonChannel channel(Id id, int revision, String name) {
		return new PhotonChannel(id, new byte[PhotonContact.ENCRYPTED_SESSION_KEY_BYTES], Id.random(),
				Channel.Permission.PUBLIC, name, null, false, null, null, false, false, 1L, 1L, revision);
	}

	private void seed(List<PhotonChannel> channels, int revision) throws Exception {
		onClient(() -> repository.putContacts(revision, new ArrayList<Contact>(channels)).compose(v -> {
			Future<Void> members = Future.succeededFuture();
			for (PhotonChannel channel : channels) {
				members = members.compose(ignored -> repository.putChannelMembers(channel.getId(), List.of(
						new ChannelMember(channel.getOwnerId(), Channel.Role.OWNER, 1L),
						new ChannelMember(client.getUserId(), Channel.Role.MEMBER, 1L))));
			}
			return members;
		}).onSuccess(v -> {
			try {
				field("contactsRevision").setInt(client, revision);
			} catch (Exception e) {
				throw new IllegalStateException(e);
			}
		}));
	}

	private Future<Void> remove(List<Id> ids) {
		return invoke("enqueueContactSync", ContactSync.class,
				ContactSync.delta(2, List.of(ContactMutation.remove(1, ids))));
	}

	private Future<Void> leave(Id channelId, Id notificationId) {
		Notification notification = new Notification(notificationId, client.getUserId(), 1L, Notification.Event.CHANNEL_LEAVE, null);
		BytesMessage message = new BytesMessage(Id.random(), client.getUserId(), Message.Type.STATE_MESSAGE, 1L, notification.serialize());
		message.setFrom(channelId);
		return invoke("processInboxMessage", BytesMessage.class, message);
	}

	@ParameterizedTest
	@CsvSource({ "false,false", "true,false", "false,true" })
	void syncedRemovalRetainsOnlyNotificationMetadata(boolean cached, boolean foreign) throws Exception {
		Id id = Id.random();
		seed(List.of(channel(id, 1, "Removed")), 1);
		if (cached)
			assertTrue(await(client.getContact(id)).isPresent());
		foreignCompletion = foreign;
		onClient(() -> remove(List.of(id)));
		assertTrue(await(client.getContact(id)).isEmpty());
		assertTrue(await(client.getContacts()).isEmpty());
		Id notification = Id.random();
		onClient(() -> leave(id, notification));
		onClient(() -> leave(id, notification));
		assertEquals(List.of(id), left.stream().map(Channel::getId).toList());
		assertEquals(List.of(client.vertxContext()), callbackContexts);
		assertEquals("Removed", left.get(0).getName().orElseThrow());
		assertTrue(await(client.getContact(id)).isEmpty());
	}

	@Test
	void foreignRemovalCompletionKeepsTheNextMutationOnTheClientContext() throws Exception {
		PhotonChannel removed = channel(Id.random(), 1, "Removed");
		PhotonChannel updated = channel(Id.random(), 1, "Updated");
		seed(List.of(removed, updated), 1);
		foreignReadCompletion = true;
		foreignCompletion = true;
		CryptoContext self = (CryptoContext) field("selfContext").get(client);
		onClient(() -> invoke("enqueueContactSync", ContactSync.class, ContactSync.delta(3, List.of(
				ContactMutation.remove(1, List.of(removed.getId())),
				ContactMutation.update(2, updated.toOpaque(self))))));
		assertEquals(client.vertxContext(), putContext);
		assertTrue(await(client.getContact(removed.getId())).isEmpty());
		assertTrue(await(client.getContact(updated.getId())).isPresent());
	}

	@Test
	void leaveBeforeContactSyncIsNotReportedTwice() throws Exception {
		Id id = Id.random();
		seed(List.of(channel(id, 1, "Removed")), 1);
		Id notification = Id.random();
		onClient(() -> leave(id, notification));
		onClient(() -> remove(List.of(id)));
		onClient(() -> leave(id, notification));
		assertEquals(1, left.size());
		assertTrue(await(client.getContact(id)).isEmpty());
	}

	@Test
	void leaveWaitsForCommittedRemovalWhoseCompletionIsPending() throws Exception {
		Id id = Id.random();
		seed(List.of(channel(id, 1, "Removed")), 1);
		removalGate = Promise.promise();
		Future<Void> syncing = begin(() -> remove(List.of(id)));
		await(removalPersisted.future());
		Future<Void> notification = begin(() -> leave(id, Id.random()));
		assertFalse(notification.isComplete());
		assertTrue(left.isEmpty());
		onClient(() -> { removalGate.complete(); return Future.succeededFuture(); });
		await(syncing);
		await(notification);
		assertEquals(1, left.size());
	}

	@Test
	void lateLeaveAndItsDuplicateDoNotRemoveARejoinedContact() throws Exception {
		Id id = Id.random();
		seed(List.of(channel(id, 1, "Old membership")), 1);
		onClient(() -> remove(List.of(id)));
		assertTrue(await(client.getContact(id)).isEmpty());
		seed(List.of(channel(id, 3, "Rejoined")), 3);
		Id notification = Id.random();
		onClient(() -> leave(id, notification));
		onClient(() -> leave(id, notification));
		assertEquals(1, left.size());
		assertEquals("Old membership", left.get(0).getName().orElseThrow());
		assertEquals("Rejoined", await(client.getContact(id)).orElseThrow().getName().orElseThrow());
	}

	@Test
	void aLaterRemovalReplacesThePreviousMembershipRelic() throws Exception {
		Id id = Id.random();
		seed(List.of(channel(id, 1, "First membership")), 1);
		onClient(() -> remove(List.of(id)));
		Id first = Id.random();
		onClient(() -> leave(id, first));
		seed(List.of(channel(id, 3, "Second membership")), 3);
		onClient(() -> invoke("enqueueContactSync", ContactSync.class,
				ContactSync.delta(4, List.of(ContactMutation.remove(3, List.of(id))))));
		onClient(() -> leave(id, Id.random()));
		onClient(() -> leave(id, first));
		assertEquals(List.of("First membership", "Second membership"),
				left.stream().map(channel -> channel.getName().orElseThrow()).toList());
	}

	@Test
	void snapshotRemovalAlsoRetainsAnUncachedChannel() throws Exception {
		Id id = Id.random();
		seed(List.of(channel(id, 1, "Snapshot removal")), 1);
		onClient(() -> invoke("enqueueContactSync", ContactSync.class, ContactSync.snapshot(2, List.of())));
		onClient(() -> leave(id, Id.random()));
		assertEquals(1, left.size());
		assertTrue(await(client.getContact(id)).isEmpty());
	}

	@Test
	void failedSyncedRemovalStillAllowsLeaveToRemoveTheLiveContact() throws Exception {
		Id id = Id.random();
		seed(List.of(channel(id, 1, "Failed removal")), 1);
		failRemoval = true;
		assertThrows(ExecutionException.class, () -> onClient(() -> remove(List.of(id))));
		onClient(() -> leave(id, Id.random()));
		assertEquals(1, left.size());
		assertTrue(await(client.getContact(id)).isEmpty());
	}

	@Test
	void relicRetentionIsBounded() throws Exception {
		List<PhotonChannel> channels = IntStream.range(0, 513).mapToObj(i -> channel(Id.random(), 1, "Channel " + i)).toList();
		seed(channels, 1);
		onClient(() -> remove(channels.stream().map(Channel::getId).toList()));
		for (PhotonChannel channel : channels)
			onClient(() -> leave(channel.getId(), Id.random()));
		assertEquals(512, left.size());
	}

	@Test
	void expiredRelicsDoNotHandleNotifications() throws Exception {
		// Use the same cache with a short lifetime, without making the suite wait a minute.
		AsyncCache<Id, Object> cache = AsyncCache.<Id, Object>newBuilder().maximumSize(512)
				.expireAfterWrite(Duration.ofMillis(5)).build(id -> Future.succeededFuture(null));
		field("relicChannelCache").set(client, cache);
		Id id = Id.random();
		seed(List.of(channel(id, 1, "Expired")), 1);
		onClient(() -> remove(List.of(id)));
		Promise<Void> expired = Promise.promise();
		vertx.setTimer(50, timer -> expired.complete());
		await(expired.future());
		onClient(() -> leave(id, Id.random()));
		assertTrue(left.isEmpty());
	}

	@Test
	void stoppingTheClientClearsRelics() throws Exception {
		Id id = Id.random();
		seed(List.of(channel(id, 1, "Stopped")), 1);
		onClient(() -> remove(List.of(id)));
		await(client.stop());
		await(client.start());
		onClient(() -> leave(id, Id.random()));
		assertTrue(left.isEmpty());
	}
}
