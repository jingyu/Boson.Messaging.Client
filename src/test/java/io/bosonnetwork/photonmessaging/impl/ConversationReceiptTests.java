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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

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
import org.junit.jupiter.params.provider.EnumSource;
import io.vertx.junit5.VertxExtension;

import io.bosonnetwork.Id;
import io.bosonnetwork.crypto.Signature;
import io.bosonnetwork.photonmessaging.Configuration;
import io.bosonnetwork.photonmessaging.Contact;
import io.bosonnetwork.photonmessaging.Conversation;
import io.bosonnetwork.photonmessaging.DeliveryStatus;
import io.bosonnetwork.photonmessaging.Message;
import io.bosonnetwork.photonmessaging.MessageDeliveryReceipt;
import io.bosonnetwork.photonmessaging.MessagingStore.StoredContact;
import io.bosonnetwork.photonmessaging.MessagingStore.StoredConversation;
import io.bosonnetwork.photonmessaging.MessagingStore.StoredMessage;
import io.bosonnetwork.photonmessaging.impl.database.SqliteDatabase;
import io.bosonnetwork.vertx.ContextualFuture;

@ExtendWith(VertxExtension.class)
class ConversationReceiptTests {
	@TempDir Path directory;
	private Vertx vertx;
	private MqttServer server;
	private SqliteDatabase store;
	private PhotonMessagingClient client;
	private Configuration config;
	private boolean foreignConversationCompletion;
	private Context conversationReadContext;

	@BeforeEach
	void setUp(Vertx vertx) throws Exception {
		this.vertx = vertx;
		server = await(MqttServer.create(vertx).endpointHandler(endpoint -> {
			endpoint.subscribeHandler(subscription -> endpoint.subscribeAcknowledge(subscription.messageId(),
					subscription.topicSubscriptions().stream().map(topic -> topic.qualityOfService()).collect(Collectors.toList())));
			endpoint.unsubscribeHandler(subscription -> endpoint.unsubscribeAcknowledge(subscription.messageId()));
			endpoint.accept(false);
		}).listen(0, "127.0.0.1"));
		startClient();
	}

	private void startClient() throws Exception {
		store = new SqliteDatabase("jdbc:sqlite:" + directory.resolve("messages.db"), 1) {
			@Override
			public Future<List<StoredConversation>> getAllConversations() {
				conversationReadContext = Vertx.currentContext();
				Future<List<StoredConversation>> loaded = super.getAllConversations();
				if (!foreignConversationCompletion)
					return loaded;
				Promise<List<StoredConversation>> completed = Promise.promise();
				loaded.onComplete(ar -> CompletableFuture.runAsync(() -> completed.handle(ar)));
				return completed.future();
			}
		};
		if (config == null) {
			config = Configuration.builder().userKey(Signature.KeyPair.random()).deviceKey(Signature.KeyPair.random())
					.servicePeerId(Id.of(Signature.KeyPair.random().publicKey().bytes()))
					.serviceEndpoint("mqtt://127.0.0.1:" + server.actualPort()).dataDir(directory).store(store).build();
		} else {
			config = Configuration.builder().userKey(config.getUserKey()).deviceKey(config.getDeviceKey())
					.servicePeerId(config.getServicePeerId()).serviceEndpoint(config.getServiceEndpoint().toString())
					.dataDir(directory).store(store).build();
		}
		client = new PhotonMessagingClient(vertx, null, config);
		await(client.start());
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

	private StoredMessage putMessage(Id conversationId, long date, String text) throws Exception {
		await(store.putContactLocally(new StoredContact(conversationId, Contact.Type.FRIEND.value(),
				new byte[PhotonContact.ENCRYPTED_SESSION_KEY_BYTES], "Bob", null, null, false, false, 1, 1L, 1L, null)));
		StoredMessage message = new StoredMessage(0L, Id.random(), conversationId, 2, conversationId,
				Message.Type.CONTENT_MESSAGE.value(), client.getUserId(), date, "text/plain", null,
				MessageContent.text(text).serialize(), date, date, DeliveryStatus.UNDELIVERED, null, 0L);
		await(store.putMessage(message));
		return message;
	}

	private void receive(StoredMessage message, DeliveryStatus status) throws Exception {
		MessageDeliveryReceipt receipt = new MessageDeliveryReceipt(message.id(), status,
				status == DeliveryStatus.DELIVERED ? null : "delivery reason", 123L);
		await(ContextualFuture.callOnContext(Objects.requireNonNull(client.vertxContext()),
				() -> client.processDeliveryReceipt(receipt)));
	}

	private static void assertReceipt(Message message, DeliveryStatus status) {
		assertEquals(status, message.getDeliveryStatus());
		assertEquals(status == DeliveryStatus.DELIVERED ? null : "delivery reason", message.getDeliveryReason());
		assertEquals(123L, message.getDeliveryAt());
	}

	@ParameterizedTest
	@EnumSource(value = DeliveryStatus.class, names = {"DELIVERED", "RECIPIENT_UNKNOWN", "EXPIRED"})
	void receiptSurvivesReopenThroughPublicMessageAndConversationApis(DeliveryStatus status) throws Exception {
		Id peer = Id.random();
		StoredMessage original = putMessage(peer, 10L, "stored receipt");
		receive(original, status);
		await(client.stop());
		startClient();

		Message paged = await(client.getMessagesBefore(peer, Long.MAX_VALUE, 10, 0)).get(0);
		assertEquals(original.id(), paged.getId());
		assertReceipt(paged, status);
		Message ranged = await(client.getMessagesInRange(peer, 0L, Long.MAX_VALUE)).get(0);
		assertReceipt(ranged, status);
		Conversation conversation = await(client.getConversation(peer)).orElseThrow();
		assertReceipt(conversation.getLastMessage().orElseThrow(), status);
		assertReceipt(await(client.getConversations()).get(0).getLastMessage().orElseThrow(), status);
	}

	@ParameterizedTest
	@EnumSource(value = DeliveryStatus.class, names = {"DELIVERED", "RECIPIENT_UNKNOWN", "EXPIRED"})
	void receiptUpdatesCachedLatestMessageWithoutChangingPreviewOrDate(DeliveryStatus status) throws Exception {
		Id peer = Id.random();
		StoredMessage original = putMessage(peer, 10L, "cached preview");
		Conversation cached = await(client.getConversation(peer)).orElseThrow();
		Message last = cached.getLastMessage().orElseThrow();
		assertEquals(DeliveryStatus.UNDELIVERED, last.getDeliveryStatus());
		assertEquals(0L, last.getDeliveryAt());
		String preview = cached.getPreview().orElseThrow();
		long date = cached.getUpdatedAt();

		receive(original, status);

		assertSame(cached, await(client.getConversation(peer)).orElseThrow());
		assertSame(last, cached.getLastMessage().orElseThrow());
		assertReceipt(last, status);
		assertSame(cached, await(client.getConversations()).get(0));
		assertReceipt(cached.getLastMessage().orElseThrow(), status);
		assertEquals(preview, cached.getPreview().orElseThrow());
		assertEquals(date, cached.getUpdatedAt());
	}

	@Test
	void conversationListRefreshesStoredReceiptWithoutReplacingCachedConversation() throws Exception {
		Id peer = Id.random();
		StoredMessage original = putMessage(peer, 10L, "cached preview");
		Conversation cached = await(client.getConversation(peer)).orElseThrow();
		assertEquals(peer, await(store.updateMessageDeliveryStatus(original.id(), DeliveryStatus.EXPIRED, "delivery reason", 123L)));

		assertSame(cached, await(client.getConversations()).get(0));
		assertReceipt(cached.getLastMessage().orElseThrow(), DeliveryStatus.EXPIRED);
	}

	@Test
	void conversationReadStartsOnClientContextAndAnswersOnCallerContext() throws Exception {
		Id peer = Id.random();
		StoredMessage original = putMessage(peer, 10L, "cached preview");
		Conversation cached = await(client.getConversation(peer)).orElseThrow();
		await(store.updateMessageDeliveryStatus(original.id(), DeliveryStatus.EXPIRED, "delivery reason", 123L));
		foreignConversationCompletion = true;
		Context caller = vertx.getOrCreateContext();
		AtomicReference<Context> completedOn = new AtomicReference<>();
		CompletableFuture<List<Conversation>> result = new CompletableFuture<>();
		caller.runOnContext(v -> client.getConversations().whenComplete((conversations, error) -> {
			if (error != null) {
				result.completeExceptionally(error);
			} else {
				completedOn.set(Vertx.currentContext());
				result.complete(conversations);
			}
		}));

		List<Conversation> conversations = await(result);
		assertSame(client.vertxContext(), conversationReadContext);
		assertSame(caller, completedOn.get());
		assertSame(cached, conversations.get(0));
		assertReceipt(cached.getLastMessage().orElseThrow(), DeliveryStatus.EXPIRED);
	}

	@Test
	void olderMessagesReceiptDoesNotChangeTheLatestMessage() throws Exception {
		Id peer = Id.random();
		StoredMessage older = putMessage(peer, 10L, "older message");
		StoredMessage newer = putMessage(peer, 20L, "latest message");
		Conversation cached = await(client.getConversation(peer)).orElseThrow();
		receive(older, DeliveryStatus.RECIPIENT_UNKNOWN);

		Conversation refreshed = await(client.getConversations()).get(0);
		assertSame(cached, refreshed);
		Message last = refreshed.getLastMessage().orElseThrow();
		assertEquals(newer.id(), last.getId());
		assertEquals(DeliveryStatus.UNDELIVERED, last.getDeliveryStatus());
		assertEquals(0L, last.getDeliveryAt());
		assertEquals("latest message", refreshed.getPreview().orElseThrow());
		assertReceipt(await(client.getMessagesInRange(peer, 0L, 20L)).get(0), DeliveryStatus.RECIPIENT_UNKNOWN);
	}

	@Test
	void delayedMessageUpdatePreservesItsReceipt() throws Exception {
		Id peer = Id.random();
		StoredMessage original = putMessage(peer, 10L, "cached preview");
		Conversation cached = await(client.getConversation(peer)).orElseThrow();
		receive(original, DeliveryStatus.EXPIRED);
		PhotonMessage<MessageContent> echo = new PhotonMessage<>(original.id(), peer, Message.Type.CONTENT_MESSAGE,
				original.createdAt(), MessageContent.text("cached preview"));
		echo.setConversationId(peer);
		await(ContextualFuture.callOnContext(Objects.requireNonNull(client.vertxContext()), () -> {
			((PhotonConversation) cached).update(echo);
			return Future.succeededFuture();
		}));
		assertReceipt(cached.getLastMessage().orElseThrow(), DeliveryStatus.EXPIRED);
	}

	@Test
	void conversationWithoutMessagesHasAnEmptyLastMessage() {
		Friend contact = new Friend(Id.random(), new byte[PhotonContact.ENCRYPTED_SESSION_KEY_BYTES],
				"Bob", null, null, false, false, 1L, 1L, 1);
		Conversation conversation = new PhotonConversation(contact, c -> null);
		assertTrue(conversation.getLastMessage().isEmpty());
	}
}
