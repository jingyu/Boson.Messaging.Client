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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

import io.bosonnetwork.Id;
import io.bosonnetwork.crypto.Signature;
import io.bosonnetwork.photonmessaging.Configuration;
import io.bosonnetwork.photonmessaging.DeliveryStatus;
import io.bosonnetwork.photonmessaging.Message;
import io.bosonnetwork.photonmessaging.MessageDeliveryReceipt;
import io.bosonnetwork.photonmessaging.MessageListener;
import io.bosonnetwork.photonmessaging.MessagingStore.StoredMessage;
import io.bosonnetwork.photonmessaging.impl.database.SqliteDatabase;
import io.bosonnetwork.vertx.ContextualFuture;

@ExtendWith(VertxExtension.class)
class EarlyDeliveryReceiptTests {
	@TempDir Path directory;
	private SqliteDatabase store;
	private PhotonMessagingClient client;
	private Context clientContext;
	private Context listenerContext;
	private Context updateContext;
	private final List<MessageDeliveryReceipt> received = new ArrayList<>();
	private Id delayedMessage;
	private Promise<Boolean> lookup;
	private boolean failUpdate;
	private boolean failPut;
	private boolean foreignPutCompletion;
	private Promise<Void> putGate;
	private final List<String> events = new ArrayList<>();
	private int updates;

	private final Promise<Void> receiptNotified = Promise.promise();
	private final Promise<Void> sentNotified = Promise.promise();

	@BeforeEach
	void setUp(Vertx vertx, VertxTestContext context) {
		clientContext = vertx.getOrCreateContext();
		listenerContext = vertx.getOrCreateContext();
		store = new SqliteDatabase("jdbc:sqlite:" + directory.resolve("messages.db"), 1) {
			@Override
			public Future<Long> putMessage(StoredMessage message) {
				if (failPut)
					return Future.failedFuture("outbox write failed");
				Future<Long> stored = putGate == null ? super.putMessage(message) : putGate.future().compose(v -> super.putMessage(message));
				if (!foreignPutCompletion)
					return stored;
				Promise<Long> completed = Promise.promise();
				stored.onComplete(ar -> CompletableFuture.runAsync(() -> completed.handle(ar)));
				return completed.future();
			}

			@Override
			public Future<Boolean> existsMessage(Id id) {
				return id.equals(delayedMessage) ? lookup.future() : super.existsMessage(id);
			}

			@Override
			public Future<Id> updateMessageDeliveryStatus(Id id, DeliveryStatus status, String reason, long date) {
				updates++;
				updateContext = Vertx.currentContext();
				return failUpdate ? Future.failedFuture("status write failed") :
						super.updateMessageDeliveryStatus(id, status, reason, date);
			}
		};
		Configuration config = Configuration.builder().userKey(Signature.KeyPair.random())
				.deviceKey(Signature.KeyPair.random())
				.servicePeerId(Id.of(Signature.KeyPair.random().publicKey().bytes()))
				.serviceEndpoint("mqtt://127.0.0.1:1883").dataDir(directory).store(store).build();
		client = new PhotonMessagingClient(vertx, null, config);
		listenerContext.runOnContext(v -> {
			client.addMessageListener(new MessageListener() {
				public void onMessage(Message message) {
				}

				public void onSent(Message message) {
					events.add(Vertx.currentContext() == listenerContext ? "sent" : "foreign-sent");
					sentNotified.tryComplete();
				}

				public void onDeliveryReceipt(MessageDeliveryReceipt receipt) {
					received.add(receipt);
					events.add(Vertx.currentContext() == listenerContext ? "receipt" : "foreign-receipt");
					receiptNotified.tryComplete();
				}
			});
			run(() -> store.open(vertx).mapEmpty()).onComplete(context.succeedingThenComplete());
		});
	}

	@AfterEach
	void tearDown(VertxTestContext context) {
		store.close().onComplete(context.succeedingThenComplete());
	}

	private Future<Void> run(Supplier<Future<Void>> operation) {
		return ContextualFuture.callOnContext(clientContext, operation);
	}

	private Future<Void> awaitReceipt() {
		return Future.fromCompletionStage(receiptNotified.future().toCompletionStage(), clientContext);
	}

	private Future<Void> awaitSent() {
		return Future.fromCompletionStage(sentNotified.future().toCompletionStage(), clientContext);
	}

	private Future<Void> drainListeners() {
		Promise<Void> done = Promise.promise();
		listenerContext.runOnContext(v -> done.complete());
		return Future.fromCompletionStage(done.future().toCompletionStage(), clientContext);
	}

	private StoredMessage row(Id id) {
		return new StoredMessage(0, id, Id.random(), 1, Id.random(), Message.Type.CONTENT_MESSAGE.value(),
				Id.random(), 1L, "text/plain", null, new byte[] {1}, 1L, 0L,
				DeliveryStatus.UNDELIVERED, null, 0L);
	}

	private MessageDeliveryReceipt receipt(Id id) {
		return new MessageDeliveryReceipt(id, DeliveryStatus.RECIPIENT_UNKNOWN, null, 123L);
	}

	private Future<Void> outbox(Id id) {
		BytesMessage message = new BytesMessage(id, Id.random(), Message.Type.CONTENT_MESSAGE, 1L,
				MessageContent.text("outbox content").serialize());
		message.setFrom(client.getUserId());
		message.setConversationId(message.getRecipient());
		try {
			java.lang.reflect.Method method = PhotonMessagingClient.class.getDeclaredMethod("processingOutboxMessage", BytesMessage.class);
			method.setAccessible(true);
			@SuppressWarnings("unchecked") Future<Void> result = (Future<Void>) method.invoke(client, message);
			return result;
		} catch (Exception e) { return Future.failedFuture(e); }
	}

	@Test
	void pendingOutboxWriteAppliesItsCachedReceiptAfterOnSent(VertxTestContext context) {
		Id id = Id.random();
		putGate = Promise.promise();
		foreignPutCompletion = true;
		run(() -> {
			Future<Void> writing = outbox(id);
			return client.processDeliveryReceipt(receipt(id)).compose(v -> {
				context.verify(() -> { assertFalse(writing.isComplete()); assertTrue(events.isEmpty()); });
				putGate.complete();
				return writing;
			}).compose(v -> awaitReceipt()).compose(v -> store.getMessage(id)).map(saved -> {
				context.verify(() -> {
					assertSame(clientContext, updateContext);
					assertEquals(DeliveryStatus.RECIPIENT_UNKNOWN, saved.deliveryStatus());
					assertEquals(List.of("sent", "receipt"), events);
				});
				return (Void) null;
			});
		}).onComplete(context.succeedingThenComplete());
	}

	@Test
	void failedOutboxWriteStillNotifiesSentWithoutCreatingAPhantomMessage(VertxTestContext context) {
		Id id = Id.random();
		failPut = true;
		run(() -> client.processDeliveryReceipt(receipt(id)).compose(v -> outbox(id))
				.compose(v -> Future.<Void>failedFuture("expected outbox failure"), e -> Future.succeededFuture())
				.compose(v -> awaitSent()).compose(v -> drainListeners())
				.compose(v -> store.getMessage(id)).map(saved -> {
					context.verify(() -> {
						assertNull(saved);
						assertEquals(List.of("sent"), events);
						assertTrue(received.isEmpty());
						assertEquals(0, updates);
					});
					return (Void) null;
				})).onComplete(context.succeedingThenComplete());
	}

	@Test
	void earlyReceiptIsAppliedAfterTheOriginalMessageIsStored(VertxTestContext context) {
		Id id = Id.random();
		run(() -> client.processDeliveryReceipt(receipt(id)).compose(v -> {
			context.verify(() -> { assertEquals(0, updates); assertTrue(received.isEmpty()); });
			return store.putMessage(row(id)).compose(rid -> client.applyCachedDeliveryReceipt(id));
		}).compose(v -> client.applyCachedDeliveryReceipt(id)).compose(v -> awaitReceipt()).compose(v -> store.getMessage(id)).map(saved -> {
			context.verify(() -> {
				assertEquals(DeliveryStatus.RECIPIENT_UNKNOWN, saved.deliveryStatus());
				assertEquals(123L, saved.deliveryAt());
				assertEquals(1, updates);
				assertEquals(1, received.size());
			});
			return (Void) null;
		})).onComplete(context.succeedingThenComplete());
	}

	@Test
	void aLateMissingLookupCannotLoseOrApplyTheReceiptTwice(VertxTestContext context) {
		Id id = Id.random();
		delayedMessage = id;
		lookup = Promise.promise();
		run(() -> {
			Future<Void> first = client.processDeliveryReceipt(receipt(id));
			return store.putMessage(row(id)).compose(rid -> client.applyCachedDeliveryReceipt(id))
					.compose(v -> awaitReceipt()).compose(v -> {
						context.verify(() -> {
							assertFalse(first.isComplete());
							assertEquals(1, received.size());
						});
						lookup.complete(false);
						return first;
					}).compose(v -> client.applyCachedDeliveryReceipt(id)).compose(v -> drainListeners()).map(v -> {
						context.verify(() -> {
							assertEquals(1, updates);
							assertEquals(1, received.size());
						});
						return (Void) null;
					});
		}).onComplete(context.succeedingThenComplete());
	}

	@Test
	void knownMessageReceiptDoesNotWaitForAnotherMessagesLookup(VertxTestContext context) {
		Id missing = Id.random();
		Id known = Id.random();
		delayedMessage = missing;
		lookup = Promise.promise();
		run(() -> store.putMessage(row(known)).compose(rid -> {
			Future<Void> pending = client.processDeliveryReceipt(receipt(missing));
			return client.processDeliveryReceipt(receipt(known)).compose(v -> awaitReceipt()).map(v -> {
				context.verify(() -> {
					assertFalse(pending.isComplete());
					assertEquals(List.of(known), received.stream().map(MessageDeliveryReceipt::getMessageId).toList());
				});
				lookup.complete(false);
				return (Void) null;
			}).compose(v -> pending);
		})).onComplete(context.succeedingThenComplete());
	}

	@Test
	void workerCompletedLookupKeepsStatusProcessingOnTheClientContext(VertxTestContext context) {
		Id id = Id.random();
		delayedMessage = id;
		lookup = Promise.promise();
		StoredMessage message = row(id);
		run(() -> store.putMessage(message).compose(rid -> {
			Future<Void> processing = client.processDeliveryReceipt(receipt(id));
			CompletableFuture.runAsync(() -> lookup.complete(true));
			return processing;
		}).compose(v -> awaitReceipt()).map(v -> {
			context.verify(() -> {
				assertSame(clientContext, updateContext);
				assertEquals(List.of("receipt"), events);
			});
			return (Void) null;
		})).onComplete(context.succeedingThenComplete());
	}

	@Test
	void unknownReceiptCreatesNeitherAMessageNorACallback(VertxTestContext context) {
		Id id = Id.random();
		run(() -> client.processDeliveryReceipt(receipt(id)).compose(v -> drainListeners()).compose(v -> store.getMessage(id)).map(saved -> {
			context.verify(() -> { assertNull(saved); assertEquals(0, updates); assertTrue(received.isEmpty()); });
			return (Void) null;
		})).onComplete(context.succeedingThenComplete());
	}

	@Test
	void messageDeletedDuringLookupStillCompletesReceiptNotification(VertxTestContext context) {
		Id id = Id.random();
		delayedMessage = id;
		lookup = Promise.promise();
		run(() -> store.putMessage(row(id)).compose(rid -> {
			Future<Void> processing = client.processDeliveryReceipt(receipt(id));
			return store.removeMessages(List.of(rid)).compose(removed -> {
				lookup.complete(true);
				return processing;
			});
		}).compose(v -> awaitReceipt()).compose(v -> store.getMessage(id)).map(saved -> {
			context.verify(() -> {
				assertNull(saved);
				assertEquals(1, updates);
				assertEquals(1, received.size());
			});
			return (Void) null;
		})).onComplete(context.succeedingThenComplete());
	}

	@Test
	void failedStatusWriteStillNotifiesWithoutRetryingTheClaimedEntry(VertxTestContext context) {
		Id id = Id.random();
		failUpdate = true;
		run(() -> store.putMessage(row(id)).compose(rid -> client.processDeliveryReceipt(receipt(id)))
				.compose(v -> Future.<Void>failedFuture("expected write failure"), e -> Future.succeededFuture())
				.compose(v -> awaitReceipt()).compose(v -> client.applyCachedDeliveryReceipt(id))
				.compose(v -> drainListeners()).compose(v -> store.getMessage(id)).map(saved -> {
					context.verify(() -> {
						assertEquals(1, updates);
						assertEquals(1, received.size());
						assertEquals(DeliveryStatus.UNDELIVERED, saved.deliveryStatus());
					});
					return (Void) null;
				})).onComplete(context.succeedingThenComplete());
	}

	@Test
	void earlyReceiptCacheIsBounded(VertxTestContext context) {
		Id first = Id.random();
		Id last = Id.random();
		run(() -> {
			Future<Void> chain = client.processDeliveryReceipt(receipt(first));
			for (int i = 0; i < 511; i++) {
				Id id = Id.random();
				chain = chain.compose(v -> client.processDeliveryReceipt(receipt(id)));
			}
			return chain.compose(v -> client.processDeliveryReceipt(receipt(last)))
					.compose(v -> store.putMessage(row(first)))
					.compose(rid -> client.applyCachedDeliveryReceipt(first))
					.compose(v -> store.putMessage(row(last)))
					.compose(rid -> client.applyCachedDeliveryReceipt(last)).compose(v -> awaitReceipt()).map(v -> {
						context.verify(() -> {
							assertEquals(1, received.size());
							assertEquals(last, received.get(0).getMessageId());
						});
						return (Void) null;
					});
		}).onComplete(context.succeedingThenComplete());
	}

	@Test
	void fullShutdownDiscardsEarlyReceipts(Vertx vertx, VertxTestContext context) {
		Id id = Id.random();
		run(() -> client.processDeliveryReceipt(receipt(id)).compose(v -> client.undeploy())
				.compose(v -> store.open(vertx)).compose(v -> store.putMessage(row(id)))
				.compose(rid -> client.applyCachedDeliveryReceipt(id)).map(v -> {
					context.verify(() -> { assertEquals(0, updates); assertTrue(received.isEmpty()); });
					return (Void) null;
				})).onComplete(context.succeedingThenComplete());
	}

	@Test
	@Timeout(45)
	void unmatchedReceiptExpires(VertxTestContext context) throws Exception {
		Id id = Id.random();
		run(() -> client.processDeliveryReceipt(receipt(id))).toCompletionStage().toCompletableFuture().get();
		Thread.sleep(31_000);
		run(() -> store.putMessage(row(id)).compose(rid -> client.applyCachedDeliveryReceipt(id)).map(v -> {
			context.verify(() -> { assertEquals(0, updates); assertTrue(received.isEmpty()); });
			return (Void) null;
		})).onComplete(context.succeedingThenComplete());
	}
}
