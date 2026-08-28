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

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.TrustOptions;
import io.vertx.mqtt.MqttClient;
import io.vertx.mqtt.MqttClientOptions;
import io.vertx.mqtt.MqttConnectionException;
import io.vertx.mqtt.messages.MqttPublishMessage;
import io.vertx.mqtt.messages.MqttSubAckMessage;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.bosonnetwork.CryptoContext;
import io.bosonnetwork.Id;
import io.bosonnetwork.Node;
import io.bosonnetwork.PeerInfo;
import io.bosonnetwork.crypto.CryptoException;
import io.bosonnetwork.crypto.CryptoIdentity;
import io.bosonnetwork.crypto.HybridTrustManager;
import io.bosonnetwork.crypto.Random;
import io.bosonnetwork.crypto.Signature;
import io.bosonnetwork.json.Json;
import io.bosonnetwork.photonmessaging.Channel;
import io.bosonnetwork.photonmessaging.ChannelListener;
import io.bosonnetwork.photonmessaging.Configuration;
import io.bosonnetwork.photonmessaging.ConnectionListener;
import io.bosonnetwork.photonmessaging.Contact;
import io.bosonnetwork.photonmessaging.ContactListener;
import io.bosonnetwork.photonmessaging.Conversation;
import io.bosonnetwork.photonmessaging.FriendRequest;
import io.bosonnetwork.photonmessaging.FriendRequestListener;
import io.bosonnetwork.photonmessaging.InviteTicket;
import io.bosonnetwork.photonmessaging.Message;
import io.bosonnetwork.photonmessaging.MessageListener;
import io.bosonnetwork.photonmessaging.MessagingClient;
import io.bosonnetwork.photonmessaging.MessagingStore;
import io.bosonnetwork.photonmessaging.SessionInfo;
import io.bosonnetwork.photonmessaging.SessionListener;
import io.bosonnetwork.photonmessaging.exceptions.AlreadyMemberException;
import io.bosonnetwork.photonmessaging.exceptions.ChannelNotExistsException;
import io.bosonnetwork.photonmessaging.exceptions.ConnectionException;
import io.bosonnetwork.photonmessaging.exceptions.ContactNotExistsException;
import io.bosonnetwork.photonmessaging.exceptions.InsufficientPermissionException;
import io.bosonnetwork.photonmessaging.exceptions.MessageTimeoutException;
import io.bosonnetwork.photonmessaging.exceptions.NotChannelMemberException;
import io.bosonnetwork.photonmessaging.exceptions.NotConnectedException;
import io.bosonnetwork.photonmessaging.exceptions.RevisionNotMonotonicException;
import io.bosonnetwork.photonmessaging.impl.database.SqliteDatabase;
import io.bosonnetwork.photonmessaging.impl.dto.ChannelInfo;
import io.bosonnetwork.photonmessaging.impl.dto.ChannelMembersRole;
import io.bosonnetwork.photonmessaging.impl.dto.ChannelSessionKeyRotation;
import io.bosonnetwork.photonmessaging.impl.dto.NewChannelInfo;
import io.bosonnetwork.photonmessaging.impl.rpc.RpcCall;
import io.bosonnetwork.photonmessaging.impl.rpc.RpcRequest;
import io.bosonnetwork.photonmessaging.impl.rpc.RpcResponse;
import io.bosonnetwork.utils.Base58;
import io.bosonnetwork.vertx.BosonVerticle;
import io.bosonnetwork.vertx.ContextualFuture;

@SuppressWarnings("CodeBlock2Expr")
public class PhotonMessagingClient extends BosonVerticle implements MessagingClient {
	private final Vertx vertx;
	private final @Nullable Node node;
	private final Configuration config;

	private final CryptoIdentity userIdentity;
	private final CryptoIdentity deviceIdentity;
	private final Id homePeerId;
	private final CryptoContext serviceContext;
	private final CryptoContext selfContext;

	private @Nullable URI serviceEndpoint;
	private @Nullable MqttClient mqttClient;
	private int failures;
	private final PhotonMessagingListeners listeners;
	private final MessagingRepository repository;

	private volatile int contactsRevision;
	private final AsyncCache<Id, PhotonContact> contactCache;
	private final AsyncCache<Id, PhotonConversation> conversationCache;
	// Thread-confinement invariant: these collections are accessed ONLY on this verticle's
	// event-loop context (from sendMessageInternal/sendRpcCall, which public methods reach
	// via runOnContext, and from the MQTT publish/response handlers which run on the same
	// context). They are therefore intentionally plain HashMaps/HashSets, not concurrent
	// collections. Do not touch them from any other thread without marshaling onto vertxContext first.
	private final Map<Integer, PhotonMessage<?>> inflightMessages;
	private final Map<Long, RpcCall<?>> inflightRpcCalls;

	// Packet ids whose broker acknowledgement arrived before the publish itself had been recorded in
	// [inflightMessages]. A message can only be registered once publish() hands back its packet id, so
	// an acknowledgement that overtakes that would otherwise find nothing to complete and the send would
	// hang forever. Parking the id here lets the registration see it and complete the message at once.
	private final Set<Integer> earlyAcknowledgements;

	// -1 means "no maintenance timer scheduled". Vert.x timer ids are non-negative and start at 0,
	// so 0 is a valid id and cannot be used as the unset sentinel.
	private long periodicTimer = -1;

	private volatile boolean connected;
	private volatile boolean ready;

	// -1 means "no readiness watchdog scheduled" (see periodicTimer note on the sentinel). Armed when
	// the MQTT connection is established (CONNACK) and cancelled once the mandatory startup contact-sync
	// has completed. The service re-pushes contact-sync on every healthy (re)connect, so a connection
	// that is established yet never delivers the sync is half-open/unhealthy; when this fires we force a
	// reconnect to re-establish a live channel that re-triggers the push. Confined to the event-loop
	// context (armed/cancelled only from the MQTT lifecycle callbacks), so a plain long is safe.
	private long readinessWatchdogTimer = -1;

	// Lifecycle gate and running status in one. The client is restartable: start() flips
	// false -> true and stop() flips true -> false via compareAndSet, so both are atomic and
	// idempotent (extra/concurrent calls are no-ops). A fresh start() after a completed stop() is
	// safe because the internal Vert.x is recreated in start(), the repository pool is recreated by
	// deploy()'s repository.initialize(), and deploy() rebuilds the caches and resets the connection
	// flags.
	private final AtomicBoolean running = new AtomicBoolean(false);

	private static final Logger log = LoggerFactory.getLogger(PhotonMessagingClient.class);

	// Cadence of the cache maintenance timer (proactive expiry/eviction). The caches are already
	// bounded by maximumSize and expire lazily on access, so this only releases entries that expire
	// and are never touched again; it does not need to be frequent (and a tight loop wastes wakeups,
	// notably on Android). The 5-minute access TTL makes a 1-minute sweep more than adequate.
	private static final long CACHE_MAINTENANCE_INTERVAL_MS = Duration.ofMinutes(1).toMillis();

	// How long to wait, after the MQTT connection is established (CONNACK), for the mandatory startup
	// contact-sync to arrive before treating the channel as unhealthy and forcing a reconnect. The
	// window spans the SUB/SUBACK round-trip plus the sync push; a healthy link completes it in well
	// under a second, so this only affects the pathological half-open case. Kept below the 30s
	// keep-alive so recovery beats the ping timeout.
	private static final long READINESS_WATCHDOG_TIMEOUT_MS = Duration.ofSeconds(20).toMillis();

	// Seconds to wait for a QoS-1 publish acknowledgement before failing the message. Applies to publish
	// acknowledgements only - CONNACK and SUBACK are not covered by it - so connection setup is
	// unaffected. Set well above any healthy round trip (a 256 KB payload on a poor link still lands
	// inside it): this is a backstop against an indefinite wait, not a latency budget.
	private static final int ACK_TIMEOUT = 60;

	public PhotonMessagingClient(@Nullable Vertx vertx, @Nullable Node node, Configuration config) {
		// Resolve the Vert.x instance: use the provided one, the current context, or the node's instance
		Vertx v = vertx;
		if (v == null) {
			Context currentContext = Vertx.currentContext();
			v = currentContext != null ? currentContext.owner() : null;
		}
		if (v == null)
			v = node != null ? node.unwrap(Vertx.class).orElse(null) : null;

		this.vertx = Objects.requireNonNull(v, "No Vertx instance available: provide a Vertx, or a Node that exposes one");
		this.config = Objects.requireNonNull(config, "config");
		this.node = config.getServiceEndpoint() == null ? Objects.requireNonNull(node, "node") : node;

		this.userIdentity = new CryptoIdentity(config.getUserKey());
		this.deviceIdentity = new CryptoIdentity(config.getDeviceKey());
		this.homePeerId = config.getServicePeerId();
		try {
			this.serviceContext = deviceIdentity.createCryptoContext(homePeerId);
			this.selfContext = userIdentity.createCryptoContext(userIdentity.getId());
		} catch (CryptoException e) {
			throw new IllegalStateException("Failed to create service encryption context", e);
		}

		this.failures = 0;

		this.inflightMessages = new HashMap<>();
		this.inflightRpcCalls = new HashMap<>();
		this.earlyAcknowledgements = new HashSet<>();

		// Prepare directories with plain NIO so the constructor does not depend on a Vert.x
		// instance (the internal Vertx, if any, does not exist until start()).
		try {
			Files.createDirectories(config.getDataDir());

			MessagingStore store = config.getStore();
			if (store == null) {
				String databaseUri = config.getDatabaseUri();
				// fix the sqlite database file location
				if (databaseUri.startsWith(SqliteDatabase.CONNECTION_URI_PREFIX)) {
					Path dbFile = Paths.get(databaseUri.substring(SqliteDatabase.CONNECTION_URI_PREFIX.length()));
					if (!dbFile.isAbsolute())
						databaseUri = SqliteDatabase.CONNECTION_URI_PREFIX + config.getDataDir().resolve(dbFile).toAbsolutePath();
					else
						Files.createDirectories(dbFile.getParent());
				}

				store = DatabaseStore.create(databaseUri, config.getDatabasePoolSize(), config.getDatabaseSchemaName());
			}

			this.repository = new MessagingRepository(store);
		} catch (IOException e) {
			throw new IllegalStateException("Failed to prepare the client data directory", e);
		}

		this.contactCache = AsyncCache.<Id, PhotonContact>newBuilder()
				.maximumSize(512)
				.expireAfterAccess(Duration.ofMinutes(5))
				.build(id -> repository.getContact(id).map(c -> (PhotonContact) c));

		this.conversationCache = AsyncCache.<Id, PhotonConversation>newBuilder()
				.maximumSize(512)
				.expireAfterAccess(Duration.ofMinutes(5))
				.build(id -> repository.getConversation(id).map(c -> (PhotonConversation) c));

		this.listeners = new PhotonMessagingListeners();
		this.connected = false;
		this.ready = false;
	}

	@Override
	public Id getUserId() {
		return userIdentity.getId();
	}

	@Override
	public Id getDeviceId() {
		return deviceIdentity.getId();
	}

	@Override
	public Id getServicePeerId() {
		return homePeerId;
	}

	@Override
	public Optional<String> getServiceEndpoint() {
		String endpoint = serviceEndpoint != null ? serviceEndpoint.toString()
				: (config.getServiceEndpoint() != null ? config.getServiceEndpoint().toString() : null);
		return Optional.ofNullable(endpoint);
	}

	@Override
	public java.nio.file.Path getDataDir() {
		return config.getDataDir();
	}

	@Override
	public void addConnectionListener(ConnectionListener listener) {
		listeners.addConnectionListener(listener);
	}

	@Override
	public void removeConnectionListener(ConnectionListener listener) {
		listeners.removeConnectionListener(listener);
	}

	@Override
	public void addMessageListener(MessageListener listener) {
		listeners.addMessageListener(listener);
	}

	@Override
	public void removeMessageListener(MessageListener listener) {
		listeners.removeMessageListener(listener);
	}

	@Override
	public void addChannelListener(ChannelListener listener) {
		listeners.addChannelListener(listener);
	}

	@Override
	public void removeChannelListener(ChannelListener listener) {
		listeners.removeChannelListener(listener);
	}

	@Override
	public void addContactListener(ContactListener listener) {
		listeners.addContactListener(listener);
	}

	@Override
	public void removeContactListener(ContactListener listener) {
		listeners.removeContactListener(listener);
	}

	@Override
	public void addSessionListener(SessionListener listener) {
		listeners.addSessionListener(listener);
	}

	@Override
	public void removeSessionListener(SessionListener listener) {
		listeners.removeSessionListener(listener);
	}

	@Override
	public void addFriendRequestListener(FriendRequestListener listener) {
		listeners.addFriendRequestListener(listener);
	}

	@Override
	public void removeFriendRequestListener(FriendRequestListener listener) {
		listeners.removeFriendRequestListener(listener);
	}

	@Override
	public void removeAllListeners() {
		listeners.removeAllListeners();
	}

	@Override
	public ContextualFuture<Void> start() {
		// Atomic + idempotent false -> true: only a stopped instance actually deploys; concurrent/
		// extra start() calls are no-ops. A start() after a completed stop() restarts the client.
		if (!running.compareAndSet(false, true))
			return ContextualFuture.succeededFuture();

		return ContextualFuture.of(vertx.deployVerticle(this).<Void>mapEmpty()
				.onFailure(e -> {
					// Roll back so the client can be started again; don't leak the internal Vertx.
					running.set(false);
				}));
	}

	@Override
	public ContextualFuture<Void> stop() {
		// Atomic + idempotent true -> false: only a running instance actually undeploys.
		if (!running.compareAndSet(true, false))
			return ContextualFuture.succeededFuture();

		Future<Void> future = vertx.undeploy(deploymentID())
				.andThen(ar -> selfContext.resetNonce());
		return ContextualFuture.of(future);
	}

	@Override
	public boolean isRunning() {
		return running.get();
	}

	@Override
	public boolean isConnected() {
		return connected;
	}

	@Override
	public boolean isReady() {
		return ready;
	}

	private void runningCheck() {
		if (!running.get())
			throw new IllegalStateException("Messaging client is not running");
	}

	@Override
	public Message.Builder message(@Nullable Id recipient) {
		runningCheck();
		return new MessageBuilder(this, recipient);
	}

	@Override
	public ContextualFuture<Optional<Conversation>> getConversation(Id conversationId) {
		Objects.requireNonNull(conversationId, "conversationId");
		runningCheck();
		return ContextualFuture.of(conversationCache.get(conversationId).map(Optional::ofNullable));
	}

	@Override
	public ContextualFuture<List<Conversation>> getConversations() {
		runningCheck();

		Future<List<Conversation>> future = repository.getAllConversations().map(convs -> {
			// keep the order, overlay the cached conversations
			Map<Id, Conversation> conversations = new LinkedHashMap<>();
			for (Conversation c : convs) {
				PhotonConversation cached = conversationCache.synchronous().asMap()
						.compute(c.getId(), (k, cc) -> cc != null ? cc : (PhotonConversation) c);
				conversations.put(c.getId(), Objects.requireNonNull(cached));
			}

			// Return a **mutable** list
			return new ArrayList<>(conversations.values());
		});

		return ContextualFuture.of(future);
	}

	@Override
	public ContextualFuture<Boolean> removeConversations(Collection<Id> conversationIds) {
		Objects.requireNonNull(conversationIds, "conversationIds");
		if (conversationIds.isEmpty())
			return ContextualFuture.succeededFuture(true);
		runningCheck();

		Future<Boolean> future = repository.removeConversations(conversationIds).onSuccess(removed ->
				conversationIds.forEach(id -> conversationCache.synchronous().invalidate(id))
		);
		return ContextualFuture.of(future);
	}

	@Override
	public ContextualFuture<List<Message>> getMessagesBefore(Id conversationId, long until, int limit, int offset) {
		Objects.requireNonNull(conversationId, "conversationId");
		if (limit <= 0 || offset < 0)
			throw new IllegalArgumentException("limit and offset must be positive");
		runningCheck();
		return ContextualFuture.of(repository.getMessagesBefore(conversationId, until, limit, offset).map(List::copyOf));
	}

	@Override
	public ContextualFuture<List<Message>> getMessagesInRange(Id conversationId, long begin, long end) {
		Objects.requireNonNull(conversationId, "conversationId");
		if (begin > end)
			throw new IllegalArgumentException("begin must be less than or equal to end");
		runningCheck();
		return ContextualFuture.of(repository.getMessagesInRange(conversationId, begin, end).map(List::copyOf));
	}

	@Override
	public ContextualFuture<Boolean> removeMessages(Collection<Long> messageIds) {
		Objects.requireNonNull(messageIds, "messageIds");
		if (messageIds.isEmpty())
			return ContextualFuture.succeededFuture(true);
		runningCheck();
		return ContextualFuture.of(repository.removeMessages(messageIds));
	}

	@Override
	public ContextualFuture<Boolean> removeMessages(Id conversationId) {
		Objects.requireNonNull(conversationId, "conversationId");
		runningCheck();
		return ContextualFuture.of(repository.removeMessagesByConversation(conversationId));
	}

	@Override
	public ContextualFuture<List<SessionInfo>> getSessions() {
		runningCheck();

		RpcCall<List<SessionInfo>> call = RpcCall.listSessions();
		Promise<List<SessionInfo>> promise = Promise.promise();
		runOnContext(v -> sendRpcCall(homePeerId, call).onComplete(promise));
		return ContextualFuture.of(promise.future());
	}

	@Override
	public ContextualFuture<Void> revokeSession(Id deviceId) {
		Objects.requireNonNull(deviceId, "deviceId");
		runningCheck();

		RpcCall<Void> call = RpcCall.revokeSession(deviceId);
		Promise<Void> promise = Promise.promise();
		runOnContext(v -> sendRpcCall(homePeerId, call).onComplete(promise));
		return ContextualFuture.of(promise.future());
	}

	@Override
	public ContextualFuture<Void> friendRequest(Id userId, String hello) {
		Objects.requireNonNull(userId, "id");
		Objects.requireNonNull(hello, "hello");
		runningCheck();

		// friend request is a notification message to the target user
		long now = System.currentTimeMillis();
		Handshake hs = Handshake.friendRequest(hello, now);
		PhotonFriendRequest request = new PhotonFriendRequest(userId, userIdentity.getId(), hello, now, now);

		Promise<Void> promise = Promise.promise();
		runOnContext(v -> {
			repository.putFriendRequest(request).compose(vv -> {
				Id messageId = DeviceOriginated.generateId(getDeviceId(), now);
				PhotonMessage<Handshake> message = new PhotonMessage<>(messageId,
						userId, Message.Type.HANDSHAKE_MESSAGE, now, hs);
				log.trace("Sending friend request to {}", userId);
				return sendMessageInternal(message).compose(packetId -> Future.<Void>succeededFuture(),
						error -> {
							log.error("Failed to send friend request to {}", userId, error);
							repository.removeFriendRequest(userId);
							return Future.failedFuture(error);
						});
			}).onComplete(promise);
		});
		return ContextualFuture.of(promise.future());
	}

	@Override
	public CompletableFuture<Void> acceptFriendRequest(Id userId) {
		Objects.requireNonNull(userId, "userId");
		runningCheck();

		Future<Void> future = repository.getFriendRequest(userId).compose(fr -> {
			if (fr == null)
				return Future.failedFuture(new IllegalArgumentException("No friend request found"));

			if (fr.getInitiatorId().equals(getUserId()))
				return Future.failedFuture(new IllegalStateException("Cannot accept your own friend request"));

			if (fr.isAccepted())
				return Future.failedFuture(new IllegalStateException("Friend request has already been accepted"));

			if (fr.isExpired())
				return Future.failedFuture(new IllegalStateException("Friend request has expired"));

			PhotonFriendRequest request = (PhotonFriendRequest) fr;
			Signature.KeyPair sessionKeypair = Signature.KeyPair.random();
			byte[] sessionKey = sessionKeypair.privateKey().bytes();
			long now = System.currentTimeMillis();
			Handshake hs = Handshake.friendRequestAccept(sessionKey, now);

			Promise<Void> promise = Promise.promise();
			runOnContext(v -> {
				Id messageId = DeviceOriginated.generateId(getDeviceId(), now);
				PhotonMessage<Handshake> message = new PhotonMessage<>(messageId,
						userId, Message.Type.HANDSHAKE_MESSAGE, now, hs);
				log.trace("Sending friend request accept to {}", userId);
				sendMessageInternal(message).compose(packetId -> {
					request.accept();
					log.trace("Friend request accept sent to {}", userId);
					return repository.putFriendRequest(request).compose(vv -> {
						log.trace("Friend request accepted, adding to contacts");
						byte[] sk = selfContext.encrypt(sessionKey);
						return addFriendInternal(userId, sk, null).<Void>mapEmpty();
					});
				}).onComplete(promise);
			});

			return promise.future();
		});

		return ContextualFuture.of(future);
	}

	@Override
	public ContextualFuture<Optional<FriendRequest>> getFriendRequest(Id userId) {
		Objects.requireNonNull(userId, "userId");
		runningCheck();
		return ContextualFuture.of(repository.getFriendRequest(userId).map(fr -> Optional.ofNullable(fr)));
	}

	@Override
	public ContextualFuture<List<FriendRequest>> getFriendRequests() {
		runningCheck();
		return ContextualFuture.of(repository.getFriendRequests());
	}

	@Override
	public ContextualFuture<Boolean> removeFriendRequest(Id userId) {
		Objects.requireNonNull(userId, "userId");
		runningCheck();
		return ContextualFuture.of(repository.removeFriendRequest(userId));
	}

	@Override
	public ContextualFuture<Boolean> removeFriendRequests(Collection<Id> userIds) {
		Objects.requireNonNull(userIds, "ids");
		runningCheck();

		if (userIds.isEmpty())
			return ContextualFuture.succeededFuture(true);

		return ContextualFuture.of(repository.removeFriendRequests(userIds));
	}

	@Override
	public ContextualFuture<Void> clearFriendRequests() {
		runningCheck();
		return ContextualFuture.of(repository.clearFriendRequests());
	}

	private Future<Contact> addFriendInternal(Id id, byte[] sessionKey, @Nullable String remark) {
		Friend friend = new Friend(id, sessionKey, remark);
		ContactMutation mutation = ContactMutation.add(contactsRevision, friend.toOpaque(selfContext));
		RpcCall<Integer> call = RpcCall.contactMutate(mutation);
		return sendRpcCall(homePeerId, call).compose(revision -> {
			Contact contact = friend.edit().setRevision(revision).build();
			return repository.putContact(revision, contact).map(vv -> {
				contactsRevision = revision;
				// A locally triggered add deliberately does NOT fire onContactAdded: that callback is
				// reserved for cross-device sync mutations, which raise it on the user's OTHER devices
				// (see applyContactMutation). The initiating device already has the contact - it is the
				// value this call resolves with - so notifying its own listeners here would be redundant.
				// The cache is still invalidated (as every other mutation path does) so subsequent reads
				// observe the newly added contact rather than a stale entry.
				contactCache.synchronous().invalidate(contact.getId());
				return contact;
			});
		});
	}

	@Override
	public ContextualFuture<Contact> addFriend(Id id, byte[] sessionKey, @Nullable String remark) {
		Objects.requireNonNull(id, "id");
		Objects.requireNonNull(sessionKey, "sessionKey");
		runningCheck();

		byte[] sk = selfContext.encrypt(sessionKey);
		Promise<Contact> promise = Promise.promise();
		runOnContext(v -> addFriendInternal(id, sk, remark).onComplete(promise));
		return ContextualFuture.of(promise.future());
	}

	@Override
	public ContextualFuture<Channel> createChannel(Channel.Permission permission, String name, @Nullable String notice, boolean announce) {
		Objects.requireNonNull(permission, "permission");
		Objects.requireNonNull(name, "name");
		runningCheck();

		Signature.KeyPair sessionKeypair = Signature.KeyPair.random();
		Id sessionId = Id.of(sessionKeypair.publicKey().bytes());
		byte[] sessionKey = selfContext.encrypt(sessionKeypair.privateKey().bytes());
		NewChannelInfo params = new NewChannelInfo(sessionId, sessionKey,
				permission, name, notice, announce);

		Promise<Channel> promise = Promise.promise();
		runOnContext(v -> {
			// 1. Request to create a channel
			RpcCall<ChannelInfo> createChannelCall = RpcCall.createChannel(params);
			sendRpcCall(homePeerId, createChannelCall).compose(ci -> {
				// 2. Save the channel locally
				PhotonChannel channel = new PhotonChannel(ci.channelId(), sessionKey, ci.ownerId(), ci.permission(),
						ci.name(), ci.notice(), ci.announce(), ci.createdAt(), ci.updateAt());
				return repository.putContactLocally(channel).compose(vv -> {
					if (!ci.members().isEmpty())
						return repository.putChannelMembers(channel.getId(), ci.members()).map(channel);
					else
						return Future.succeededFuture(channel);
				});
			}).compose(ch -> {
				// 3. Add the channel as a new contact (sync cross all devices)
				ContactMutation mutation = ContactMutation.add(contactsRevision, ch.toOpaque(selfContext));
				RpcCall<Integer> addContactCall = RpcCall.contactMutate(mutation);
				return sendRpcCall(homePeerId, addContactCall).compose(revision -> {
					PhotonChannel channel = (PhotonChannel) ch.edit().setRevision(revision).build();
					return repository.putContacts(revision, List.of(channel)).map(vv -> {
						contactsRevision = revision;
						return channel;
					});
				});
			}).onComplete(promise);
		});
		return ContextualFuture.of(promise.future());
	}

	@Override
	public ContextualFuture<Boolean> removeChannel(Id channelId) {
		Objects.requireNonNull(channelId, "channelId");
		runningCheck();

		Promise<Boolean> promise = Promise.promise();
		runOnContext(v -> {
			channel(channelId).compose(channel -> {
				if (channel == null)
					return Future.failedFuture(new ChannelNotExistsException(channelId.toString()));

				if (!channel.getOwnerId().equals(getUserId()))
					return Future.failedFuture(new InsufficientPermissionException("Only the owner can delete the channel"));

				// 1. Request to delete the channel
				RpcCall<Void> deleteChannelCall = RpcCall.deleteChannel();
				return sendRpcCall(channelId, deleteChannelCall).compose(vv -> {
					// 2. Remove the channel from the contact list (sync cross all devices)
					ContactMutation mutation = ContactMutation.remove(contactsRevision, List.of(channelId));
					RpcCall<Integer> removeContactCall = RpcCall.contactMutate(mutation);
					return sendRpcCall(homePeerId, removeContactCall).compose(revision ->
							repository.removeContacts(revision, List.of(channelId)).map(ignored -> {
								contactsRevision = revision;
								contactCache.synchronous().invalidate(channelId);
								return true;
							})
					);
				});
			}).onComplete(promise);
		});
		return ContextualFuture.of(promise.future());
	}

	@Override
	public ContextualFuture<Channel> joinChannel(InviteTicket ticket) {
		Objects.requireNonNull(ticket, "ticket");
		if (!ticket.isGenuine())
			throw new IllegalArgumentException("Invalid ticket");
		if (ticket.isExpired())
			throw new IllegalArgumentException("Ticket has expired");
		if (ticket.isNamedTicket() && !ticket.getInvitee().map(invitee -> invitee.equals(getUserId())).orElse(false))
			throw new IllegalArgumentException("Only the invitee can join the channel");

		runningCheck();

		// 1. check the session key is valid and re-encrypt with self-encryption context
		final InviteTicket revisedTicket;
		try {
			byte[] sk = ticket.isNamedTicket() ?
					userIdentity.decrypt(ticket.getInviter(), ticket.getSessionKey()) :
					ticket.getSessionKey();
			Signature.KeyPair sessionKeypair = Signature.KeyPair.fromPrivateKey(sk);
			Id sessionId = Id.of(sessionKeypair.publicKey().bytes());
			if (!ticket.getSessionId().equals(sessionId))
				return ContextualFuture.failedFuture("Invalid ticket: session key not valid");

			byte[] sessionKey = selfContext.encrypt(sk);
			revisedTicket = ticket.revise(sessionKey);
		} catch (CryptoException e) {
			return ContextualFuture.failedFuture("Invalid session key");
		}

		Promise<Channel> promise = Promise.promise();
		runOnContext(v -> {
			// 2. check if joined the channel already
			channel(ticket.getChannelId()).compose(existing -> {
				if (existing == null || !existing.hasSessionKey())
					return Future.succeededFuture(null);

				return existing.tryLoadMembers().compose(vv -> {
					if (existing.hasMember(getUserId()))
						return Future.failedFuture(new AlreadyMemberException("Already joined the channel"));

					return Future.succeededFuture(null);
				});
			}).compose(existing -> {
				// 3. request to join the channel
				RpcCall<ChannelInfo> joinChannelCall = RpcCall.joinChannel(revisedTicket);
				return sendRpcCall(ticket.getChannelId(), joinChannelCall).compose(ci -> {
					Objects.requireNonNull(ci.sessionKey(), "Invalid channel info: session key is missing");

					PhotonChannel channel = new PhotonChannel(ci.channelId(), ci.sessionKey(), ci.ownerId(), ci.permission(),
							ci.name(), ci.notice(), ci.announce(), ci.createdAt(), ci.updateAt());
					return repository.putContactLocally(channel).compose(vv -> {
						if (!ci.members().isEmpty())
							return repository.putChannelMembers(channel.getId(), ci.members()).map(channel);
						else
							return Future.succeededFuture(channel);
					});
				}).compose(ch -> {
					// 4. add the channel as a new contact (sync cross all devices)
					ContactMutation mutation = ContactMutation.add(contactsRevision, ch.toOpaque(selfContext));
					RpcCall<Integer> addContactCall = RpcCall.contactMutate(mutation);
					return sendRpcCall(homePeerId, addContactCall).compose(revision -> {
						PhotonChannel channel = (PhotonChannel) ch.edit().setRevision(revision).build();
						return repository.putContacts(revision, List.of(channel)).map(vv -> {
							contactsRevision = revision;
							contactCache.synchronous().invalidate(channel.getId());
							return channel;
						});
					});
				});
			}).onComplete(promise);
		});
		return ContextualFuture.of(promise.future());
	}

	@Override
	public ContextualFuture<Boolean> leaveChannel(Id channelId) {
		Objects.requireNonNull(channelId, "channelId");
		runningCheck();

		Promise<Boolean> promise = Promise.promise();
		runOnContext(v -> {
			channel(channelId).compose(channel -> {
				if (channel == null)
					return Future.failedFuture(new ChannelNotExistsException(channelId.toString()));

				if (channel.getOwnerId().equals(getUserId()))
					return Future.failedFuture(new InsufficientPermissionException("owner can not leave the channel"));

				RpcCall<Void> leaveChannelCall = RpcCall.leaveChannel();
				return sendRpcCall(channelId, leaveChannelCall).compose(vv -> {
					ContactMutation mutation = ContactMutation.remove(contactsRevision, List.of(channelId));
					RpcCall<Integer> removeContactCall = RpcCall.contactMutate(mutation);
					return sendRpcCall(homePeerId, removeContactCall).compose(revision ->
							repository.removeContacts(revision, List.of(channelId)).map(ignored -> {
								contactsRevision = revision;
								contactCache.synchronous().invalidate(channelId);
								return true;
							})
					);
				});
			}).onComplete(promise);
		});
		return ContextualFuture.of(promise.future());
	}

	@Override
	public ContextualFuture<InviteTicket> createInviteTicket(Id channelId, @Nullable Id invitee) {
		Objects.requireNonNull(channelId, "channelId");
		runningCheck();

		Promise<InviteTicket> promise = Promise.promise();
		runOnContext(v -> {
			channel(channelId).compose(channel -> {
				if (channel == null)
					return Future.failedFuture(new ChannelNotExistsException(channelId.toString()));

				return channel.tryLoadMembers().compose(vv -> {
					Optional<Channel.Member> om = channel.getMember(getUserId());
					if (om.isEmpty())
						return Future.failedFuture(new InsufficientPermissionException("Not a member of the channel"));

					Channel.Member member = om.get();
					Channel.Permission permission = channel.getPermission();
					switch (permission) {
						case PUBLIC, MEMBER_INVITE -> {
							if (member.isBanned())
								return Future.failedFuture(new InsufficientPermissionException("You are banned from the channel"));
						}

						case MODERATOR_INVITE -> {
							if (!member.isModerator() && !member.isOwner())
								return Future.failedFuture(new InsufficientPermissionException("You are not a moderator of the channel"));
						}

						case OWNER_INVITE -> {
							if (!member.isOwner())
								return Future.failedFuture(new InsufficientPermissionException("You are not the owner of the channel"));
						}
					}

					if (invitee != null && channel.hasMember(invitee))
						return Future.failedFuture(new AlreadyMemberException("The invitee is already a member of the channel"));

					try {
						// TODO: why nonce is duplicated sometimes?
						selfContext.resetNonce();
						byte[] sk = selfContext.decrypt(channel.getSessionKey());
						Signature.KeyPair sessionKeypair = Signature.KeyPair.fromPrivateKey(sk);
						Id sessionId = Id.of(sessionKeypair.publicKey().bytes());

						if (invitee != null)
							sk = userIdentity.encrypt(invitee, sk);

						InviteTicket ticket = InviteTicket.create(userIdentity, channelId, sessionId, invitee,
								System.currentTimeMillis() + InviteTicket.DEFAULT_EXPIRATION, sk);
						return Future.succeededFuture(ticket);
					} catch (CryptoException e) {
						return Future.failedFuture("Invalid process session key: " + e.getMessage());
					}
				});
			}).onComplete(promise);
		});
		return ContextualFuture.of(promise.future());
	}

	@Override
	public ContextualFuture<Void> transferChannelOwnership(Id channelId, Id newOwner) {
		Objects.requireNonNull(channelId, "channelId");
		Objects.requireNonNull(newOwner, "newOwner");
		runningCheck();

		Promise<Void> promise = Promise.promise();
		runOnContext(v -> {
			channel(channelId).compose(channel -> {
				if (channel == null)
					return Future.failedFuture(new ChannelNotExistsException(channelId.toString()));

				if (!channel.getOwnerId().equals(getUserId()))
					return Future.failedFuture(new InsufficientPermissionException("not owner"));

				return channel.tryLoadMembers().compose(vv -> {
					Optional<Channel.Member> om = channel.getMember(newOwner);
					if (om.isEmpty())
						return Future.failedFuture(new NotChannelMemberException("newOwner is not the member of channel"));
					if (om.get().isBanned())
						return Future.failedFuture(new NotChannelMemberException("newOwner is banned from the channel"));

					RpcCall<Void> call = RpcCall.transferChannelOwnership(newOwner);
					return sendRpcCall(channelId, call).compose(vvv -> {
						Contact updatedChannel = channel.editChannel().setOwnerId(newOwner).build();
						return repository.putContactLocally(updatedChannel).andThen(ar -> {
							contactCache.synchronous().invalidate(channelId);
						});
					});
				});
			}).onComplete(promise);
		});
		return ContextualFuture.of(promise.future());
	}

	@Override
	public ContextualFuture<Void> rotateChannelSessionKey(Id channelId, Signature.@Nullable KeyPair sessionKeypair) {
		Objects.requireNonNull(channelId, "channelId");
		runningCheck();

		final Signature.KeyPair keypair = sessionKeypair != null ? sessionKeypair : Signature.KeyPair.random();

		Promise<Void> promise = Promise.promise();
		runOnContext(v -> {
			getOrCreateConversation(channelId).compose(conv -> {
				if (!conv.isChannel())
					return Future.failedFuture(new ChannelNotExistsException(channelId.toString()));

				PhotonChannel channel = (PhotonChannel) conv.getContact();
				if (!channel.getOwnerId().equals(getUserId()))
					return Future.failedFuture(new InsufficientPermissionException("not owner"));

				final byte[] sessionKey;
				try {
					SessionContext sc = conv.getSessionContext();
					sessionKey = sc.getTxCryptoContext().encrypt(keypair.privateKey().bytes());
				} catch (CryptoException e) {
					return Future.failedFuture(e);
				}

				Id sessionId = Id.of(keypair.publicKey().bytes());
				ChannelSessionKeyRotation params = new ChannelSessionKeyRotation(sessionId, sessionKey);
				RpcCall<Void> call = RpcCall.rotateChannelSessionKey(params);
				return sendRpcCall(channelId, call).compose(vv -> {
					Contact updatedChannel = channel.edit().setSessionKey(sessionKey).build();
					return repository.putContactLocally(updatedChannel);
				});
			}).onComplete(promise);
		});
		return ContextualFuture.of(promise.future());
	}

	@Override
	public ContextualFuture<Void> updateChannelInfo(Channel channel) {
		Objects.requireNonNull(channel, "channel");
		runningCheck();

		Promise<Void> promise = Promise.promise();
		runOnContext(v -> {
			channel(channel.getId()).compose(origin -> {
				if (origin == null)
					return Future.failedFuture(new ChannelNotExistsException(channel.getId().toString()));

				if (channel == origin)
					return Future.failedFuture(new IllegalStateException("Channel info not changed"));

				if (!origin.getOwnerId().equals(getUserId()))
					return Future.failedFuture(new InsufficientPermissionException("not owner"));

				ObjectNode changes = Json.cborMapper().createObjectNode();
				if (channel.getPermission() != origin.getPermission())
					changes.put("p", channel.getPermission().value());
				if (!Objects.equals(channel.getName(), origin.getName()))
					changes.put("n", channel.getName().orElse(null));
				if (!Objects.equals(channel.getNotice(), origin.getNotice()))
					changes.put("nt", channel.getNotice().orElse(null));
				if (channel.isAnnounce() != origin.isAnnounce())
					changes.put("a", channel.isAnnounce());
				if (channel.getUpdatedAt() != origin.getUpdatedAt())
					changes.put("u", channel.getUpdatedAt());

				RpcCall<Void> call = RpcCall.updateChannelInfo(changes);
				return sendRpcCall(channel.getId(), call).compose(vv -> {
					contactCache.synchronous().invalidate(channel.getId());
					return repository.putContactLocally(channel);
				});
			}).onComplete(promise);
		});
		return ContextualFuture.of(promise.future());
	}

	@Override
	public ContextualFuture<Void> setChannelMembersRole(Id channelId, Collection<Id> members, Channel.Role role) {
		Objects.requireNonNull(channelId, "channelId");
		Objects.requireNonNull(members, "members");
		Objects.requireNonNull(role, "role");
		if (members.isEmpty())
			throw new IllegalArgumentException("No members specified");
		if (role == Channel.Role.OWNER || role == Channel.Role.BANNED)
			throw new IllegalArgumentException("Role " + role + " is not allowed");
		runningCheck();

		Promise<Void> promise = Promise.promise();
		runOnContext(v -> {
			channel(channelId).compose(channel -> {
				if (channel == null)
					return Future.failedFuture(new ChannelNotExistsException(channelId.toString()));

				return channel.tryLoadMembers().compose(na -> {
					Optional<Channel.Member> om = channel.getMember(getUserId());
					if (om.isEmpty())
						return Future.failedFuture(new InsufficientPermissionException("Not a member of the channel"));
					Channel.Member member = om.get();
					if (!member.isOwner() && !member.isModerator())
						return Future.failedFuture(new InsufficientPermissionException("Not an owner or a moderator of the channel"));

					ChannelMembersRole params = new ChannelMembersRole(List.copyOf(members), role);
					RpcCall<List<Id>> call = RpcCall.updateChannelMembersRole(params);
					return sendRpcCall(channelId, call).compose(ids -> {
						channel.invalidateMembers();
						return repository.updateChannelMembersRole(channelId, ids, role).<Void>mapEmpty();
					});
				});
			}).onComplete(promise);
		});
		return ContextualFuture.of(promise.future());
	}

	@Override
	public ContextualFuture<Void> banChannelMembers(Id channelId, Collection<Id> members) {
		Objects.requireNonNull(channelId, "channelId");
		Objects.requireNonNull(members, "members");
		if (members.isEmpty())
			throw new IllegalArgumentException("No members specified");
		runningCheck();

		Promise<Void> promise = Promise.promise();
		runOnContext(v -> {
			channel(channelId).compose(channel -> {
				if (channel == null)
					return Future.failedFuture(new ChannelNotExistsException(channelId.toString()));

				return channel.tryLoadMembers().compose(na -> {
					Optional<Channel.Member> om = channel.getMember(getUserId());
					if (om.isEmpty())
						return Future.failedFuture(new InsufficientPermissionException("Not a member of the channel"));
					Channel.Member member = om.get();
					if (!member.isOwner() && !member.isModerator())
						return Future.failedFuture(new InsufficientPermissionException("Not an owner or a moderator of the channel"));

					RpcCall<List<Id>> call = RpcCall.banChannelMembers(members);
					return sendRpcCall(channelId, call).compose(banned -> {
						if (!banned.isEmpty()) {
							channel.invalidateMembers();
							return repository.updateChannelMembersRole(channelId, banned, Channel.Role.BANNED).<Void>mapEmpty();
						} else {
							return Future.succeededFuture();
						}
					});
				});
			}).onComplete(promise);
		});
		return ContextualFuture.of(promise.future());
	}

	@Override
	public ContextualFuture<Void> unbanChannelMembers(Id channelId, Collection<Id> members) {
		Objects.requireNonNull(channelId, "channelId");
		Objects.requireNonNull(members, "members");
		if (members.isEmpty())
			throw new IllegalArgumentException("No members specified");
		runningCheck();

		Promise<Void> promise = Promise.promise();
		runOnContext(v -> {
			channel(channelId).compose(channel -> {
				if (channel == null)
					return Future.failedFuture(new ChannelNotExistsException(channelId.toString()));

				return channel.tryLoadMembers().compose(na -> {
					Optional<Channel.Member> om = channel.getMember(getUserId());
					if (om.isEmpty())
						return Future.failedFuture(new InsufficientPermissionException("Not a member of the channel"));
					Channel.Member member = om.get();
					if (!member.isOwner() && !member.isModerator())
						return Future.failedFuture(new InsufficientPermissionException("Not an owner or a moderator of the channel"));

					RpcCall<List<Id>> call = RpcCall.unbanChannelMembers(members);
					return sendRpcCall(channelId, call).compose(unbanned -> {
						if (!unbanned.isEmpty()) {
							channel.invalidateMembers();
							return repository.updateChannelMembersRole(channelId, unbanned, Channel.Role.MEMBER).<Void>mapEmpty();
						} else {
							return Future.succeededFuture();
						}
					});
				});
			}).onComplete(promise);
		});
		return ContextualFuture.of(promise.future());
	}

	@Override
	public ContextualFuture<Void> removeChannelMembers(Id channelId, Collection<Id> members) {
		Objects.requireNonNull(channelId, "channelId");
		Objects.requireNonNull(members, "members");
		if (members.isEmpty())
			throw new IllegalArgumentException("No members specified");
		runningCheck();

		Promise<Void> promise = Promise.promise();
		runOnContext(v -> {
			channel(channelId).compose(channel -> {
				if (channel == null)
					return Future.failedFuture(new ChannelNotExistsException(channelId.toString()));

				return channel.tryLoadMembers().compose(na -> {
					Optional<Channel.Member> om = channel.getMember(getUserId());
					if (om.isEmpty())
						return Future.failedFuture(new InsufficientPermissionException("Not a member of the channel"));
					Channel.Member member = om.get();
					if (!member.isOwner() && !member.isModerator())
						return Future.failedFuture(new InsufficientPermissionException("Not an owner or a moderator of the channel"));

					RpcCall<List<Id>> call = RpcCall.removeChannelMembers(members);
					return sendRpcCall(channelId, call).compose(removed -> {
						if (!removed.isEmpty()) {
							channel.invalidateMembers();
							return repository.removeChannelMembers(channelId, removed).<Void>mapEmpty();
						} else {
							return Future.succeededFuture();
						}
					});
				});
			}).onComplete(promise);
		});
		return ContextualFuture.of(promise.future());
	}

	@Override
	public ContextualFuture<Optional<Contact>> getContact(Id contactId) {
		Objects.requireNonNull(contactId, "contactId");
		runningCheck();
		return ContextualFuture.of(contactCache.get(contactId).compose(c -> {
			if (c instanceof PhotonChannel ch)
				// TODO: should we call refreshChannel() here
				return Future.fromCompletionStage(ch.loadMembers()).map(v -> Optional.of(ch));

			return Future.succeededFuture(Optional.ofNullable(c));
		}));
	}

	@Override
	public ContextualFuture<List<Contact>> getContacts() {
		runningCheck();

		Future<List<Contact>> future = repository.getAllContacts().map(contacts -> {
			Map<Id, Contact> result = new LinkedHashMap<>();
			for (Contact c : contacts) {
				PhotonContact cached = contactCache.synchronous().asMap()
						.compute(c.getId(), (k, cc) -> cc != null ? cc : (PhotonContact) c);
				result.put(c.getId(), Objects.requireNonNull(cached));
			}

			// Return a **mutable** list
			return new ArrayList<>(result.values());
		});
		return ContextualFuture.of(future);
	}

	@Override
	public ContextualFuture<Contact> updateContact(Contact contact) {
		Objects.requireNonNull(contact, "contact");
		runningCheck();

		Promise<Contact> promise = Promise.promise();
		runOnContext(v -> {
			contact(contact.getId()).compose(origin -> {
				if (origin == null)
					return Future.failedFuture(new ContactNotExistsException(contact.getId().toString()));

				if (contact == origin)
					return Future.failedFuture(new IllegalStateException("Contact not changed"));

				if (contact.getRevision() < origin.getRevision())
					return Future.failedFuture(new RevisionNotMonotonicException("Contact revision is outdate"));

				final PhotonContact updated;
				if (contact instanceof PhotonContact pc) {
					updated = pc;
				} else {
					updated = origin.edit()
							.setRemark(contact.getRemark().orElse(null))
							.setTags(contact.getTags().orElse(null))
							.setMuted(contact.isMuted())
							.setBlocked(contact.isBlocked())
							.build();
				}

				ContactMutation mutation = ContactMutation.update(contactsRevision, updated.toOpaque(selfContext));
				RpcCall<Integer> call = RpcCall.contactMutate(mutation);
				return sendRpcCall(homePeerId, call).compose(revision -> {
					Contact updatedContact = ((ContactEditor) updated.edit()).setRevision(revision).build();
					return repository.putContacts(revision, List.of(updatedContact)).map(vv -> {
						contactsRevision = revision;
						contactCache.synchronous().invalidate(updated.getId());
						return updatedContact;
					});
				});
			}).onComplete(promise);
		});
		return ContextualFuture.of(promise.future());
	}

	@Override
	public ContextualFuture<Boolean> removeContacts(Collection<Id> contactIds) {
		Objects.requireNonNull(contactIds, "contactIds");
		if (contactIds.isEmpty())
			throw new IllegalArgumentException("No contacts specified");
		runningCheck();

		List<Id> ids = List.copyOf(contactIds);
		Promise<Boolean> promise = Promise.promise();
		runOnContext(v -> {
			removeContactsInternal(ids).map(true).onComplete(promise);
		});
		return ContextualFuture.of(promise.future());
	}

	private Future<Void> removeContactsInternal(List<Id> contactIds) {
		ContactMutation mutation = ContactMutation.remove(contactsRevision, contactIds);
		RpcCall<Integer> call = RpcCall.contactMutate(mutation);
		return (Future<Void>) sendRpcCall(homePeerId, call).compose(revision ->
				repository.removeContacts(revision, contactIds).<@Nullable Void>map(ignored -> {
					contactsRevision = revision;
					return null;
				}));
	}

	@Override
	public ContextualFuture<Void> clearContacts() {
		runningCheck();

		Promise<Void> promise = Promise.promise();
		runOnContext(v -> {
			ContactMutation mutation = ContactMutation.clear(contactsRevision);
			RpcCall<Integer> call = RpcCall.contactMutate(mutation);
			sendRpcCall(homePeerId, call).compose(revision ->
					repository.clearContacts(revision).<@Nullable Void>map(vv -> {
						contactsRevision = revision;
						return null;
					})
			).onComplete((Promise<@Nullable Void>) promise);
		});
		return ContextualFuture.of(promise.future());
	}

	private SessionContext sessionContextFactory(PhotonContact contact) {
		byte[] encryptedSessionKey = contact.getSessionKey();
		if (encryptedSessionKey == null)
			throw new IllegalStateException("INTERNAL ERROR: Contact has no session key");

		try {
			// TODO: check me!!! continuous create the SessionContext twice?
			selfContext.resetNonce();
			byte[] sessionKey = selfContext.decrypt(encryptedSessionKey);
			CryptoIdentity sessionIdentity = new CryptoIdentity(sessionKey);

			if (contact instanceof PhotonChannel)
				return SessionContext.forChannel(contact.getId(), userIdentity, sessionIdentity);
			else
				return SessionContext.forUser(contact.getId(), userIdentity, sessionIdentity);
		} catch (CryptoException e) {
			throw new IllegalStateException("INTERNAL ERROR: Failed to decrypt session key", e);
		}
	}

	@Override
	protected Future<Void> deploy() {
		this.connected = false;
		this.ready = false;

		return repository.initialize(vertx, this::sessionContextFactory)
				.compose(v -> connect())
				.onSuccess(v -> { periodicTimer = vertx.setPeriodic(CACHE_MAINTENANCE_INTERVAL_MS, this::periodicCheck); });
	}

	@Override
	protected Future<Void> undeploy() {
		if (periodicTimer >= 0) {
			vertx.cancelTimer(periodicTimer);
			periodicTimer = -1;
		}

		return disconnect().compose(v -> repository.close());
	}

	private void periodicCheck(long timer) {
		contactCache.cleanUp();
		conversationCache.cleanUp();
	}

	private Future<PhotonConversation> getOrCreateConversation(Id id) {
		return conversationCache.get(id).compose(conv -> {
			if (conv != null)
				return Future.<PhotonConversation>succeededFuture(conv);
			else
				return contact(id).compose(contact -> {
					if (contact == null)
						return Future.failedFuture(new ContactNotExistsException(id.toString()));

					// Prevents the race condition that could create duplicate Conversation instances for the same Contact.
					// noinspection SynchronizationOnLocalVariableOrMethodParameter
					synchronized (contact) { // contact object is **NOT** a local object
						PhotonConversation cached = conversationCache.synchronous().getIfPresent(id);
						if (cached != null)
							return Future.succeededFuture(cached);

						PhotonConversation c = new PhotonConversation(contact, this::sessionContextFactory);
						conversationCache.synchronous().asMap().compute(id, (k, v) -> c);
						return Future.succeededFuture(c);
					}
				});
		});
	}

	private Future<@Nullable PhotonContact> contact(Id id) {
		return contactCache.get(id).<@Nullable PhotonContact>map(contact -> contact);
	}

	private Future<@Nullable PhotonChannel> channel(Id id) {
		return contactCache.get(id)
				.<@Nullable PhotonChannel>map(contact -> contact instanceof PhotonChannel channel ? channel : null);
	}

	private Future<Void> refreshChannel(PhotonChannel channel) {
		final Id channelId = channel.getId();

		RpcCall<ChannelInfo> call = RpcCall.getChannelInfo();
		return sendRpcCall(channelId, call).compose(ci -> {
			PhotonChannel updatedChannel = new PhotonChannel(channelId, channel.getSessionKey(), ci.ownerId(), ci.permission(),
					ci.name(), ci.notice(), ci.announce(), channel.getRemark().orElse(null),
					channel.getTags().orElse(null), channel.isMuted(),
					channel.isBlocked(), ci.createdAt(), ci.updateAt(), channel.getRevision());
			return repository.putContactLocally(updatedChannel).compose(vv ->
					repository.refillChannelMembers(channelId, ci.members()).andThen(ar ->
							contactCache.synchronous().invalidate(channelId)
					)
			);
		});
	}

	private <R> Future<R> sendRpcCall(Id recipient, RpcCall<R> call) {
		inflightRpcCalls.put(call.getId(), call);
		long now = System.currentTimeMillis();
		Id messageId = DeviceOriginated.generateId(getDeviceId(), now);
		PhotonMessage<RpcRequest> message = new PhotonMessage<>(messageId, recipient, Message.Type.CONTROL_MESSAGE, now, call.getRequest());
		log.debug("Sending RPC call {}:{} to {} ...", call.getId(), call.getMethod(), recipient);
		return sendMessageInternal(message)
				// Stage 1: the RPC request could not even be sent - drop the pending call now,
				// otherwise it would leak in inflightRpcCalls, then propagate the failure.
				.recover(error -> {
					inflightRpcCalls.remove(call.getId());
					log.error("Send RPC call {}:{} failed", call.getId(), call.getMethod(), error);
					return Future.failedFuture(error);
				})
				// Stage 2: request sent successfully - await the response future. If the call
				// later fails (timeout or RPC error), drop the pending call. The success path
				// removes it when the matching response arrives (see onResponse).
				.compose(packetId -> (Future<R>) call.getFuture()
						.onFailure(e -> inflightRpcCalls.remove(call.getId())));
	}

	protected Future<Message> sendMessage(Message message) {
		@SuppressWarnings("unchecked")
		PhotonMessage<MessageContent> msg = (PhotonMessage<MessageContent>) message;
		msg.setConversationId(msg.getRecipient());
		msg.setFrom(getUserId());

		Promise<Message> promise = Promise.promise();
		Context vertxContext = Objects.requireNonNull(this.vertxContext, "vertxContext");
		vertxContext.runOnContext(v ->
				repository.putMessage(msg).compose(vv -> sendMessageInternal(msg))
						.compose(packetId -> msg.getFuture())
						.compose(vv -> repository.updateMessageSentTime(msg))
						.compose(vv -> getOrCreateConversation(msg.getRecipient()).map(conv -> {
							conv.update(msg);
							return msg;
						})).onComplete(promise)
		);

		return promise.future();
	}

	private Future<Integer> sendMessageInternal(PhotonMessage<?> message) {
		// the input message is immutable, so we should not modify it

		// encrypt message payload for the recipient
		return encryptMessage(message).compose(encrypted -> {
			// Clear the sender ID; the server will inject the authoritative ID
			// to ensure message integrity and prevent spoofing.
			encrypted.setFrom(null);

			// encrypt the whole message for transmission
			byte[] mqttPayload = serviceContext.encrypt(encrypted.serialize());
			Buffer buffer = Buffer.buffer(mqttPayload);
			message.prepareForSending();
			// The transport can drop between the caller's send() and here (a reconnect is in
			// progress). Publishing on a disconnected MqttClient dereferences a null socket and
			// throws a low-level NullPointerException; fail with a typed, retryable error instead so
			// callers can surface a clean "not connected" message and retry once reconnected.
			MqttClient mqttClient = this.mqttClient;
			if (mqttClient == null || !mqttClient.isConnected())
				return Future.<Integer>failedFuture(new NotConnectedException("Not connected to the messaging service"));
			// Workaround for a Vert.x MQTT design issue: when the internal connection is unavailable,
			// MqttClient.publish() should return a failed future, but instead it dereferences a null
			// NetSocketInternal and throws a NullPointerException. isConnected() can also still report
			// true while that socket is being torn down, so the NPE can surface either synchronously
			// (before publish() returns) or asynchronously (the returned future fails). Translate both
			// into a clean, retryable NotConnectedException rather than leaking a raw NPE to the UI.
			// Remove once the upstream Vert.x behaviour is fixed to return a failed future.
			Future<Integer> published;
			try {
				published = mqttClient.publish(Topic.DEVICE_OUTBOX.toString(), buffer, MqttQoS.AT_LEAST_ONCE, false, false);
			} catch (Exception e) {
				return Future.failedFuture(new NotConnectedException("Not connected to the messaging service"));
			}
			return published.andThen(ar -> {
				if (ar.succeeded()) {
					int packetId = ar.result();
					log.debug("Sending message {} to {}, packetId {} ...", message.getId(), message.getRecipient(), packetId);
					// The broker's acknowledgement can in principle overtake this registration; if it
					// already did, complete the message here instead of leaving it pending forever.
					if (earlyAcknowledgements.remove(packetId))
						message.sent();
					else
						inflightMessages.put(packetId, message);
				} else {
					log.error("Send message {} to {} failed", message.getId(), message.getRecipient(), ar.cause());
				}
			}).recover(cause -> cause instanceof NullPointerException
					? Future.failedFuture(new NotConnectedException("Not connected to the messaging service"))
					: Future.failedFuture(cause));
		});
	}

	private Future<BytesMessage> encryptMessage(PhotonMessage<?> message) {
		final byte[] payload;
		try {
			payload = message.getPayloadAsBytes();
		} catch (Exception e) {
			return Future.failedFuture(e);
		}

		if (payload.length == 0)
			return Future.succeededFuture(BytesMessage.dup(message, payload));

		return switch (message.getType()) {
			case CONTENT_MESSAGE -> getOrCreateConversation(message.getConversationIdOrThrow()).compose(conv -> {
				try {
					SessionContext sc = conv.getSessionContext();
					byte[] encryptedPayload = sc.getTxCryptoContext().encrypt(payload);
					return Future.succeededFuture(BytesMessage.dup(message, encryptedPayload));
				} catch (CryptoException e) {
					return Future.failedFuture(e);
				}
			});

			case CONTROL_MESSAGE -> {
				if (message.getRecipient().equals(homePeerId)) {
					// Optimization: When sending to the home peer, the message body and envelope
					// share the same CryptoContext. Explicit body encryption is skipped here
					// because it would be redundant; the envelope encryption provides
					// the necessary security for the entire payload.
					yield Future.succeededFuture(BytesMessage.dup(message, payload));
				} else {
					try {
						byte[] encryptedPayload = userIdentity.encrypt(message.getRecipient(), payload);
						yield Future.succeededFuture(BytesMessage.dup(message, encryptedPayload));
					} catch (CryptoException e) {
						yield Future.failedFuture(e);
					}
				}
			}

			case STATE_MESSAGE -> {
				log.error("INTERNAL ERROR: messaging client is not supposed to send state messages");
				yield Future.failedFuture(new IllegalStateException("INTERNAL ERROR: messaging client is not supposed to send state messages"));
			}

			case HANDSHAKE_MESSAGE -> {
				try {
					byte[] encryptedPayload = userIdentity.encrypt(message.getRecipient(), payload);
					yield Future.succeededFuture(BytesMessage.dup(message, encryptedPayload));
				} catch (CryptoException e) {
					yield Future.failedFuture(e);
				}
			}
		};
	}

	private Future<BytesMessage> checkAndDecryptMessage(BytesMessage message) {
		final byte[] payload = message.getPayload();
		if (payload.length == 0)
			return Future.succeededFuture(message);

		final Id recipient = message.getRecipient();
		final Id from = message.getFromOrThrow();
		final Id conversationId = message.getConversationIdOrThrow();

		Message.Type type = message.getType();
		return switch (type) {
			case CONTENT_MESSAGE -> contact(conversationId).compose(contact -> {
				if (contact == null) {
					log.error("Received a {} from unknown contact {}, ignored", type, from);
					return Future.failedFuture("Received a message from unknown contact");
				}

				if (contact.isBlocked()) {
					log.debug("Received a {}} from blocked contact {}, ignored", type, from);
					return Future.failedFuture("Received a message from blocked contact");
				}

				return getOrCreateConversation(conversationId).compose(conv -> {
					try {
						SessionContext sc = conv.getSessionContext();
						CryptoContext ctx = from.equals(getUserId()) ? sc.getTxCryptoContext() : sc.getRxCryptoContext(from);
						byte[] decryptedPayload = ctx.decrypt(payload);
						return Future.succeededFuture(BytesMessage.dup(message, decryptedPayload));
					} catch (CryptoException e) {
						log.error("Decrypt {} from {} failed", type, from, e);
						return Future.failedFuture(e);
					}
				});
			});

			case CONTROL_MESSAGE, STATE_MESSAGE -> {
				// from the home peer, the payload is not encrypted
				if (from.equals(homePeerId))
					yield Future.succeededFuture(message);

				if (recipient.equals(getUserId())) {
					// channel to user RPC responses or notifications
					try {
						byte[] decryptedPayload = userIdentity.decrypt(from, payload);
						yield Future.succeededFuture(BytesMessage.dup(message, decryptedPayload));
					} catch (CryptoException e) {
						log.error("Decrypt {} from {} failed", type, from, e);
						yield Future.failedFuture(e);
					}
				} else {
					// channel broadcast notifications
					yield channel(recipient).compose(channel -> {
						if (channel == null) {
							log.warn("Received a {} from unknown channel {}, ignored", type, recipient);
							return Future.failedFuture("Received a message from unknown channel");
						}

						if (channel.isBlocked()) {
							log.debug("Received a {} from blocked channel {}, ignored", type, recipient);
							return Future.failedFuture("Received a message from blocked channel");
						}

						return getOrCreateConversation(conversationId).compose(conv -> {
							try {
								SessionContext sc = conv.getSessionContext();
								CryptoContext ctx = sc.getRxCryptoContext();
								byte[] decryptedPayload = ctx.decrypt(payload);
								return Future.succeededFuture(BytesMessage.dup(message, decryptedPayload));
							} catch (CryptoException e) {
								log.error("Decrypt {} from channel {} failed", type, recipient, e);
								return Future.failedFuture(e);
							}
						});
					});
				}
			}

			case HANDSHAKE_MESSAGE -> {
				try {
					Id id = from.equals(getUserId()) ? recipient : from;
					byte[] decryptedPayload = userIdentity.decrypt(id, payload);
					yield Future.succeededFuture(BytesMessage.dup(message, decryptedPayload));
				} catch (CryptoException e) {
					log.error("Decrypt {} from {} failed", type, from, e);
					yield Future.failedFuture(e);
				}
			}
		};
	}

	private Future<Void> resolvePeer() {
		if (config.getServiceEndpoint() == null) {
			Node node = Objects.requireNonNull(this.node, "INTERNAL ERROR: inconsistent state - node not initialized");
			log.info("Looking up service peer {} ...", config.getServicePeerId());
			return Future.fromCompletionStage(node.findPeer(config.getServicePeerId())).compose(p -> {
				if (p.isEmpty()) {
					log.error("Service peer not found {}", config.getServicePeerId());
					return Future.failedFuture("Service peer not found: " + config.getServicePeerId());
				}

				PeerInfo peer = p.get();
				URI uri = URI.create(peer.getEndpoint());
				if (!uri.isAbsolute() || uri.getHost() == null || uri.getScheme() == null ||
						uri.getPort() <= 0 || uri.getPort() > 65535 ||
						(!uri.getScheme().equals("mqtt") && !uri.getScheme().equals("mqtts"))) {
					log.error("Service peer endpoint {} is invalid", peer.getEndpoint());
					return Future.failedFuture("Service peer endpoint is invalid: " + peer.getEndpoint());
				}

				serviceEndpoint = uri;
				return Future.succeededFuture();
			});
		} else {
			serviceEndpoint = config.getServiceEndpoint();
			return Future.succeededFuture();
		}
	}

	private String getPassword() {
		byte[] nonce = Random.randomBytesSecure(16);
		byte[] deviceSig = deviceIdentity.sign(nonce);

		byte[] password = new byte[nonce.length + deviceSig.length];
		System.arraycopy(nonce, 0, password, 0, nonce.length);
		System.arraycopy(deviceSig, 0, password, nonce.length, deviceSig.length);
		return Base58.encode(password);
	}

	private int getRetryInterval() {
		if (failures == 0)
			return 0; // no delay
		else if (failures <= 6)
			return (1 << failures); // 2 ~ 64 seconds retry interval
		else if (failures <= 20)
			return 300; // 5 minutes retry interval
		else
			return 600; // 10 minutes retry interval
	}

	private Future<Void> connect() {
		if (!running.get())
			return Future.succeededFuture();

		log.info("Connecting ...");
		listeners.onConnecting();

		return resolvePeer().compose(v -> repository.getContactsRevision()).compose(revision -> {
			log.info("Current client contacts revision: {}", revision);

			contactsRevision = revision;

			MqttClientOptions options = new MqttClientOptions()
					.setAutoGeneratedClientId(false)
					.setClientId(getDeviceId().toBase58String())
					.setUsername(getUserId().toBase58String())
					.setPassword(getPassword() + "?contactsRevision=" + revision)
					// Sized to carry a one-minute inline voice note (~120-150 KB Opus/Ogg) in a single
					// message, with headroom for the envelope. This bounds what a client will both send
					// and receive, so every client must share this cap; a smaller-cap receiver would
					// drop a larger inbound publish. Still well under the service max message size (2 MB).
					.setMaxMessageSize(256 * 1024)
					.setReceiveBufferSize(264 * 1024)
					// Keep the link warm well inside the broker's keep-alive grace (1.5x the interval).
					// The connection was being dropped ~90s after connect (== 1.5 x 60s) whenever it went
					// idle, i.e. the broker timed us out before a PINGREQ was due; a shorter interval with
					// auto-ping explicitly enabled keeps an otherwise-idle session alive.
					.setKeepAliveInterval(30)
					.setAutoKeepAlive(true)
					// Bound the wait for a publish acknowledgement. Vert.x leaves this disabled by
					// default, which means a QoS-1 publish whose PUBACK never arrives would leave the
					// send pending forever. A dropped connection is already handled (onClose fails the
					// in-flight messages); this covers the case with no close to react to - a link that
					// stalls with the socket still open, which a mobile TCP stack can take minutes to
					// notice and which no PINGRESP timeout catches either.
					.setAckTimeout(ACK_TIMEOUT)
					.setHostnameVerificationAlgorithm("")
					.setCleanSession(false);

			URI serviceEndpoint = Objects.requireNonNull(this.serviceEndpoint, "INTERNAL ERROR: inconsistent state - serviceEndpoint not initialized");
			if (Objects.equals("mqtts", serviceEndpoint.getScheme())) {
				options.setSsl(true)
						.setEnabledSecureTransportProtocols(Set.of("TLSv1.2", "TLSv1.3"))
						.setTrustOptions(TrustOptions.wrap(new HybridTrustManager(homePeerId.toString(), homePeerId.bytesUnsafe())));
			}

			MqttClient client = MqttClient.create(vertx, options);
			client.closeHandler((v) -> onClose())
					.subscribeCompletionHandler(this::onSubscribeCompletion)
					.publishHandler(this::onPublish)
					.publishCompletionHandler(this::onPublishCompletion)
					.publishCompletionExpirationHandler(this::onPublishCompletionExpiration)
					.publishCompletionUnknownPacketIdHandler(this::onPublishCompletionUnknownPacketId)
					.unsubscribeCompletionHandler(this::onUnsubscribeCompletion)
					.exceptionHandler((e) -> log.error("Messaging client error", e));

			return client.connect(serviceEndpoint.getPort(), serviceEndpoint.getHost()).compose(ack -> {
				log.info("Connected to the messaging server {} @ {}", homePeerId, serviceEndpoint);
				// Start the readiness watchdog now that the connection is established: the whole
				// subscription-setup + contact-sync exchange must complete within the window, else the
				// channel is half-open and we reconnect (see armReadinessWatchdog).
				armReadinessWatchdog(client);
				Map<String, Integer> topics = Map.of(
						Topic.USER_INBOX.toString(), MqttQoS.AT_LEAST_ONCE.value(),
						Topic.USER_OUTBOX.toString(), MqttQoS.AT_LEAST_ONCE.value(),
						Topic.DEVICE_INBOX.toString(), MqttQoS.AT_LEAST_ONCE.value());
				return client.subscribe(topics).compose(pid -> {
					log.info("Subscribing the messages...");
					this.failures = 0;
					this.mqttClient = client;
					return Future.succeededFuture();
				}, error -> {
					this.failures++;
					// This branch already drives its own reconnect, so drop the readiness watchdog armed
					// at CONNACK to avoid a second, racing reconnect.
					cancelReadinessWatchdog();

					if (running.get()) {
						long delay = getRetryInterval();
						log.warn("Failed to subscribe to the messaging server {} @ {}, will retry in {} seconds...",
								homePeerId, serviceEndpoint, delay);
						vertx.setTimer(delay * 1000L, (tid) -> connect());
					} else {
						log.warn("Failed to subscribe to the messaging server {} @ {}", homePeerId, serviceEndpoint);
					}

					return Future.succeededFuture();
				});
			}, error -> {
				// A rejected CONNACK fails the connect() future (Vert.x MqttClientImpl.handleConnack).
				// For unrecoverable rejections, fail terminally and skip the retry loop.
				Optional<Future<Void>> terminal = terminalConnectionFailure(error);
				if (terminal.isPresent())
					return terminal.get();

				this.failures++;

				if (running.get()) {
					long delay = getRetryInterval();
					log.warn("Failed to connect to the messaging server {} @ {}, will retry in {} seconds...",
							homePeerId, serviceEndpoint, delay);
					vertx.setTimer(delay * 1000L, (tid) -> connect());
				} else {
					log.warn("Failed to connect to the messaging server {} @ {}", homePeerId, serviceEndpoint);
				}

				return Future.succeededFuture();
			});
		});
	}

	/**
	 * Maps an unrecoverable MQTT connection rejection to a terminal failed future so the caller
	 * stops the retry loop and surfaces the failure to the application. Returns an empty optional
	 * for recoverable or unknown errors (for example CONNECTION_REFUSED_SERVER_UNAVAILABLE), which
	 * the caller should retry.
	 */
	private Optional<Future<Void>> terminalConnectionFailure(Throwable error) {
		if (!(error instanceof MqttConnectionException mce))
			return Optional.empty();

		MqttConnectReturnCode rc = mce.code();
		switch (rc) {
			case CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION -> {
				log.error("Connection refused: unacceptable protocol version", error);
				return Optional.of(Future.failedFuture(new ConnectionException(
						ConnectionException.Reason.UNACCEPTABLE_PROTOCOL_VERSION,
						"Connection refused: unacceptable protocol version", error)));
			}
			case CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD -> {
				log.error("Connection refused: invalid client credentials", error);
				return Optional.of(Future.failedFuture(new ConnectionException(
						ConnectionException.Reason.INVALID_CREDENTIALS,
						"Connection refused: invalid client credentials", error)));
			}
			case CONNECTION_REFUSED_NOT_AUTHORIZED -> {
				log.error("Connection refused: client not authorized", error);
				return Optional.of(Future.failedFuture(new ConnectionException(
						ConnectionException.Reason.NOT_AUTHORIZED,
						"Connection refused: client not authorized", error)));
			}
			case CONNECTION_REFUSED_QUOTA_EXCEEDED -> {
				log.error("Connection refused: session limit exceeded", error);
				return Optional.of(Future.failedFuture(new ConnectionException(
						ConnectionException.Reason.SESSION_LIMIT_EXCEEDED,
						"Connection refused: session limit exceeded", error)));
			}
			default -> {
				return Optional.empty();
			}
		}
	}

	private Future<Void> disconnect() {
		if (mqttClient == null || !mqttClient.isConnected())
			return Future.succeededFuture();

		return mqttClient.disconnect();
	}

	// Arm the readiness watchdog as soon as the MQTT connection is established (CONNACK), NOT on the
	// SUB/SUBACK: subscription setup and the contact-sync push both happen within this window, and on
	// a resumed session the push can even precede the SUBACK. Readiness is the mandatory startup
	// contact-sync ([ready]); if it has not arrived by the timeout the channel is half-open, so force
	// a reconnect on [client] (its close handler -> onClose() schedules a fresh reconnect, on which the
	// service re-runs the subscription setup and re-pushes contact-sync). Arming with the connection's
	// own client makes recovery work even if the SUBACK never lands and [mqttClient] was never set.
	private void armReadinessWatchdog(MqttClient client) {
		cancelReadinessWatchdog();
		readinessWatchdogTimer = vertx.setTimer(READINESS_WATCHDOG_TIMEOUT_MS, tid -> {
			readinessWatchdogTimer = -1;
			if (!running.get() || ready)
				return;

			log.warn("Connected to {} but the startup contact sync did not arrive within {} ms; the "
					+ "channel looks half-open, forcing a reconnect to re-trigger the sync",
					homePeerId, READINESS_WATCHDOG_TIMEOUT_MS);
			if (client.isConnected())
				client.disconnect();
		});
	}

	private void cancelReadinessWatchdog() {
		if (readinessWatchdogTimer != -1) {
			vertx.cancelTimer(readinessWatchdogTimer);
			readinessWatchdogTimer = -1;
		}
	}

	private void onClose() {
		log.debug("Disconnected from the messaging server {} @ {}", homePeerId, serviceEndpoint);

		this.connected = false;
		this.ready = false;
		cancelReadinessWatchdog();
		failInflightMessages();

		listeners.onDisconnected();

		if (!running.get()) {
			log.info("Disconnected");
		} else {
			this.failures++;
			int delay = getRetryInterval();
			log.warn("Connection lost, will try to reconnect in {} seconds...", delay);
			vertx.setTimer(delay * 1000L, (tid) -> connect());
		}
	}

	private void onSubscribeCompletion(MqttSubAckMessage m) {
		List<Integer> grantedQoSLevels = m.grantedQoSLevels();
		log.debug("Subscription complete:\n\t{} - {}\n\t{} - {}\n\t{} - {}",
				Topic.USER_INBOX, grantedQoSLevels.get(0),
				Topic.USER_OUTBOX, grantedQoSLevels.get(1),
				Topic.DEVICE_INBOX, grantedQoSLevels.get(2));

		log.info("Subscribe topics success");
		log.info("Client connected to the messaging server");
		this.connected = true;
		listeners.onConnected();
	}

	private void onUnsubscribeCompletion(int packetId) {
		// nothing to do
	}

	/**
	 * Fails every message that was published but not yet acknowledged, because the connection carrying
	 * it is gone.
	 * <p>
	 * Without this a send could hang indefinitely: a message is only completed by the broker's
	 * acknowledgement, and an acknowledgement for a connection that no longer exists is never coming.
	 * The caller would sit on a future that neither succeeds nor fails - a message stuck "sending"
	 * forever, with no failure to report and nothing to retry. A publish that the broker did receive
	 * before the drop is redelivered by the session anyway, so failing here at worst asks the caller to
	 * re-send something that also went through; leaving it pending has no upside at all.
	 */
	private void failInflightMessages() {
		if (inflightMessages.isEmpty() && earlyAcknowledgements.isEmpty())
			return;

		log.debug("Failing {} in-flight message(s) on disconnect", inflightMessages.size());
		// Drain first: failing a message runs the caller's handlers, which may send again.
		List<PhotonMessage<?>> pending = new ArrayList<>(inflightMessages.values());
		inflightMessages.clear();
		earlyAcknowledgements.clear();
		for (PhotonMessage<?> message : pending)
			message.failed(new NotConnectedException("Connection lost before the message was acknowledged"));
	}

	private void onPublishCompletion(int packetId) {
		log.trace("Received publish completion for packetId {}", packetId);

		PhotonMessage<?> message = inflightMessages.remove(packetId);
		if (message == null) {
			// The acknowledgement beat the registration in sendMessageInternal; park it so that
			// registration completes the message the moment it runs, instead of stranding it.
			log.debug("Publish completion for packet {} arrived before its registration", packetId);
			earlyAcknowledgements.add(packetId);
			return;
		}

		message.sent();
	}

	private void onPublishCompletionExpiration(int packetId) {
		log.trace("Received publish completion expiration for packetId {}", packetId);

		PhotonMessage<?> message = inflightMessages.remove(packetId);
		if (message == null) {
			// noinspection LoggingSimilarMessage
			log.error("INTERNAL ERROR: no message associated with packet {}", packetId);
			return;
		}

		// Sent message failed, remove it from the sending messages
		message.failed(new MessageTimeoutException("Get message ACK timeout"));
	}

	private void onPublishCompletionUnknownPacketId(int packetId) {
		log.warn("INTERNAL WARN: unknown packet {}", packetId);
		// check is existing message associated with the unknown packet id
		PhotonMessage<?> message = inflightMessages.remove(packetId);
		if (message != null)
			message.sent();
	}

	private void onPublish(MqttPublishMessage mm) {
		final String topicName = mm.topicName();
		final BytesMessage received;

		log.trace("Received message on topic {}, packetId {}, dup {}", topicName, mm.messageId(), mm.isDup());

		try {
			// decrypt the message envelope
			final byte[] mqttPayload = serviceContext.decrypt(mm.payload().getBytes());
			received = BytesMessage.parse(mqttPayload);
			received.received();

			log.debug("Got message {}:{} from {} to {} on topic {}, packetId: {}, dup: {}",
					received.getId(), received.getType(), received.getFrom(), received.getRecipient(), topicName, mm.messageId(), mm.isDup());

			final Id recipient = received.getRecipient();
			final Id from = received.getFromOrThrow();
			final Message.Type mt = received.getType();
			final Topic topic = Topic.of(topicName);
			switch (topic) {
				case USER_INBOX -> {
					if (mt == Message.Type.CONTROL_MESSAGE) {
						log.warn("Received a {} from user inbox, ignored", mt);
						return;
					}

					final Id conversationId = recipient.equals(getUserId()) ? from : recipient;
					received.setConversationId(conversationId);
				}
				case USER_OUTBOX -> {
					if (mt == Message.Type.CONTROL_MESSAGE) {
						log.warn("Received a {} from user outbox, ignored", mt);
						return;
					}
					received.setConversationId(recipient);
				}
				case DEVICE_INBOX -> {
					if (mt != Message.Type.CONTROL_MESSAGE) {
						log.warn("Received a {} from device inbox, ignored", mt);
						return;
					}
					received.setConversationId(from);
				}
				default -> {
					log.warn("Received a {} from unknown topic {}, ignored", mt, topic);
					return;
				}
			}

			checkAndDecryptMessage(received).compose(message ->
					switch (topic) {
						case USER_INBOX -> processInboxMessage(message);
						case USER_OUTBOX -> processingOutboxMessage(message);
						case DEVICE_INBOX -> processDeviceInboxMessage(message);
						default -> {
							log.warn("Received a {} from DEVICE OUTBOX, ignored", mt);
							yield Future.succeededFuture();
						}
					}
			).onFailure(e -> {
				log.error("Failed to process message {} from {} to {}",
						received.getId(), received.getFrom(), received.getRecipient(), e);
			});
		} catch (Exception e) {
			log.error("Failed to process message on topic {}, packetId {}", topicName, mm.messageId(), e);
		}
	}

	private Future<Void> processInboxMessage(BytesMessage message) {
		final Message.Type type = message.getType();
		if (type == Message.Type.CONTENT_MESSAGE) {
			MessageContent content = MessageContent.parse(message.getPayload());
			PhotonMessage<MessageContent> contentMessage = message.dup(content);
			return repository.putMessage(contentMessage).andThen(rar -> {
				if (rar.failed())
					log.error("Failed to save the message {} to repository", contentMessage.getId(), rar.cause());

				final Id conversationId = contentMessage.getConversationIdOrThrow();
				getOrCreateConversation(conversationId).andThen(ar -> {
					if (ar.succeeded()) {
						PhotonConversation conv = ar.result();
						conv.update(contentMessage);
					} else {
						log.error("Failed to get conversation {}", conversationId, ar.cause());
					}

					listeners.onMessage(contentMessage);
				});
			});
		}

		if (type == Message.Type.STATE_MESSAGE) {
			Notification notif = Notification.parse(message.getPayload());
			if (notif.getSource().equals(userIdentity.getId()) && notif.isOriginated(getDeviceId())) {
				// This notification was triggered by an RPC request from this device.
				// Since the local state was already updated via the RPC response,
				// this redundant notification can be safely ignored.
				log.trace("Ignoring redundant(originated from self) notification {}", notif);
				return Future.succeededFuture();
			}

			final Id from = message.getFromOrThrow();
			// Notification from home peer?
			if (from.equals(homePeerId))
				return processHomePeerNotification(notif);

			// Notification from channel?
			return channel(from).compose(channel -> {
				if (channel == null) {
					if (notif.getEvent() == Notification.Event.CHANNEL_JOIN) {
						// channel join notification is a special case: no channel contact existing
						ChannelInfo ci = notif.getBody();
						if (ci.sessionKey() == null) {
							log.error("Received a channel join notification without sessionKey from {}, ignored", from);
							return Future.succeededFuture();
						}

						// The session key is encrypted with the user's own key; no external decryption needed.
						channel = new PhotonChannel(ci.channelId(), ci.sessionKey(), ci.ownerId(),
								ci.permission(), ci.name(), ci.notice(), ci.announce(), ci.createdAt(), ci.updateAt());
					} else {
						log.warn("Received a notification from unknown channel {}, ignored - {}", message.getRecipient(), notif);
						return Future.succeededFuture();
					}
				}
				return processChannelNotification(channel, notif);
			});
		}

		if (type == Message.Type.HANDSHAKE_MESSAGE) {
			Handshake hs = Handshake.parse(message.getPayload());
			hs.setFrom(message.getFromOrThrow());
			return processHandshake(hs);
		}

		log.warn("Received a CONTROL message from USER inbox, ignored");
		return Future.succeededFuture();
	}

	private Future<Void> processingOutboxMessage(BytesMessage message) {
		message.setSentAt();
		return switch (message.getType()) {
			case CONTENT_MESSAGE -> {
				MessageContent content = MessageContent.parse(message.getPayload());
				PhotonMessage<MessageContent> msg = message.dup(content);
				if (msg.isOriginated(getDeviceId())) {
					log.trace("Message {} sent to {}", msg.getId(), msg.getRecipient());
					// message is sent by this device
					listeners.onSent(msg);

					yield Future.succeededFuture();
				} else {
					log.trace("Message {} sent to {} from the other device", msg.getId(), msg.getRecipient());
					yield repository.putMessage(msg).andThen(ar -> {
						listeners.onSent(msg);
					});
				}
			}

			case HANDSHAKE_MESSAGE -> {
				if (message.isOriginated(getDeviceId())) {
					log.trace("Ignoring redundant(originated from self) handshake {} to {}", message.getId(), message.getRecipient());
					yield Future.succeededFuture(); // initialized by this device, ignore
				}

				Handshake handshake = Handshake.parse(message.getPayload());
				yield switch (handshake.getType()) {
					case FRIEND_REQUEST -> {
						String hello = handshake.getBody();
						log.trace("Friend request sent to {}", message.getRecipient());
						PhotonFriendRequest fr = new PhotonFriendRequest(message.getRecipient(), getUserId(), hello,
								handshake.getTimestamp(), System.currentTimeMillis());
						yield repository.putFriendRequest(fr);
					}

					case FRIEND_REQUEST_ACCEPT -> {
						log.trace("Accept friend request accepted from {}", message.getRecipient());
						yield repository.getFriendRequest(message.getRecipient()).compose(fr -> {
							// Trying to update the existing friend request status.
							// **DO NOT** add the contact here; the accepting device is responsible for adding
							// the new contact and broadcasting the change via a CONTACT_MUTATE notification.
							if (fr == null) {
								log.warn("No friend request to {}, ignored", message.getRecipient());
								return Future.succeededFuture();
							}

							PhotonFriendRequest request = (PhotonFriendRequest) fr;
							request.accept(handshake.getTimestamp());
							return repository.putFriendRequest(request);
						});
					}
				};
			}

			case CONTROL_MESSAGE -> {
				log.warn("Received a CONTROL message from USER outbox, ignored");
				yield Future.succeededFuture();
			}

			case STATE_MESSAGE -> {
				log.warn("Received a STATE message from USER outbox, ignored");
				yield Future.succeededFuture();
			}
		};
	}

	private Future<Void> processDeviceInboxMessage(BytesMessage message) {
		if (message.getType() == Message.Type.CONTROL_MESSAGE) {
			return processControlMessage(message);
		} else {
			log.warn("Received a {} from DEVICE inbox, ignored", message.getType());
			return Future.succeededFuture();
		}
	}

	private Future<Void> processControlMessage(BytesMessage message) {
		final RpcResponse response;
		try {
			response = RpcResponse.parse(message.getPayload());
		} catch (Exception e) {
			log.error("Failed to parse RPC response from the message from device inbox", e);
			return Future.failedFuture(e);
		}

		log.trace("Received RPC response {}", response);

		@SuppressWarnings("unchecked")
		final RpcCall<?> call = inflightRpcCalls.remove(response.getId());
		if (call == null) {
			log.warn("Received a RPC response but no matching call found, ignored");
			return Future.succeededFuture();
		}

		if (response.failed())
			log.warn("RPC call returned an error: {}", response.getError());
		else
			log.debug("RPC call completed successfully");

		call.setResponse(response);
		return Future.succeededFuture();
	}

	private Future<Void> processHandshake(Handshake handshake) {
		final Id from = handshake.getFromOrThrow();

		return switch (handshake.getType()) {
			case FRIEND_REQUEST -> {
				String hello = handshake.getBody();
				log.trace("Received a friend request from {}: {}", from, hello);
				yield contact(from).compose(contact -> {
					if (contact != null) {
						if (contact.getType() == Contact.Type.FRIEND) {
							log.warn("Received a friend request from a friend {}, ignored", contact.getId());
							return Future.succeededFuture();
						}

						if (contact.getType() == Contact.Type.CHANNEL) {
							log.error("INTERNAL ERROR!!! Received a friend request from a channel {}, ignored", contact.getId());
							return Future.succeededFuture();
						}
					}

					PhotonFriendRequest fr = new PhotonFriendRequest(from, from, hello,
							handshake.getTimestamp(), System.currentTimeMillis());
					return repository.putFriendRequest(fr).andThen(ar -> {
						listeners.onFriendRequest(from, hello);
					});
				});
			}

			case FRIEND_REQUEST_ACCEPT -> {
				log.trace("Received a friend request accept from {}", from);
				yield contact(from).compose(contact -> {
					if (contact != null) {
						if (contact.getType() == Contact.Type.FRIEND) {
							log.warn("Received a friend request accept from a friend {}, ignored", contact.getId());
							return Future.succeededFuture();
						}

						if (contact.getType() == Contact.Type.CHANNEL) {
							log.error("INTERNAL ERROR!!! Received a friend request accept from a channel {}, ignored", contact.getId());
							return Future.succeededFuture();
						}
					}

					return repository.getFriendRequest(from).compose(fr -> {
						if (fr == null || fr.getInitiatorId().equals(from)) {
							log.warn("Received a friend request accept without matched request from: {}, ignored", from);
							return Future.succeededFuture();
						}

						byte[] sessionKey = handshake.getBody();
						if (sessionKey.length != Signature.PrivateKey.BYTES) {
							log.warn("Received a friend request accept with invalid session key from: {}, ignored", from);
							return Future.succeededFuture();
						}

						PhotonFriendRequest request = (PhotonFriendRequest) fr;
						request.accept();

						log.trace("Updating friend request status to accepted: {}", from);
						return repository.putFriendRequest(request).andThen(rar -> {
							if (rar.failed())
								log.error("Failed to update the friend request status", rar.cause());

							byte[] sk = selfContext.encrypt(sessionKey);
							Friend friend = new Friend(from, sk, null);
							log.trace("Saving friend {} locally first...", from);
							repository.putContactLocally(friend).andThen(ar -> {
								// All user devices receive the 'accept' notification.
								// Since we are lacking a leader-election mechanism between devices, all devices attempt
								// to add the new contact upon receiving the 'accept' notification.
								// The contact synchronization service handles concurrency by ensuring only
								// the first received update is applied and synchronized to all devices.
								log.trace("trying to add the contact {} ...", from);
								addFriendInternal(from, sk, null).andThen(rr -> {
									if (rr.succeeded())
										log.trace("Synced new friend {} cross devices", friend.getId());
									else
										log.trace("Failed to sync new friend {} cross devices", friend.getId(), rr.cause());

									listeners.onFriendRequestAccepted(friend.getId());
								}).otherwiseEmpty();
							});
						});
					});
				});
			}
		};
	}

	private Future<Void> processHomePeerNotification(Notification notif) {
		// notification from home peer
		return switch (notif.getEvent()) {
			case SESSION_NEW -> {
				SessionInfo si = notif.getBody();
				listeners.onNewSession(si);
				yield Future.succeededFuture();
			}

			case CONTACT_SYNC -> {
				ContactSync contactSync = notif.getBody();
				yield applyContactSync(contactSync).andThen(ar -> {
					if (ar.succeeded()) {
						if (!ready) {
							log.info("Contact synchronization completed on startup, revision {}, client is ready", contactsRevision);
							ready = true;
							cancelReadinessWatchdog();
							listeners.onContactSynced();
						}
					}
				});
			}

			case CHANNEL_CREATE -> {
				ChannelInfo ci = notif.getBody();
				if (ci.sessionKey() == null) {
					log.error("Received a channel create notification without sessionKey from {}, ignored", notif.getSource());
					yield Future.succeededFuture();
				}

				log.trace("Channel {} created", ci.channelId());

				// The session key is encrypted with the user's own key; no external decryption needed.
				PhotonChannel channel = new PhotonChannel(ci.channelId(), ci.sessionKey(), ci.ownerId(),
						ci.permission(), ci.name(), ci.notice(), ci.announce(), ci.createdAt(), ci.updateAt());

				// Save the channel to the local database immediately to provide a responsive UI.
				// We bypass the official 'contactsRevision' update here because this is a local-only
				// optimistic insert. The server will later broadcast a CONTACT_MUTATE notification
				// triggered by the device that initiated the original CHANNEL_CREATE request,
				// which will perform the formal, synchronized update of the contact list and revision state.
				yield channel(channel.getId()).compose(existing -> {
					if (existing != null) {
						log.trace("Channel already exists(synced as contact), skip save channel");
						return Future.succeededFuture();
					}
					return repository.putContactLocally(channel);
				}).compose(v -> {
					if (!ci.members().isEmpty())
						return repository.putChannelMembers(channel.getId(), ci.members());
					else
						return Future.succeededFuture();
				}).andThen(ar -> {
					if (ar.failed())
						log.error("Failed to save channel {} to repository", ci.channelId(), ar.cause());

					listeners.onChannelCreated(channel);
				});
			}

			default -> {
				log.warn("Received an unknown notification from home peer, ignored - {}", notif);
				yield Future.succeededFuture();
			}
		};
	}

	private Future<Void> processChannelNotification(PhotonChannel channel, Notification notif) {
		return switch (notif.getEvent()) {
			case CHANNEL_DELETE -> {
				log.trace("Channel {} deleted", channel.getId());
				yield repository.removeContactLocally(channel.getId()).compose(removed -> {
					contactCache.synchronous().invalidate(channel.getId());
					return removeContactsInternal(List.of(channel.getId())).andThen(sar -> {
						if (sar.succeeded())
							log.debug("Synced channel {} remove on CHANNEL_DELETE cross devices", channel.getId());
						else
							log.debug("Failed to sync channel {} remove on CHANNEL_DELETE cross devices", channel.getId());

						listeners.onChannelDeleted(channel);
					});
				}).mapEmpty();
			}

			case CHANNEL_JOIN -> {
				log.trace("Channel {} joined", channel.getId());
				yield channel(channel.getId()).compose(existing -> {
					if (existing == null) {
						// Save the channel to the local database immediately to provide a responsive UI.
						// We bypass the official 'contactsRevision' update here because this is a local-only
						// optimistic insert. The server will later broadcast a CONTACT_MUTATE notification
						// triggered by the device that initiated the original CHANNEL_CREATE request,
						// which will perform the formal, synchronized update of the contact list and revision state.
						return repository.putContactLocally(channel);
					} else {
						// ready synced the channel contact
						return Future.succeededFuture();
					}
				}).compose(v -> {
					// put the channel members to the local database
					ChannelInfo ci = notif.getBody();
					if (!ci.members().isEmpty())
						return repository.putChannelMembers(channel.getId(), ci.members());
					else
						return Future.succeededFuture();
				}).andThen(ar -> {
					listeners.onJoinedChannel(channel);
				});
			}

			case CHANNEL_LEAVE -> {
				log.trace("Channel {} left", channel.getId());
				yield repository.removeContactLocally(channel.getId()).andThen(ar -> {
					contactCache.synchronous().invalidate(channel.getId());
					if (ar.succeeded() && !ar.result())
						log.warn("!!! Channel contact {} should exists, but failed to remove.", channel.getId());

					listeners.onLeftChannel(channel);
				}).mapEmpty();
			}

			case CHANNEL_OWNERSHIP_TRANSFER -> {
				Id newOwnerId = notif.getBody();
				log.trace("Channel {} ownership transfer to {}", channel.getId(), newOwnerId);

				yield channel.tryLoadMembers().compose(v -> {
					Id oldOwnerId = channel.getOwnerId();
					Optional<Channel.Member> oldOwner = channel.getMember(channel.getOwnerId());
					Optional<Channel.Member> newOwner = channel.getMember(newOwnerId);
					Future<Void> future;
					if (oldOwner.isEmpty() || newOwner.isEmpty()) {
						// not up-to-date?
						log.warn("Looks like channel {} is outdated, try to refresh", channel.getId());
						future = refreshChannel(channel);
					} else {
						future = repository.updateChannelOwnership(channel.getId(), channel.getOwnerId(), newOwnerId).compose(vv -> {
							PhotonChannel updatedChannel = channel.editChannel().setOwnerId(newOwnerId).build();
							repository.putContactLocally(updatedChannel);
							updatedChannel.invalidateMembers();
							contactCache.synchronous().invalidate(channel.getId());
							return Future.succeededFuture();
						}, error -> {
							log.warn("Update channel ownership failed, looks like channel {} is outdated, try to refresh", channel.getId());
							return refreshChannel(channel);
						});
					}
					return future.andThen(ar -> {
						listeners.onChannelOwnershipTransferred(channel, oldOwnerId, newOwnerId);
					});
				});
			}

			case CHANNEL_SESSION_KEY_ROTATE -> {
				log.trace("Channel {} session key rotated", channel.getId());
				byte[] sessionKey = notif.getBody();
				PhotonContact updatedChannel = channel.edit().setSessionKey(sessionKey).build();
				yield repository.putContactLocally(updatedChannel).andThen(ar -> {
					contactCache.synchronous().invalidate(channel.getId());
					listeners.onChannelSessionKeyRotated(channel);
				});
			}

			case CHANNEL_INFO_UPDATE -> {
				log.trace("Channel {} info updated: {}", channel.getId(), notif.getBody());
				PhotonChannel updatedChannel = channel.editChannel().patch(notif.getBody()).build();
				yield repository.putContactLocally(updatedChannel).andThen(ar -> {
					contactCache.synchronous().invalidate(channel.getId());
					listeners.onChannelUpdated(updatedChannel);
				});
			}

			case CHANNEL_MEMBERS_ROLE_UPDATE -> {
				ChannelMembersRole content = notif.getBody();
				log.trace("Channel {} members role updated: {} - {}", channel.getId(), content.role(), content.memberIds());
				yield repository.updateChannelMembersRole(channel.getId(), content.memberIds(), content.role()).andThen(ar -> {
					channel.invalidateMembers();
					repository.getChannelMembers(channel.getId(), content.memberIds()).onSuccess(members -> {
						listeners.onChannelMembersRoleChanged(channel, members, content.role());
					});
				}).mapEmpty();
			}

			case CHANNEL_MEMBERS_BAN -> {
				List<Id> memberIds = notif.getBody();
				log.trace("Channel {} members banned: {}", channel.getId(), memberIds);
				yield repository.updateChannelMembersRole(channel.getId(), memberIds, Channel.Role.BANNED).andThen(ar -> {
					channel.invalidateMembers();
					repository.getChannelMembers(channel.getId(), memberIds).onSuccess(members -> {
						listeners.onChannelMembersBanned(channel, members);
					});
				}).mapEmpty();
			}

			case CHANNEL_MEMBERS_UNBAN -> {
				List<Id> memberIds = notif.getBody();
				log.trace("Channel {} members unbanned: {}", channel.getId(), memberIds);
				yield repository.updateChannelMembersRole(channel.getId(), memberIds, Channel.Role.MEMBER).andThen(ar -> {
					channel.invalidateMembers();
					repository.getChannelMembers(channel.getId(), memberIds).onSuccess(members -> {
						listeners.onChannelMembersUnbanned(channel, members);
					});
				}).mapEmpty();
			}

			case CHANNEL_MEMBERS_REMOVE -> {
				List<Id> memberIds = notif.getBody();
				log.trace("Channel {} members removed: {}", channel.getId(), memberIds);
				yield repository.getChannelMembers(channel.getId(), memberIds).compose(known -> {
					// The local member rows can be behind this notification: the member join it refers
					// to may not be applied yet, or may never have arrived. Fall back to the ids the
					// notification carries, so the removal is always applied and reported - dropping it
					// would leave the removed member, or this user's own channel contact, behind
					// forever. Mirrors the CHANNEL_MEMBER_LEAVE handling below.
					List<Channel.Member> members = resolveNotifiedMembers(known, memberIds);

					if (memberIds.contains(getUserId())) {
						return repository.removeContactLocally(channel.getId()).compose(removed -> {
							contactCache.synchronous().invalidate(channel.getId());
							// All user devices receive the 'CHANNEL_MEMBERS_REMOVE' notification.
							// Since we are lacking a leader-election mechanism between devices, all devices attempt
							// to remove the channel contact upon receiving the notification.
							// The contact synchronization service handles concurrency by ensuring only
							// the first received update is applied and synchronized to all devices.
							return removeContactsInternal(List.of(channel.getId())).andThen(sar -> {
								if (sar.succeeded())
									log.debug("Synced channel {} remove cross devices", channel.getId());
								else
									log.debug("Failed to sync channel {} remove cross devices", channel.getId());

								listeners.onChannelMembersRemoved(channel, members);
							}).otherwiseEmpty();
						});
					} else {
						return repository.removeChannelMembers(channel.getId(), memberIds).andThen(ar -> {
							channel.invalidateMembers();
							listeners.onChannelMembersRemoved(channel, members);
						}).mapEmpty();
					}
				});
			}

			case CHANNEL_MEMBER_JOIN -> {
				ChannelMember member = notif.getBody();
				log.trace("Channel {} member joined: {}", channel.getId(), member);
				yield repository.putChannelMember(channel.getId(), member).andThen(ar -> {
					channel.invalidateMembers();
					listeners.onChannelMemberJoined(channel, member);
				});
			}

			case CHANNEL_MEMBER_LEAVE -> {
				Id memberId = notif.getBody();
				log.trace("Channel {} member left: {}", channel.getId(), memberId);
				yield repository.getChannelMember(channel.getId(), memberId).compose(member -> {
							if (member == null)
								return Future.succeededFuture(new ChannelMember(memberId, Channel.Role.MEMBER, 0));
							else
								return repository.removeChannelMember(channel.getId(), memberId).map(member);
						}, error ->
								Future.succeededFuture(new ChannelMember(memberId, Channel.Role.MEMBER, 0))
				).map(member -> {
					channel.invalidateMembers();
					listeners.onChannelMemberLeft(channel, member);
					return null;
				});
			}

			default -> {
				log.warn("Received an unknown notification from channel {}, ignored", channel.getId());
				yield Future.succeededFuture();
			}
		};
	}

	// Pairs every member id carried by a channel membership notification with the member record this
	// client holds for it, synthesizing a placeholder for the ids it does not know about. Keeps a
	// notification from being dropped only because the local member rows have not caught up yet.
	static List<Channel.Member> resolveNotifiedMembers(List<Channel.Member> known, List<Id> memberIds) {
		if (known.size() == memberIds.size())
			return known;

		Map<Id, Channel.Member> knownById = new HashMap<>(known.size());
		for (Channel.Member member : known)
			knownById.put(member.getId(), member);

		List<Channel.Member> members = new ArrayList<>(memberIds.size());
		for (Id memberId : memberIds)
			members.add(knownById.getOrDefault(memberId, new ChannelMember(memberId, Channel.Role.MEMBER, 0)));

		return members;
	}

	private Future<Void> applyContactSync(ContactSync contactSync) {
		log.info("Applying contact sync: local revision {}, remote revision {} ...",
				contactsRevision, contactSync.getRevision());

		return switch (contactSync.getType()) {
			case UP_TO_DATE -> {
				log.debug("Local contacts are up-to-date");
				yield Future.succeededFuture();
			}

			case DELTA -> {
				log.debug("Applying contact updates with delta");
				List<ContactMutation> mutations = contactSync.getMutations();
				Future<Void> applyChain = Future.succeededFuture();
				for (ContactMutation mutation : mutations) {
					applyChain = applyChain.compose(v -> applyContactMutation(mutation));
				}
				yield applyChain.compose(v -> {
					// contactsRevision = contactSync.getRevision();
					if (contactsRevision != contactSync.getRevision()) {
						log.error("the revision not up-to-data after applied the mutations, expected: {}, actual: {}",
								contactSync.getRevision(), contactsRevision);
						return Future.failedFuture(new IllegalStateException("the revision not up-to-data after applied the mutations"));
					}

					return Future.succeededFuture();
				});
			}

			case SNAPSHOT -> {
				log.debug("Applying contact updates with snapshot");
				int revision = contactSync.getRevision();
				List<Contact> contacts;
				try {
					contacts = contactSync.getContacts().stream()
							.map(opaque -> (Contact) PhotonContact.fromOpaque(opaque, selfContext))
							.toList();
				} catch (IllegalArgumentException e) {
					// PhotonContact.fromOpaque will throw exception
					log.error("Failed to parse opaque contact from snapshot", e);
					yield Future.failedFuture(e);
				}
				yield repository.putContacts(revision, contacts).onSuccess(v -> {
					contactsRevision = revision;
				});
			}
		};
	}

	private Future<Void> applyContactMutation(ContactMutation mutation) {
		final int revision = mutation.getRevision() + 1;
		return switch (mutation.getOp()) {
			case ADD -> {
				PhotonContact contact;
				try {
					contact = PhotonContact.fromOpaque(Objects.requireNonNull(mutation.getData()), selfContext);
				} catch (IllegalArgumentException e) {
					log.error("Failed to parse opaque contact from mutation", e);
					yield Future.failedFuture(e);
				}

				log.trace("Applying contact mutation(base rev {}): add {}", mutation.getRevision(), contact.getId());
				yield repository.putContacts(revision, List.of(contact)).onSuccess(v -> {
					contactsRevision = revision;
					contactCache.synchronous().invalidate(contact.getId());
					if (contact instanceof PhotonChannel ch)
						refreshChannel(ch).andThen(v2 -> listeners.onContactAdded(contact));
					else
						listeners.onContactAdded(contact);
				});
			}

			case UPDATE -> {
				PhotonContact contact;
				try {
					contact = PhotonContact.fromOpaque(Objects.requireNonNull(mutation.getData()), selfContext);
				} catch (IllegalArgumentException e) {
					log.error("Failed to parse opaque contact from mutation", e);
					yield Future.failedFuture(e);
				}

				log.trace("Applying contact mutation(base rev {}): update {}", mutation.getRevision(), contact.getId());
				yield repository.putContacts(revision, List.of(contact)).onSuccess(v -> {
					contactsRevision = revision;
					contactCache.synchronous().invalidate(contact.getId());
					listeners.onContactsUpdated(List.of(contact));
				});
			}

			case REMOVE -> {
				List<Id> contactIds = Objects.requireNonNull(mutation.getData());
				log.trace("Applying contact mutation(base rev {}): remove {}", mutation.getRevision(), contactIds);
				yield (Future<Void>) repository.removeContacts(revision, contactIds).<@Nullable Void>map(removed -> {
					contactsRevision = revision;
					contactCache.synchronous().invalidateAll(contactIds);
					if (removed)
						listeners.onContactsRemoved(contactIds);
					return null;
				});
			}

			case CLEAR -> repository.clearContacts(revision).onSuccess(v -> {
				log.trace("Applying contact mutation(base rev {}): clear", mutation.getRevision());
				contactsRevision = revision;
				contactCache.synchronous().invalidateAll();
				listeners.onContactsCleared();
			});
		};
	}
}
