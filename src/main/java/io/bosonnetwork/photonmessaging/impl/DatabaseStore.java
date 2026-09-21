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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.SqlResult;
import io.vertx.sqlclient.templates.SqlTemplate;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;

import io.bosonnetwork.Id;
import io.bosonnetwork.database.CollectionParameter;
import io.bosonnetwork.database.VersionedSchema;
import io.bosonnetwork.database.VertxDatabase;
import io.bosonnetwork.photonmessaging.Channel;
import io.bosonnetwork.photonmessaging.Contact;
import io.bosonnetwork.photonmessaging.MessagingStore;
import io.bosonnetwork.photonmessaging.exceptions.RepositoryException;
import io.bosonnetwork.photonmessaging.impl.database.PostgresDatabase;
import io.bosonnetwork.photonmessaging.impl.database.SqlDialect;
import io.bosonnetwork.photonmessaging.impl.database.SqliteDatabase;

public abstract class DatabaseStore implements VertxDatabase, MessagingStore {
	private int schemaVersion;

	protected abstract Logger getLogger();

	protected abstract void init(Vertx vertx);

	protected @Nullable String getSchema() {
		return null;
	}

	protected abstract Path getMigrationPath();

	public abstract SqlDialect getDialect();

	public static boolean supports(String uri) {
		return uri.startsWith(SqliteDatabase.CONNECTION_URI_PREFIX) ||
				uri.startsWith(PostgresDatabase.CONNECTION_URI_PREFIX);
	}

	public static DatabaseStore create(String uri, int poolSize, @Nullable String schema) {
		Objects.requireNonNull(uri, "uri");

		if (uri.startsWith(SqliteDatabase.CONNECTION_URI_PREFIX))
			return new SqliteDatabase(uri, poolSize);
		if (uri.startsWith(PostgresDatabase.CONNECTION_URI_PREFIX))
			return new PostgresDatabase(uri, poolSize, schema);

		throw new IllegalArgumentException("Unsupported database: " + uri);
	}

	public static DatabaseStore create(String uri, int poolSize) {
		return create(uri, poolSize, null);
	}

	public static DatabaseStore create(String uri) {
		return create(uri, 0, null);
	}

	private SqlClient getClientOrThrow() {
		return Objects.requireNonNull(getClient(), "Client is not initialized");
	}

	@Override
	public Future<Void> open() {
		throw new UnsupportedOperationException("open() is not supported for DatabaseStore, use open(Vertx) instead");
	}

	public Future<Integer> open(Vertx vertx) {
		init(vertx);

		VersionedSchema schema = VersionedSchema.init(vertx, getClientOrThrow(), getSchema(), getMigrationPath());
		return schema.migrate().andThen(ar -> {
					if (ar.succeeded()) {
						schemaVersion = schema.getCurrentVersion().version();
						getLogger().info("DatabaseStore is ready, current schema version: {}", schemaVersion);
					} else {
						getLogger().error("Schema migration failed, current schema version: {}",
								schema.getCurrentVersion().version(), ar.cause());
					}
				})
				.map(v -> schema.getCurrentVersion().version())
				.recover(e -> Future.failedFuture(new RepositoryException("DatabaseStore operation failed", e)));
	}

	public int getSchemaVersion() {
		return schemaVersion;
	}

	// close() is declared by both VertxDatabase (default) and MessagingStore (abstract);
	// resolve the diamond explicitly by delegating to the VertxDatabase implementation.
	@Override
	public Future<Void> close() {
		return VertxDatabase.super.close();
	}

	////////////////////////////////////////////////////////////////////////////
	// MessagingRepository - Contacts
	////////////////////////////////////////////////////////////////////////////

	@Override
	public Future<Integer> getContactsRevision() {
		return withConnection(c ->
				forQuery(c, getDialect().selectContactsRevision())
						.execute(Map.of())
						.map(this::findInteger)
		).recover(e -> {
			getLogger().error("Failed to get ContactsRevision", e);
			return Future.failedFuture(new RepositoryException(e));
		});
	}

	@Override
	public Future<Void> putContact(int revision, StoredContact contact) {
		return withTransaction(c -> {
			Future<?> future = forUpdate(c, getDialect().upsertContact())
						.execute(paramsFromContact(contact));

			if (contact.type() == Contact.Type.CHANNEL.value()) {
				StoredChannel channel = Objects.requireNonNull(contact.channel());
				future = future.compose(v -> forUpdate(c, getDialect().upsertChannel())
						.execute(paramsFromChannel(channel)));
			}

			return future.compose(na ->
					forUpdate(c, getDialect().upsertContactsRevision())
							.execute(Map.of("revision", revision, "updatedAt", System.currentTimeMillis()))
			).<Void>mapEmpty();
		}).recover(e -> {
			getLogger().error("Failed to put contact for revision {}", revision, e);
			return Future.failedFuture(new RepositoryException(e));
		});
	}

	@Override
	public Future<Void> putContacts(int revision, Collection<StoredContact> updated) {
		return withTransaction(c -> {
			List<Future<?>> futures = new ArrayList<>();
			for (StoredContact contact : updated) {
				Future<?> future = forUpdate(c, getDialect().upsertContact())
						.execute(paramsFromContact(contact));

				if (contact.type() == Contact.Type.CHANNEL.value()) {
					StoredChannel channel = Objects.requireNonNull(contact.channel());
					future = future.compose(v -> forUpdate(c, getDialect().upsertChannel())
							.execute(paramsFromChannel(channel)));
				}

				futures.add(future);
			}

			return Future.all(futures).compose(na ->
					forUpdate(c, getDialect().upsertContactsRevision())
							.execute(Map.of("revision", revision, "updatedAt", System.currentTimeMillis()))
			).<Void>mapEmpty();
		}).recover(e -> {
			getLogger().error("Failed to put contacts for revision {}", revision, e);
			return Future.failedFuture(new RepositoryException(e));
		});
	}

	@Override
	public Future<Void> putContactLocally(StoredContact contact) {
		return withTransaction(c -> {
			Future<?> future = forUpdate(c, getDialect().upsertContact())
					.execute(paramsFromContact(contact));

			if (contact.type() == Contact.Type.CHANNEL.value()) {
				StoredChannel channel = Objects.requireNonNull(contact.channel());
				future = future.compose(v -> forUpdate(c, getDialect().upsertChannel())
						.execute(paramsFromChannel(channel)));
			}

			return future.<Void>mapEmpty();
		}).recover(e -> {
			getLogger().error("Failed to put contact locally {}", contact.id(), e);
			return Future.failedFuture(new RepositoryException(e));
		});
	}

	@Override
	public Future<Boolean> removeContactLocally(Id contactId) {
		return withTransaction(c ->
				forUpdate(c, getDialect().deleteContact())
						.execute(Map.of("id", contactId.bytesUnsafe()))
						.map(this::hasAffectedRows)
						.compose(removed -> forUpdate(c, getDialect().deleteMessagesByConversation())
								.execute(Map.of("conversationId", contactId.bytesUnsafe()))
								.map(removed)
						)
		).recover(e -> {
			getLogger().error("Failed to remove contact locally {}", contactId, e);
			return Future.failedFuture(new RepositoryException(e));
		});
	}

	@Override
	public Future<Boolean> removeContact(int revision, Id contactId) {
		return removeContacts(revision, List.of(contactId));
	}

	@Override
	public Future<Boolean> removeContacts(int revision, Collection<Id> contactIds) {
		return withTransaction(c -> {
			Future<?> future;
			if (contactIds.isEmpty())
				future = Future.succeededFuture();
			else {
				CollectionParameter<byte[]> idsParam = new CollectionParameter<>("id",
						contactIds.stream().map(Id::bytesUnsafe).toList());
				future = forUpdate(c, getDialect().deleteContacts(idsParam))
						.execute(idsParam.getParams())
						.map(this::hasAffectedRows)
						.compose(removed -> {
							CollectionParameter<byte[]> cidsParam = new CollectionParameter<>("cid",
									contactIds.stream().map(Id::bytesUnsafe).toList());
							return forUpdate(c, getDialect().deleteMessagesByConversations(cidsParam))
									.execute(cidsParam.getParams())
									.map(removed);
						});
			}

			return future.compose(na -> forUpdate(c, getDialect().upsertContactsRevision())
							.execute(Map.of("revision", revision, "updatedAt", System.currentTimeMillis())))
					.map(true);
		}).recover(e -> {
			getLogger().error("Failed to remove contacts for revision {}", revision, e);
			return Future.failedFuture(new RepositoryException(e));
		});
	}

	@Override
	public Future<Void> clearContacts(int revision) {
		return withTransaction(c ->
				forUpdate(c, getDialect().clearContacts())
						.execute(Map.of())
						.compose(r -> forUpdate(c, getDialect().upsertContactsRevision())
								.execute(Map.of("revision", revision, "updatedAt", System.currentTimeMillis())))
						.<Void>mapEmpty()
		).recover(e -> {
			getLogger().error("Failed to clear contacts for revision {}", revision, e);
			return Future.failedFuture(new RepositoryException(e));
		});
	}

	@Override
	public Future<@Nullable StoredContact> getContact(Id contactId) {
		return withConnection(c ->
				forQuery(c, getDialect().selectContact())
						.execute(Map.of("id", contactId.bytesUnsafe()))
						.map(rs -> findUnique(rs, this::rowToContact))
		).recover(e -> {
			getLogger().error("Failed to get contact {}", contactId, e);
			return Future.failedFuture(new RepositoryException(e));
		}).<@Nullable StoredContact>map(contact -> contact);
	}

	@Override
	public Future<List<StoredContact>> getContacts(Collection<Id> contactIds) {
		if (contactIds.isEmpty())
			return Future.succeededFuture(List.of());

		CollectionParameter<byte[]> idsParam = new CollectionParameter<>("id",
				contactIds.stream().map(Id::bytesUnsafe).toList());
		return withConnection(c ->
				forQuery(c, getDialect().selectContacts(idsParam))
						.execute(idsParam.getParams())
						.map(rs -> findMany(rs, this::rowToContact))
		).recover(e -> {
			getLogger().error("Failed to get contacts {}", contactIds, e);
			return Future.failedFuture(new RepositoryException(e));
		});
	}

	@Override
	public Future<List<StoredContact>> getAllContacts() {
		return withConnection(c ->
				forQuery(c, getDialect().selectAllContacts())
						.execute(Map.of())
						.map(rs -> findMany(rs, this::rowToContact))
		).recover(e -> {
			getLogger().error("Failed to get all contacts", e);
			return Future.failedFuture(new RepositoryException(e));
		});
	}

	////////////////////////////////////////////////////////////////////////////
	// MessagingRepository - Messages
	////////////////////////////////////////////////////////////////////////////

	@Override
	public Future<Long> putMessage(StoredMessage message) {
		return withTransaction(c ->
				forQuery(c, getDialect().insertMessage())
						.execute(paramsFromMessage(message))
						.map(this::findLong)
		).recover(e -> {
			getLogger().error("Failed to put message {}", message.id(), e);
			return Future.failedFuture(new RepositoryException(e));
		});
	}

	@Override
	public Future<Void> updateMessageSentTime(Id messageId, long sentAt) {
		return withTransaction(c ->
				forUpdate(c, getDialect().updateMessageSentTime())
						.execute(Map.of("id", messageId.bytesUnsafe(), "sentAt",sentAt))
						.<Void>mapEmpty()
		).recover(e -> {
			getLogger().error("Failed to update message sent time for {}", messageId, e);
			return Future.failedFuture(new RepositoryException(e));
		});
	}

	@Override
	public Future<List<StoredMessage>> getMessagesInRange(Id conversationId, long begin, long end) {
		return withConnection(c ->
				forQuery(c, getDialect().selectMessagesByTimeRange())
						.execute(Map.of("conversationId", conversationId.bytesUnsafe(),
								"begin", begin,
								"end", end))
						.map(rs -> findMany(rs, this::rowToMessage))
		).recover(e -> {
			getLogger().error("Failed to get messages for conversation {} in range [{}, {})", conversationId, begin, end, e);
			return Future.failedFuture(new RepositoryException(e));
		});
	}

	@Override
	public Future<List<StoredMessage>> getMessagesBefore(Id conversationId, long until, int limit, int offset) {
		return withConnection(c ->
				forQuery(c, getDialect().selectMessagesWithPagination())
						.execute(Map.of("conversationId", conversationId.bytesUnsafe(),
								"until", until,
								"limit", limit,
								"offset", offset))
						.map(rs -> findMany(rs, this::rowToMessage))
		).recover(e -> {
			getLogger().error("Failed to get messages for conversation {} until {}, limit={}, offset={}", conversationId, until, limit, offset, e);
			return Future.failedFuture(new RepositoryException(e));
		});
	}

	@Override
	public Future<Boolean> removeMessages(Collection<Long> rids) {
		if (rids.isEmpty())
			return Future.succeededFuture(true);

		CollectionParameter<Long> ridsParam = new CollectionParameter<>("rid", rids);
		return withTransaction(c ->
				forUpdate(c, getDialect().deleteMessages(ridsParam))
						.execute(ridsParam.getParams())
						.map(this::hasAffectedRows)
		).recover(e -> {
			getLogger().error("Failed to remove messages by rids {}", rids, e);
			return Future.failedFuture(new RepositoryException(e));
		});
	}

	@Override
	public Future<Boolean> removeMessagesByConversation(Id conversationId) {
		return withTransaction(c ->
				forUpdate(c, getDialect().deleteMessagesByConversation())
						.execute(Map.of("conversationId", conversationId.bytesUnsafe()))
						.map(this::hasAffectedRows)
		).recover(e -> {
			getLogger().error("Failed to remove messages for conversation {}", conversationId, e);
			return Future.failedFuture(new RepositoryException(e));
		});
	}

	// Internal helper used by removeContacts/removeConversations; not part of the MessagingStore contract.
	Future<Boolean> removeMessagesByConversations(Collection<Id> conversationIds) {
		if (conversationIds.isEmpty())
			return Future.succeededFuture(true);

		CollectionParameter<byte[]> cidsParam = new CollectionParameter<>("cid",
				conversationIds.stream().map(Id::bytesUnsafe).toList());

		return withTransaction(c ->
				forUpdate(c, getDialect().deleteMessagesByConversations(cidsParam))
						.execute(cidsParam.getParams())
						.map(this::hasAffectedRows)
		).recover(e -> {
			getLogger().error("Failed to remove messages for conversations {}", conversationIds, e);
			return Future.failedFuture(new RepositoryException(e));
		});
	}

	@Override
	public Future<Void> clearMessages() {
		return withTransaction(c ->
				forUpdate(c, getDialect().clearMessages())
						.execute(Map.of())
						.<Void>mapEmpty()
		).recover(e -> {
			getLogger().error("Failed to clear messages", e);
			return Future.failedFuture(new RepositoryException(e));
		});
	}

	////////////////////////////////////////////////////////////////////////////
	// MessagingRepository - Friend Requests
	////////////////////////////////////////////////////////////////////////////

	@Override
	public Future<Void> putFriendRequest(StoredFriendRequest friendRequest) {
		return withTransaction(c ->
				forUpdate(c, getDialect().upsertFriendRequest())
						.execute(paramsFromFriendRequest(friendRequest))
						.<Void>mapEmpty()
		).recover(e -> {
			getLogger().error("Failed to put friend request for {}", friendRequest.id(), e);
			return Future.failedFuture(new RepositoryException(e));
		});
	}

	@Override
	public Future<@Nullable StoredFriendRequest> getFriendRequest(Id userId) {
		return withConnection(c ->
				forQuery(c, getDialect().selectFriendRequest())
						.execute(Map.of("id", userId.bytesUnsafe()))
						.<@Nullable StoredFriendRequest>map(rs -> findUnique(rs, this::rowToFriendRequest))
		).recover(e -> {
			getLogger().error("Failed to get friend request for {}", userId, e);
			return Future.failedFuture(new RepositoryException(e));
		}).<@Nullable StoredFriendRequest>map(friendRequest -> friendRequest);
	}

	@Override
	public Future<List<StoredFriendRequest>> getFriendRequests() {
		return withConnection(c ->
				forQuery(c, getDialect().selectAllFriendRequests())
						.execute(Map.of())
						.map(rs -> findMany(rs, this::rowToFriendRequest))
		).recover(e -> {
			getLogger().error("Failed to get all friend requests", e);
			return Future.failedFuture(new RepositoryException(e));
		});
	}

	@Override
	public Future<Boolean> removeFriendRequest(Id userId) {
		return withTransaction(c ->
				forUpdate(c, getDialect().deleteFriendRequest())
						.execute(Map.of("id", userId.bytesUnsafe()))
						.map(this::hasAffectedRows)
		).recover(e -> {
			getLogger().error("Failed to remove friend request for {}", userId, e);
			return Future.failedFuture(new RepositoryException(e));
		});
	}

	@Override
	public Future<Boolean> removeFriendRequests(Collection<Id> userIds) {
		if (userIds.isEmpty())
			return Future.succeededFuture(false);

		CollectionParameter<byte[]> idsParam = new CollectionParameter<>("id", userIds.stream().map(Id::bytesUnsafe).toList());
		return withTransaction(c ->
				forUpdate(c, getDialect().deleteFriendRequests(idsParam))
						.execute(idsParam.getParams())
						.map(this::hasAffectedRows)
		).recover(e -> {
			getLogger().error("Failed to remove friend requests for {}", userIds, e);
			return Future.failedFuture(new RepositoryException(e));
		});
	}

	@Override
	public Future<Void> clearFriendRequests() {
		return withTransaction(c ->
				forUpdate(c, getDialect().clearFriendRequests())
						.execute(Map.of())
						.<Void>mapEmpty()
		).recover(e -> {
			getLogger().error("Failed to clear friend requests", e);
			return Future.failedFuture(new RepositoryException(e));
		});
	}

	////////////////////////////////////////////////////////////////////////////
	// MessagingRepository - Conversations
	////////////////////////////////////////////////////////////////////////////

	@Override
	public Future<@Nullable StoredConversation> getConversation(Id conversationId) {
		return withConnection(c ->
				forQuery(c, getDialect().selectConversation())
						.execute(Map.of("contactId", conversationId.bytesUnsafe()))
						.<@Nullable StoredConversation>map(rs -> findUnique(rs, this::rowToConversation))
		).recover(e -> {
			getLogger().error("Failed to get conversation {}", conversationId, e);
			return Future.failedFuture(new RepositoryException(e));
		}).<@Nullable StoredConversation>map(conversation -> conversation);
	}

	@Override
	public Future<List<StoredConversation>> getAllConversations() {
		return withConnection(c ->
				forQuery(c, getDialect().selectAllConversations())
						.execute(Map.of())
						.map(rs -> findMany(rs, this::rowToConversation))
		).recover(e -> {
			getLogger().error("Failed to get all conversations", e);
			return Future.failedFuture(new RepositoryException(e));
		});
	}

	@Override
	public Future<Boolean> removeConversation(Id conversationId) {
		return removeMessagesByConversation(conversationId);
	}

	@Override
	public Future<Boolean> removeConversations(Collection<Id> conversationIds) {
		return removeMessagesByConversations(conversationIds);
	}

	////////////////////////////////////////////////////////////////////////////
	// MessagingRepository - Channels
	////////////////////////////////////////////////////////////////////////////

	@Override
	public Future<Void> updateChannelOwnership(Id channelId, Id oldOwnerId, Id newOwnerId) {
		return withTransaction(c ->
				forUpdate(c, getDialect().updateChannelOwnership())
						.execute(Map.of("id", channelId.bytesUnsafe(),
								"owner", newOwnerId.bytesUnsafe()))
						.compose(v -> forUpdate(c, getDialect().updateChannelMemberRole())
								.execute(Map.of("channelId", channelId.bytesUnsafe(),
										"id", oldOwnerId.bytesUnsafe(),
										"role", Channel.Role.MEMBER.value())))
						.compose(v -> forUpdate(c, getDialect().updateChannelMemberRole())
								.execute(Map.of("channelId", channelId.bytesUnsafe(),
										"id", newOwnerId.bytesUnsafe(),
										"role", Channel.Role.OWNER.value())))
						.<Void>mapEmpty()
		).recover(e -> {
			getLogger().error("Failed to update channel ownership for {} from {} to {}", channelId, oldOwnerId, newOwnerId, e);
			return Future.failedFuture(new RepositoryException(e));
		});
	}

	@Override
	public Future<Void> putChannelMembers(Id channelId, Collection<StoredChannelMember> members) {
		if (members.isEmpty())
			return Future.succeededFuture();

		List<Map<String, Object>> batchParams = members.stream()
				.map(m -> paramsFromChannelMember(channelId, m))
				.toList();

		return withTransaction(c ->
				forUpdate(c, getDialect().upsertChannelMember())
						.executeBatch(batchParams)
						.<Void>mapEmpty()
		).recover(e -> {
			getLogger().error("Failed to put channel members for {}", channelId, e);
			return Future.failedFuture(new RepositoryException(e));
		});
	}

	@Override
	public Future<Void> refillChannelMembers(Id channelId, Collection<StoredChannelMember> members) {
		return withTransaction(c ->
				forUpdate(c, getDialect().clearChannelMembers())
						.execute(Map.of("channelId", channelId.bytesUnsafe()))
						.compose(v -> {
							if (members.isEmpty())
								return Future.succeededFuture();

							List<Map<String, Object>> batchParams = members.stream()
									.map(m -> paramsFromChannelMember(channelId, m))
									.toList();

							return forUpdate(c, getDialect().upsertChannelMember()).executeBatch(batchParams);
						}).<Void>mapEmpty()
		).recover(e -> {
			getLogger().error("Failed to refill channel members for {}", channelId, e);
			return Future.failedFuture(new RepositoryException(e));
		});
	}

	@Override
	public Future<@Nullable StoredChannelMember> getChannelMember(Id channelId, Id memberId) {
		return withConnection(c ->
				forQuery(c, getDialect().selectChannelMember())
						.execute(Map.of("channelId", channelId.bytesUnsafe(), "id", memberId.bytesUnsafe()))
						.<@Nullable StoredChannelMember>map(rs -> findUnique(rs, this::rowToChannelMember))
		).recover(e -> {
			getLogger().error("Failed to get channel member {} for channel {}", memberId, channelId, e);
			return Future.failedFuture(new RepositoryException(e));
		}).<@Nullable StoredChannelMember>map(channelMember -> channelMember);
	}

	@Override
	public Future<List<StoredChannelMember>> getChannelMembers(Id channelId, @Nullable Collection<Id> memberId) {
		if (memberId == null || memberId.isEmpty())
			return Future.succeededFuture(Collections.emptyList());

		CollectionParameter<byte[]> idsParam = new CollectionParameter<>("id", memberId.stream().map(Id::bytesUnsafe).toList());
		Map<String, Object> params = idsParam.getParams();
		params.put("channelId", channelId.bytesUnsafe());

		return withConnection(c ->
				forQuery(c, getDialect().selectChannelMembers(idsParam))
						.execute(params)
						.map(rs -> findMany(rs, this::rowToChannelMember))
		).recover(e -> {
			getLogger().error("Failed to get channel members {} for channel {}", memberId, channelId, e);
			return Future.failedFuture(new RepositoryException(e));
		});
	}

	@Override
	public Future<List<StoredChannelMember>> getAllChannelMembers(Id channelId) {
		return withConnection(c ->
				forQuery(c, getDialect().selectAllChannelMembers())
						.execute(Map.of("channelId", channelId.bytesUnsafe()))
						.map(rs -> findMany(rs, this::rowToChannelMember))
		).recover(e -> {
			getLogger().error("Failed to get all channel members for channel {}", channelId, e);
			return Future.failedFuture(new RepositoryException(e));
		});
	}

	@Override
	public Future<Boolean> updateChannelMembersRole(Id channelId, Collection<Id> memberIds, int role) {
		if (memberIds.isEmpty())
			return Future.succeededFuture(true);

		CollectionParameter<byte[]> idsParam = new CollectionParameter<>("id", memberIds.stream().map(Id::bytesUnsafe).toList());
		Map<String, Object> params = idsParam.getParams();
		params.put("channelId", channelId.bytesUnsafe());
		params.put("role", role);

		return withTransaction(c ->
				forUpdate(c, getDialect().updateChannelMembersRole(idsParam))
						.execute(params)
						.map(this::hasAffectedRows)
		).recover(e -> {
			getLogger().error("Failed to update channel members role for channel {}", channelId, e);
			return Future.failedFuture(new RepositoryException(e));
		});
	}

	@Override
	public Future<Boolean> removeChannelMembers(Id channelId, Collection<Id> memberIds) {
		if (memberIds.isEmpty())
			return Future.succeededFuture(true);

		CollectionParameter<byte[]> idsParam = new CollectionParameter<>("id", memberIds.stream().map(Id::bytesUnsafe).toList());
		Map<String, Object> params = idsParam.getParams();
		params.put("channelId", channelId.bytesUnsafe());

		return withTransaction(c ->
				forUpdate(c, getDialect().deleteChannelMembers(idsParam))
						.execute(params)
						.map(this::hasAffectedRows)
		).recover(e -> {
			getLogger().error("Failed to remove channel members for channel {}", channelId, e);
			return Future.failedFuture(new RepositoryException(e));
		});
	}

	////////////////////////////////////////////////////////////////////////////
	// Mapping & Parameter Helpers
	////////////////////////////////////////////////////////////////////////////

	private Map<String, @Nullable Object> paramsFromMessage(StoredMessage message) {
		Map<String, @Nullable Object> params = new HashMap<>();
		params.put("id", message.id().bytesUnsafe());
		params.put("conversationId", message.conversationId() != null ? message.conversationId().bytesUnsafe() : null);
		params.put("version", message.version());
		params.put("recipient", message.recipient().bytesUnsafe());
		params.put("type", message.type());
		params.put("fromId", message.from() != null ? message.from().bytesUnsafe() : null);
		params.put("createdAt", message.createdAt());
		params.put("sentAt", message.sentAt());
		params.put("receivedAt", message.receivedAt());

		params.put("contentType", message.contentType());
		params.put("contentDisposition", message.contentDisposition());
		params.put("payload", message.payload());
		return params;
	}

	private StoredMessage rowToMessage(Row row) {
		long rid = row.getLong("rid");
		Id conversationId = Objects.requireNonNull(getId(row, "conversation_id"));
		int version = row.getInteger("version");
		Id id = Objects.requireNonNull(getId(row, "id"));
		Id recipient = Objects.requireNonNull(getId(row, "recipient"));
		int type = row.getInteger("type");
		Id fromId = getId(row, "from_id");
		long createdAt = row.getLong("created_at");
		String contentType = row.getString("content_type");
		String contentDisposition = row.getString("content_disposition");
		byte[] payloadBytes = getBytes(row, "payload");
		long sentAt = row.getLong("sent_at");
		long receivedAt = row.getLong("received_at");

		// StoredMessage carries the raw payload bytes; parsing into MessageContent happens once in the
		// repository layer (toMessage). The payload column is nullable (BLOB/BYTEA DEFAULT NULL).
		return new StoredMessage(rid, id, conversationId, version, recipient, type, fromId,
				createdAt, contentType, contentDisposition, payloadBytes, sentAt, receivedAt);
	}

	private Map<String, @Nullable Object> paramsFromFriendRequest(StoredFriendRequest request) {
		Map<String, @Nullable Object> params = new HashMap<>();
		params.put("id", request.id().bytesUnsafe());
		params.put("initiator", request.initiator().bytesUnsafe());
		params.put("hello", request.hello());
		params.put("createdAt", request.createdAt());
		params.put("updatedAt", request.updatedAt());
		params.put("accepted", request.accepted());
		params.put("acceptedAt", request.acceptedAt());
		return params;
	}

	private StoredFriendRequest rowToFriendRequest(Row row) {
		Id userId = Objects.requireNonNull(getId(row, "id"));
		Id initiatorId = Objects.requireNonNull(getId(row, "initiator"));
		String hello = row.getString("hello");
		long createdAt = row.getLong("created_at");
		long updatedAt = row.getLong("updated_at");
		boolean accepted = getBoolean(row, "accepted");
		long acceptedAt = row.getLong("accepted_at");

		return new StoredFriendRequest(userId, initiatorId, hello, createdAt, updatedAt, accepted, acceptedAt);
	}

	private Map<String, @Nullable Object> paramsFromContact(StoredContact contact) {
		Map<String, @Nullable Object> params = new HashMap<>();
		params.put("id", contact.id().bytesUnsafe());
		params.put("type", contact.type());
		params.put("sessionKey", contact.sessionKey());
		params.put("name", contact.name());
		params.put("remark", contact.remark());
		params.put("tags", contact.tags());
		params.put("muted", contact.muted());
		params.put("blocked", contact.blocked());
		params.put("revision", contact.revision());
		params.put("createdAt", contact.createdAt());
		params.put("updatedAt", contact.updatedAt());
		return params;
	}

	private StoredContact rowToContact(Row row) {
		Id id = Objects.requireNonNull(getId(row, "id"));
		int type = row.getInteger("type");
		byte[] sessionKey = getBytes(row, "session_key");
		String name = row.getString("name");
		String remark = row.getString("remark");
		String tags = row.getString("tags");
		boolean muted = getBoolean(row, "muted");
		boolean blocked = getBoolean(row, "blocked");
		int revision = row.getInteger("revision");
		long createdAt = row.getLong("created_at");
		long updatedAt = row.getLong("updated_at");

		StoredChannel channel = null;
		if (type == Contact.Type.CHANNEL.value()) {
			Id owner = getId(row, "owner");
			int permission = row.getInteger("permission");
			String notice = row.getString("notice");
			boolean announce = getBoolean(row, "announce");
			channel = new StoredChannel(id, Objects.requireNonNull(owner), permission, notice, announce);
		}

		return new StoredContact(id, type, sessionKey, name, remark, tags, muted, blocked,
				revision, createdAt, updatedAt, channel);
	}

	private Map<String, @Nullable Object> paramsFromChannel(StoredChannel channel) {
		Map<String, @Nullable Object> params = new HashMap<>();
		params.put("id", channel.id().bytesUnsafe());
		params.put("owner", channel.owner().bytesUnsafe());
		params.put("permission", channel.permission());
		params.put("notice", channel.notice());
		params.put("announce", channel.announce());

		return params;
	}

	private Map<String, Object> paramsFromChannelMember(Id channelId, StoredChannelMember member) {
		Map<String, Object> params = new HashMap<>();
		params.put("id", member.id().bytesUnsafe());
		params.put("channelId", channelId.bytesUnsafe());
		params.put("role", member.role());
		params.put("joined", member.joined());
		return params;
	}

	private StoredChannelMember rowToChannelMember(Row row) {
		Id id = Objects.requireNonNull(getId(row, "id"));
		Id channelId = Objects.requireNonNull(getId(row, "channel_id"));
		int role = row.getInteger("role");
		long joined = row.getLong("joined");
		return new StoredChannelMember(id, channelId, role, joined);
	}

	private StoredConversation rowToConversation(Row row) {
		StoredContact contact = rowToContact(row);

		Long msgRid = row.getLong("last_message_rid");
		int msgVersion = row.getInteger("last_message_version");
		Id msgId = Objects.requireNonNull(getId(row, "last_message_id"));
		Id msgConversationId = Objects.requireNonNull(getId(row, "last_message_conversation_id"));
		Id msgRecipient = Objects.requireNonNull(getId(row, "last_message_recipient"));
		int msgType = row.getInteger("last_message_type");
		Id msgFromId = getId(row, "last_message_from_id");
		long msgCreatedAt = row.getLong("last_message_created_at");
		String msgContentType = row.getString("last_message_content_type");
		String msgContentDisposition = row.getString("last_message_content_disposition");
		byte[] msgPayloadBytes = getBytes(row, "last_message_payload");
		long msgSentAt = row.getLong("last_message_sent_at");
		long msgReceivedAt = row.getLong("last_message_received_at");

		// StoredMessage carries the raw payload bytes; parsing into MessageContent happens once in the
		// repository layer (toMessage). The payload column is nullable (BLOB/BYTEA DEFAULT NULL).
		StoredMessage lastMessage = new StoredMessage(msgRid, msgId, msgConversationId, msgVersion,
				msgRecipient, msgType, msgFromId, msgCreatedAt, msgContentType, msgContentDisposition,
				msgPayloadBytes, msgSentAt, msgReceivedAt);

		return new StoredConversation(contact, lastMessage);
	}

	////////////////////////////////////////////////////////////////////////////
	// VertxDatabase Helpers
	////////////////////////////////////////////////////////////////////////////

	protected static SqlTemplate<Map<String, Object>, SqlResult<Void>> forUpdate(SqlConnection connection, String template) {
		return SqlTemplate.forUpdate(connection, template);
	}

	protected static SqlTemplate<Map<String, Object>, RowSet<Row>> forQuery(SqlConnection connection, String template) {
		return SqlTemplate.forQuery(connection, template);
	}

	protected static @Nullable Id getId(Row row, String column) {
		byte[] bytes = getBytes(row, column);
		return bytes == null ? null : Id.of(bytes);
	}

	protected static byte @Nullable [] getBytes(Row row, String column) {
		Buffer buf = row.getBuffer(column);
		return buf == null ? null : buf.getBytes();
	}

	// Booleans arrive typed differently per backend: PostgreSQL returns a Boolean, while SQLite
	// stores them as integer 0/1 (Number). The String branch is only a last-resort fallback.
	protected static boolean getBoolean(Row row, String column) {
		Object value = row.getValue(column);
		return value instanceof Boolean b ? b :
				(value instanceof Number n ? n.intValue() != 0 :
				 (value instanceof String s && Boolean.parseBoolean(s)));
	}
}