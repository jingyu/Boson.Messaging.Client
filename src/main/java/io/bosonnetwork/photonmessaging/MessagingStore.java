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

package io.bosonnetwork.photonmessaging;

import java.util.Collection;
import java.util.List;

import io.vertx.core.Future;
import org.jspecify.annotations.Nullable;

import io.bosonnetwork.Id;

/**
 * Public persistence contract for the Photon messaging client, expressed entirely in plain data
 * records that mirror the storage columns. It exists so a platform can supply its own storage backend
 * (for example a native Android androidx.sqlite/Room implementation) without depending on the client's
 * internal types.
 *
 * <p>A {@code MessagingStore} is supplied through {@link Configuration} (see
 * {@code Configuration.Builder.store}). When present, the client uses it instead of the built-in
 * JDBC/Vert.x SQL backend; the client adapts between these records and its internal model.
 *
 * <p>All operations are asynchronous and return a Vert.x {@link Future}. Enum-like fields (contact
 * {@code type}, message {@code type}, channel {@code permission}, member {@code role}) are carried as
 * their stable integer values. The {@code rid} of a message is assigned by the store (an
 * auto-incrementing sequence) and is the canonical ordering key for messages.
 *
 * <p>Several operations are required to be atomic: contact writes that also bump the contacts revision,
 * channel ownership transfer, and {@link #refillChannelMembers}. Implementations should run those in a
 * single transaction.
 */
public interface MessagingStore {
	// ------------------------------------------------------------------------
	// Records (mirror the storage columns)
	// ------------------------------------------------------------------------

	/**
	 * A persisted message row.
	 *
	 * @param rid the store-assigned, auto-incrementing sequence id; the canonical ordering key for
	 *            messages. Ignored when supplied to {@link #putMessage} and assigned by the store.
	 * @param id the application-level message id
	 * @param conversationId the id of the conversation (contact) the message belongs to
	 * @param version the message format version
	 * @param recipient the id of the message recipient
	 * @param type the message type, carried as its stable integer value
	 * @param from the id of the sender, or {@code null} when not applicable
	 * @param createdAt the creation time, in milliseconds since the epoch
	 * @param contentType the MIME content type of the payload, or {@code null}
	 * @param contentDisposition the content disposition of the payload, or {@code null}
	 * @param payload the raw message payload, or {@code null}
	 * @param sentAt the time the message was sent, in milliseconds since the epoch
	 * @param receivedAt the time the message was received, in milliseconds since the epoch
	 */
	record StoredMessage(
			long rid,
			Id id,
			Id conversationId,
			int version,
			Id recipient,
			int type,
			@Nullable Id from,
			long createdAt,
			@Nullable String contentType,
			@Nullable String contentDisposition,
			byte @Nullable [] payload,
			long sentAt,
			long receivedAt) {
	}

	/**
	 * A persisted channel row (present only when a contact is a channel).
	 *
	 * @param id the channel id (the same id as the owning contact)
	 * @param owner the id of the channel owner
	 * @param permission the channel permission, carried as its stable integer value
	 * @param notice the channel notice text, or {@code null}
	 * @param announce whether the channel is in announce-only mode
	 */
	record StoredChannel(
			Id id,
			Id owner,
			int permission,
			@Nullable String notice,
			boolean announce) {
	}

	/**
	 * A persisted contact row.
	 *
	 * @param id the contact id
	 * @param type the contact type, carried as its stable integer value
	 * @param sessionKey the session key bytes for the contact, or {@code null}
	 * @param name the display name, or {@code null}
	 * @param remark the local remark/alias, or {@code null}
	 * @param tags the tags assigned to the contact, or {@code null}
	 * @param muted whether the contact is muted
	 * @param blocked whether the contact is blocked
	 * @param revision the contact's revision
	 * @param createdAt the creation time, in milliseconds since the epoch
	 * @param updatedAt the last update time, in milliseconds since the epoch
	 * @param channel the channel row, non-null when the contact is a channel, otherwise {@code null}
	 */
	record StoredContact(
			Id id,
			int type,
			byte @Nullable [] sessionKey,
			@Nullable String name,
			@Nullable String remark,
			@Nullable String tags,
			boolean muted,
			boolean blocked,
			int revision,
			long createdAt,
			long updatedAt,
			@Nullable StoredChannel channel) {
	}

	/**
	 * A persisted channel-member row.
	 *
	 * @param id the member's id
	 * @param channelId the id of the channel the member belongs to
	 * @param role the member's role, carried as its stable integer value
	 * @param joined the time the member joined, in milliseconds since the epoch
	 */
	record StoredChannelMember(
			Id id,
			Id channelId,
			int role,
			long joined) {
	}

	/**
	 * A persisted friend-request row.
	 *
	 * @param id the id of the user the request concerns
	 * @param initiator the id of the user who initiated the request
	 * @param hello the optional greeting message, or {@code null}
	 * @param createdAt the creation time, in milliseconds since the epoch
	 * @param updatedAt the last update time, in milliseconds since the epoch
	 * @param accepted whether the request has been accepted
	 * @param acceptedAt the time the request was accepted, in milliseconds since the epoch
	 */
	record StoredFriendRequest(
			Id id,
			Id initiator,
			@Nullable String hello,
			long createdAt,
			long updatedAt,
			boolean accepted,
			long acceptedAt) {
	}

	/**
	 * A derived conversation: a contact that has at least one message, together with the latest message
	 * (the row with the greatest {@code rid}). {@code lastMessage} is never null for results returned by
	 * the store, but is modeled nullable for callers that build conversations incrementally.
	 */
	record StoredConversation(
			StoredContact contact,
			@Nullable StoredMessage lastMessage) {
	}

	/**
	 * Opens the store and prepares its schema. Called once during client start, before any other
	 * operation. A native implementation creates/migrates its own schema here.
	 *
	 * @return a future completing when the store is ready
	 */
	Future<Void> open();

	/**
	 * Closes the store and releases its resources.
	 *
	 * @return a future completing when closed
	 */
	Future<Void> close();

	// ------------------------------------------------------------------------
	// Messages
	// ------------------------------------------------------------------------

	/**
	 * Inserts a message and returns the assigned {@code rid}.
	 *
	 * @param message the message to store (its {@code rid} field is ignored on input)
	 * @return a future with the assigned rid
	 */
	Future<Long> putMessage(StoredMessage message);

	/**
	 * Updates the sent time of a message identified by its message id.
	 *
	 * @param messageId the application-level id of the message to update
	 * @param sentAt the new sent time, in milliseconds since the epoch
	 * @return a future completing when the sent time is updated
	 */
	Future<Void> updateMessageSentTime(Id messageId, long sentAt);

	/**
	 * Retrieves the messages of a conversation within a time range, ordered by {@code rid} ascending.
	 *
	 * @param conversationId the id of the conversation
	 * @param begin the start time (inclusive), in milliseconds since the epoch
	 * @param end the end time (exclusive), in milliseconds since the epoch
	 * @return a future with the list of messages where {@code begin <= createdAt < end}
	 */
	Future<List<StoredMessage>> getMessagesInRange(Id conversationId, long begin, long end);

	/**
	 * Retrieves a page of messages of a conversation, newest first.
	 *
	 * @param conversationId the id of the conversation
	 * @param until the upper bound (inclusive) for {@code createdAt}, in milliseconds since the epoch
	 * @param limit the maximum number of messages to return
	 * @param offset the number of messages to skip
	 * @return a future with the page of messages where {@code createdAt <= until}, ordered newest first
	 */
	Future<List<StoredMessage>> getMessagesBefore(Id conversationId, long until, int limit, int offset);

	/**
	 * Removes a single message by its rid.
	 *
	 * @param id the rid of the message to remove
	 * @return a future with {@code true} if a message was removed, {@code false} otherwise
	 */
	default Future<Boolean> removeMessage(long id) {
		return removeMessages(List.of(id));
	}

	/**
	 * Removes messages by their rids.
	 *
	 * @param rids the rids of the messages to remove
	 * @return a future with {@code true} if any message was removed, {@code false} otherwise
	 */
	Future<Boolean> removeMessages(Collection<Long> rids);

	/**
	 * Removes all messages of a conversation.
	 *
	 * @param conversationId the id of the conversation
	 * @return a future with {@code true} if any message was removed, {@code false} otherwise
	 */
	Future<Boolean> removeMessagesByConversation(Id conversationId);

	/**
	 * Removes all messages from the store.
	 *
	 * @return a future completing when all messages have been removed
	 */
	Future<Void> clearMessages();

	// ------------------------------------------------------------------------
	// Friend requests
	// ------------------------------------------------------------------------

	/**
	 * Inserts a friend request, or replaces the one already stored for the same user.
	 * <p>
	 * A replacement overwrites every field, the initiator and the creation time included: there is
	 * one record per user, and a new request replaces the old one whatever its direction or state.
	 * </p>
	 *
	 * @param friendRequest the friend request to store
	 * @return a future completing when the friend request is stored
	 */
	Future<Void> putFriendRequest(StoredFriendRequest friendRequest);

	/**
	 * Retrieves the friend request concerning the given user.
	 *
	 * @param userId the id of the user whose friend request is requested
	 * @return a future with the friend request, or {@code null} if none exists
	 */
	Future<@Nullable StoredFriendRequest> getFriendRequest(Id userId);

	/**
	 * Retrieves all friend requests.
	 *
	 * @return a future with the list of all friend requests
	 */
	Future<List<StoredFriendRequest>> getFriendRequests();

	/**
	 * Removes the friend request concerning the given user.
	 *
	 * @param userId the id of the user whose friend request should be removed
	 * @return a future with {@code true} if a friend request was removed, {@code false} otherwise
	 */
	Future<Boolean> removeFriendRequest(Id userId);

	/**
	 * Removes the friend requests concerning the given users.
	 *
	 * @param userIds the ids of the users whose friend requests should be removed
	 * @return a future with {@code true} if any friend request was removed, {@code false} otherwise
	 */
	Future<Boolean> removeFriendRequests(Collection<Id> userIds);

	/**
	 * Removes all friend requests from the store.
	 *
	 * @return a future completing when all friend requests have been removed
	 */
	Future<Void> clearFriendRequests();

	// ------------------------------------------------------------------------
	// Conversations (derived)
	// ------------------------------------------------------------------------

	/**
	 * Retrieves a conversation by its id.
	 *
	 * @param conversationId the id of the conversation (contact)
	 * @return a future with the conversation, or {@code null} if the contact has no messages
	 */
	Future<@Nullable StoredConversation> getConversation(Id conversationId);

	/**
	 * Retrieves all conversations (every contact that has at least one message).
	 *
	 * @return a future with the list of all conversations
	 */
	Future<List<StoredConversation>> getAllConversations();

	/**
	 * Removes a single conversation and its messages.
	 *
	 * @param conversationId the id of the conversation to remove
	 * @return a future with {@code true} if the conversation was removed, {@code false} otherwise
	 */
	Future<Boolean> removeConversation(Id conversationId);

	/**
	 * Removes multiple conversations and their messages.
	 *
	 * @param conversationIds the ids of the conversations to remove
	 * @return a future with {@code true} if any conversation was removed, {@code false} otherwise
	 */
	Future<Boolean> removeConversations(Collection<Id> conversationIds);

	// ------------------------------------------------------------------------
	// Contacts (revision-aware writes must be atomic)
	// ------------------------------------------------------------------------

	/**
	 * Returns the current local contacts revision.
	 *
	 * @return a future with the current revision number
	 */
	Future<Integer> getContactsRevision();

	/**
	 * Upserts a contact (and its channel row when present) without changing the contacts revision.
	 *
	 * @param contact the contact to insert or update
	 * @return a future completing when the contact is stored
	 */
	Future<Void> putContactLocally(StoredContact contact);

	/**
	 * Removes a contact (and its messages) without changing the contacts revision.
	 *
	 * @param contactId the id of the contact to remove
	 * @return a future with {@code true} if the contact was removed, {@code false} otherwise
	 */
	Future<Boolean> removeContactLocally(Id contactId);

	/**
	 * Atomically upserts a contact and sets the contacts revision.
	 *
	 * @param revision the new contacts revision
	 * @param contact the contact to insert or update
	 * @return a future completing when the operation is finished
	 */
	Future<Void> putContact(int revision, StoredContact contact);

	/**
	 * Atomically upserts contacts and sets the contacts revision.
	 *
	 * @param revision the new contacts revision
	 * @param contacts the contacts to insert or update
	 * @return a future completing when the operation is finished
	 */
	Future<Void> putContacts(int revision, Collection<StoredContact> contacts);

	/**
	 * Atomically removes a contact and sets the contacts revision.
	 *
	 * @param revision the new contacts revision
	 * @param contactId the id of the contact to remove
	 * @return a future with {@code true} if the contact was removed, {@code false} otherwise
	 */
	Future<Boolean> removeContact(int revision, Id contactId);

	/**
	 * Atomically removes contacts and sets the contacts revision.
	 *
	 * @param revision the new contacts revision
	 * @param contactIds the ids of the contacts to remove
	 * @return a future with {@code true} if any contact was removed, {@code false} otherwise
	 */
	Future<Boolean> removeContacts(int revision, Collection<Id> contactIds);

	/**
	 * Atomically clears all contacts and sets the contacts revision.
	 *
	 * @param revision the new contacts revision
	 * @return a future completing when the operation is finished
	 */
	Future<Void> clearContacts(int revision);

	/**
	 * Retrieves a contact by its id.
	 *
	 * @param contactId the id of the contact
	 * @return a future with the contact, or {@code null} if not found
	 */
	Future<@Nullable StoredContact> getContact(Id contactId);

	/**
	 * Retrieves multiple contacts by their ids.
	 *
	 * @param contactIds the ids of the contacts to retrieve
	 * @return a future with the list of found contacts
	 */
	Future<List<StoredContact>> getContacts(Collection<Id> contactIds);

	/**
	 * Retrieves all contacts.
	 *
	 * @return a future with the list of all contacts
	 */
	Future<List<StoredContact>> getAllContacts();

	/**
	 * Checks whether a contact exists.
	 *
	 * @param contactId the id of the contact
	 * @return a future with {@code true} if the contact exists, {@code false} otherwise
	 */
	default Future<Boolean> existsContact(Id contactId) {
		return getContact(contactId).map(c -> c != null);
	}

	// ------------------------------------------------------------------------
	// Channel members
	// ------------------------------------------------------------------------

	/**
	 * Atomically transfers channel ownership: sets the channel owner, demotes the old owner to the
	 * member role, and promotes the new owner to the owner role.
	 *
	 * @param channelId the id of the channel whose ownership is being transferred
	 * @param oldOwnerId the id of the current owner
	 * @param newOwnerId the id of the new owner
	 * @return a future completing when the transfer is finished
	 */
	Future<Void> updateChannelOwnership(Id channelId, Id oldOwnerId, Id newOwnerId);

	/**
	 * Inserts or updates a single channel member.
	 *
	 * @param channelId the id of the channel
	 * @param member the member to insert or update
	 * @return a future completing when the operation is finished
	 */
	default Future<Void> putChannelMember(Id channelId, StoredChannelMember member) {
		return putChannelMembers(channelId, List.of(member));
	}

	/**
	 * Inserts or updates multiple channel members.
	 *
	 * @param channelId the id of the channel
	 * @param members the members to insert or update
	 * @return a future completing when the operation is finished
	 */
	Future<Void> putChannelMembers(Id channelId, Collection<StoredChannelMember> members);

	/**
	 * Atomically replaces all members of a channel with the given collection.
	 *
	 * @param channelId the id of the channel
	 * @param members the new set of members
	 * @return a future completing when the operation is finished
	 */
	Future<Void> refillChannelMembers(Id channelId, Collection<StoredChannelMember> members);

	/**
	 * Retrieves a single member of a channel.
	 *
	 * @param channelId the id of the channel
	 * @param memberId the id of the member
	 * @return a future with the member, or {@code null} if not found
	 */
	Future<@Nullable StoredChannelMember> getChannelMember(Id channelId, Id memberId);

	/**
	 * Retrieves multiple members of a channel by their ids.
	 *
	 * @param channelId the id of the channel
	 * @param memberIds the ids of the members to retrieve
	 * @return a future with the list of found members
	 */
	Future<List<StoredChannelMember>> getChannelMembers(Id channelId, Collection<Id> memberIds);

	/**
	 * Retrieves all members of a channel.
	 *
	 * @param channelId the id of the channel
	 * @return a future with the list of all members
	 */
	Future<List<StoredChannelMember>> getAllChannelMembers(Id channelId);

	/**
	 * Sets the role of multiple members in a channel.
	 *
	 * @param channelId the id of the channel
	 * @param memberIds the ids of the members to update
	 * @param role the new role, carried as its stable integer value
	 * @return a future with {@code true} if any member was updated, {@code false} otherwise
	 */
	Future<Boolean> updateChannelMembersRole(Id channelId, Collection<Id> memberIds, int role);

	/**
	 * Removes a single member from a channel.
	 *
	 * @param channelId the id of the channel
	 * @param memberId the id of the member to remove
	 * @return a future with {@code true} if the member was removed, {@code false} otherwise
	 */
	default Future<Boolean> removeChannelMember(Id channelId, Id memberId) {
		return removeChannelMembers(channelId, List.of(memberId));
	}

	/**
	 * Removes multiple members from a channel.
	 *
	 * @param channelId the id of the channel
	 * @param memberIds the ids of the members to remove
	 * @return a future with {@code true} if any member was removed, {@code false} otherwise
	 */
	Future<Boolean> removeChannelMembers(Id channelId, Collection<Id> memberIds);
}