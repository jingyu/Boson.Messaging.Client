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

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import io.vertx.core.Vertx;
import org.jspecify.annotations.Nullable;

import io.bosonnetwork.Id;
import io.bosonnetwork.Node;
import io.bosonnetwork.crypto.Signature;
import io.bosonnetwork.photonmessaging.impl.PhotonMessagingClient;

/**
 * The primary interface for the Boson Messaging Client.
 * <p>
 * This client provides comprehensive APIs for managing conversations, messages,
 * contacts, and channels within the Boson network.
 * <p>
 * <b>Asynchronous error contract.</b> Unless stated otherwise, the methods on this
 * interface are non-blocking and report failures by completing the returned
 * {@link CompletableFuture} exceptionally rather than by throwing. The completion
 * exception is a {@link io.bosonnetwork.photonmessaging.exceptions.MessagingException}
 * (or one of its subtypes), for example:
 * <ul>
 *   <li>{@link io.bosonnetwork.photonmessaging.exceptions.ChannelNotExistsException} /
 *       {@link io.bosonnetwork.photonmessaging.exceptions.ContactNotExistsException}
 *       when the target channel or contact does not exist;</li>
 *   <li>{@link io.bosonnetwork.photonmessaging.exceptions.InsufficientPermissionException}
 *       when the caller is not allowed to perform the operation;</li>
 *   <li>{@link io.bosonnetwork.photonmessaging.exceptions.MessageTimeoutException} when a
 *       send is not acknowledged in time;</li>
 *   <li>{@link io.bosonnetwork.photonmessaging.exceptions.RepositoryException} for local
 *       persistence failures, and
 *       {@link io.bosonnetwork.photonmessaging.exceptions.rpc.RpcException} (carrying an
 *       error code) for failures reported by the messaging service.</li>
 * </ul>
 * Argument-validation problems (such as {@code null} arguments or out-of-range values)
 * are signalled synchronously with {@link NullPointerException} /
 * {@link IllegalArgumentException}, and calling a method while the client is not running
 * fails fast with {@link IllegalStateException}.
 */
public interface MessagingClient {
	/**
	 * Default limit for the number of messages to retrieve in a single request.
	 */
	static final int DEFAULT_MESSAGES_LIMIT = 100;

	////////////////////////////////////////////////////////////////////////////
	// Client identities
	////////////////////////////////////////////////////////////////////////////
	/**
	 * Retrieves the identifier of the local user.
	 *
	 * @return the user's {@link Id}.
	 */
	Id getUserId();

	/**
	 * Retrieves the identifier of the current device.
	 *
	 * @return the device's {@link Id}.
	 */
	Id getDeviceId();

	/**
	 * Retrieves the identifier of the messaging service peer.
	 *
	 * @return the service's {@link Id}.
	 */
	Id getServicePeerId();

	/**
	 * Retrieves the endpoint of the messaging service.
	 *
	 * @return an {@link Optional} holding the service endpoint URI as a string, or an empty
	 *         {@code Optional} if no endpoint has been configured or resolved yet.
	 */
	Optional<String> getServiceEndpoint();

	/**
	 * Retrieves the data directory used by the client.
	 *
	 * @return the {@link Path} to the data directory.
	 */
	Path getDataDir();

	////////////////////////////////////////////////////////////////////////////
	// Listeners
	////////////////////////////////////////////////////////////////////////////
	/**
	 * Adds a listener for connection state changes.
	 *
	 * @param listener the {@link ConnectionListener} to add.
	 */
	void addConnectionListener(ConnectionListener listener);

	/**
	 * Removes a previously added connection state listener.
	 *
	 * @param listener the {@link ConnectionListener} to remove.
	 */
	void removeConnectionListener(ConnectionListener listener);

	/**
	 * Adds a listener for message-related events.
	 *
	 * @param listener the {@link MessageListener} to add.
	 */
	void addMessageListener(MessageListener listener);

	/**
	 * Removes a previously added message listener.
	 *
	 * @param listener the {@link MessageListener} to remove.
	 */
	void removeMessageListener(MessageListener listener);

	/**
	 * Adds a listener for channel-related events.
	 *
	 * @param listener the {@link ChannelListener} to add.
	 */
	void addChannelListener(ChannelListener listener);

	/**
	 * Removes a previously added channel listener.
	 *
	 * @param listener the {@link ChannelListener} to remove.
	 */
	void removeChannelListener(ChannelListener listener);

	/**
	 * Adds a listener for contact-related events.
	 *
	 * @param listener the {@link ContactListener} to add.
	 */
	void addContactListener(ContactListener listener);

	/**
	 * Removes a previously added contact listener.
	 *
	 * @param listener the {@link ContactListener} to remove.
	 */
	void removeContactListener(ContactListener listener);

	/**
	 * Adds a session listener to receive session-related events.
	 *
	 * @param listener the session listener to be added; cannot be null
	 */
	void addSessionListener(SessionListener listener);

	/**
	 * Removes a session listener from the list of listeners.
	 *
	 * @param listener the session listener to be removed
	 */
	void removeSessionListener(SessionListener listener);

	/**
	 * Adds a listener for friend request events.
	 *
	 * @param listener the {@link FriendRequestListener} to add.
	 */
	void addFriendRequestListener(FriendRequestListener listener);

	/**
	 * Removes a previously added friend request listener.
	 *
	 * @param listener the {@link FriendRequestListener} to remove.
	 */
	void removeFriendRequestListener(FriendRequestListener listener);

	/**
	 * Removes all listeners that have been previously registered with this instance.
	 * This method clears the internal collection of listeners, effectively
	 * detaching any observers or handlers that may have been added.
	 * After calling this method, no further events or notifications will be sent
	 * to the removed listeners.
	 */
	void removeAllListeners();

	////////////////////////////////////////////////////////////////////////////
	// Start and stop, status check
	////////////////////////////////////////////////////////////////////////////
	/**
	 * Starts the messaging client, initiating the connection and synchronization process.
	 * <p>
	 * This method is idempotent while the client is running, and a stopped client may be
	 * started again (the client is restartable).
	 *
	 * @return a {@link CompletableFuture} that completes when the client has started.
	 */
	CompletableFuture<Void> start();

	/**
	 * Stops the messaging client and releases all associated resources.
	 * <p>
	 * This method is idempotent. A stopped client may be started again via {@link #start()}.
	 *
	 * @return a {@link CompletableFuture} that completes when the client has stopped.
	 */
	CompletableFuture<Void> stop();

	/**
	 * Checks if the messaging client is currently running.
	 *
	 * @return {@code true} if running; {@code false} otherwise.
	 */
	boolean isRunning();

	/**
	 * Checks if the messaging client is currently connected to the messaging service.
	 *
	 * @return {@code true} if connected; {@code false} otherwise.
	 */
	boolean isConnected();

	/**
	 * Checks if the messaging client is ready to send and receive messages.
	 *
	 * @return true if the messaging client is ready; false otherwise.
	 */
	boolean isReady();

	////////////////////////////////////////////////////////////////////////////
	// Message and conversation APIs
	////////////////////////////////////////////////////////////////////////////
	/**
	 * Creates a new {@link Message.Builder} for composing and sending a message.
	 *
	 * @return a new message builder.
	 */
	default Message.Builder message() {
		return message(null);
	}

	/**
	 * Creates a new {@link Message.Builder} for composing and sending a message to a specific recipient.
	 *
	 * @param recipient the identifier of the recipient.
	 * @return a new message builder.
	 */
	Message.Builder message(@Nullable Id recipient);

	/**
	 * Retrieves a specific conversation by its identifier.
	 *
	 * @param conversationId the identifier of the conversation.
	 * @return a {@link CompletableFuture} that completes with an {@link Optional} holding the
	 *         {@link Conversation}, or an empty {@code Optional} if no conversation exists for
	 *         the given identifier.
	 */
	CompletableFuture<Optional<Conversation>> getConversation(Id conversationId);

	/**
	 * Retrieves all conversations for the local user.
	 *
	 * @return a {@link CompletableFuture} that will be completed with the list of {@link Conversation}s.
	 */
	CompletableFuture<List<Conversation>> getConversations();

	/**
	 * Removes a conversation and all its associated messages.
	 *
	 * @param conversationId the identifier of the conversation to remove.
	 * @return a {@link CompletableFuture} that completes with {@code true} if at least one matching
	 *         entry was removed, or {@code false} if nothing was removed because no matching entry
	 *         existed. The future completes exceptionally if the operation itself fails.
	 */
	default CompletableFuture<Boolean> removeConversation(Id conversationId) {
		return removeConversations(List.of(conversationId));
	}

	/**
	 * Removes multiple conversations and all their associated messages.
	 *
	 * @param conversationIds the identifiers of the conversations to remove.
	 * @return a {@link CompletableFuture} that completes with {@code true} if at least one matching
	 *         entry was removed, or {@code false} if nothing was removed because no matching entry
	 *         existed. The future completes exceptionally if the operation itself fails.
	 */
	CompletableFuture<Boolean> removeConversations(Collection<Id> conversationIds);

	/**
	 * Retrieves a list of messages for a specific conversation using default limits.
	 *
	 * @param conversationId the identifier of the conversation.
	 * @return a {@link CompletableFuture} that will be completed with the list of {@link Message}s.
	 */
	default CompletableFuture<List<Message>> getMessages(Id conversationId) {
		return getMessagesBefore(conversationId, System.currentTimeMillis(), DEFAULT_MESSAGES_LIMIT, 0);
	}

	/**
	 * Retrieves a list of messages for a specific conversation with pagination support.
	 *
	 * @param conversationId the identifier of the conversation.
	 * @param until A timestamp indicating the maximum message creation time (inclusive) for the retrieved messages.
	 * @param limit the maximum number of messages to retrieve.
	 * @param offset the number of messages to skip.
	 * @return a {@link CompletableFuture} that will be completed with the list of {@link Message}s.
	 */
	CompletableFuture<List<Message>> getMessagesBefore(Id conversationId, long until, int limit, int offset);

	/**
	 * Retrieves a list of messages for a specific conversation within a time range.
	 *
	 * @param conversationId the identifier of the conversation.
	 * @param begin the start timestamp (inclusive).
	 * @param end the end timestamp (inclusive).
	 * @return a {@link CompletableFuture} that will be completed with the list of {@link Message}s.
	 */
	CompletableFuture<List<Message>> getMessagesInRange(Id conversationId, long begin, long end);

	/**
	 * Removes a specific message by its internal identifier.
	 *
	 * @param messageId the internal numeric identifier of the message.
	 * @return a {@link CompletableFuture} that completes with {@code true} if at least one matching
	 *         entry was removed, or {@code false} if nothing was removed because no matching entry
	 *         existed. The future completes exceptionally if the operation itself fails.
	 */
	default CompletableFuture<Boolean> removeMessage(long messageId) {
		return removeMessages(List.of(messageId));
	}

	/**
	 * Removes multiple specific messages by their internal identifiers.
	 *
	 * @param messageIds the internal numeric identifiers of the messages to remove.
	 * @return a {@link CompletableFuture} that completes with {@code true} if at least one matching
	 *         entry was removed, or {@code false} if nothing was removed because no matching entry
	 *         existed. The future completes exceptionally if the operation itself fails.
	 */
	CompletableFuture<Boolean> removeMessages(Collection<Long> messageIds);

	/**
	 * Removes all messages associated with a specific conversation.
	 *
	 * @param conversationId the identifier of the conversation.
	 * @return a {@link CompletableFuture} that completes with {@code true} if at least one matching
	 *         entry was removed, or {@code false} if nothing was removed because no matching entry
	 *         existed. The future completes exceptionally if the operation itself fails.
	 */
	CompletableFuture<Boolean> removeMessages(Id conversationId);

	////////////////////////////////////////////////////////////////////////////
	// Session APIs
	////////////////////////////////////////////////////////////////////////////
	/**
	 * Retrieves all active sessions for the current user across all devices.
	 *
	 * @return a {@link CompletableFuture} that will be completed with the list of {@link SessionInfo}.
	 */
	CompletableFuture<List<SessionInfo>> getSessions();

	/**
	 * Revokes a specific device session.
	 *
	 * @param deviceId the identifier of the device session to revoke.
	 * @return a {@link CompletableFuture} that completes when the session has been revoked.
	 */
	CompletableFuture<Void> revokeSession(Id deviceId);

	////////////////////////////////////////////////////////////////////////////
	// Friend APIs
	////////////////////////////////////////////////////////////////////////////
	/**
	 * Sends a friend request to a specific user.
	 * <p>
	 * There is at most one friend request record per user: sending replaces any record already
	 * kept for that user, whatever its direction or state, on both sides.
	 * </p>
	 *
	 * @param id the identifier of the user to send the request to.
	 * @param hello an optional greeting message.
	 * @return a {@link CompletableFuture} that completes when the request is sent.
	 */
	CompletableFuture<Void> friendRequest(Id id, String hello);

	/**
	 * Accepts an incoming friend request from a specific user.
	 * <p>
	 * Only an incoming request that is still pending can be accepted; the record is kept, marked
	 * accepted, and the sender is added as a friend. Ignoring a request needs no call at all: the
	 * record stays as it is and can be accepted later while it has not expired.
	 * </p>
	 *
	 * @param id the identifier of the user who sent the request.
	 * @return a {@link CompletableFuture} that completes when the request is accepted, or fails with
	 *         {@link IllegalArgumentException} if there is no request from that user, or with
	 *         {@link IllegalStateException} if the request is outgoing, already accepted or expired
	 *         (the record is left unchanged).
	 */
	CompletableFuture<Void> acceptFriendRequest(Id id);

	/**
	 * Retrieves the details of a specific friend request.
	 *
	 * @param id the identifier of the user associated with the request.
	 * @return a {@link CompletableFuture} that completes with an {@link Optional} holding the
	 *         {@link FriendRequest}, or an empty {@code Optional} if no friend request exists
	 *         for the given user.
	 */
	CompletableFuture<Optional<FriendRequest>> getFriendRequest(Id id);

	/**
	 * Retrieves all friend requests.
	 * <p>
	 * Every stored record is returned, incoming and outgoing, pending, accepted and expired alike;
	 * a record only goes away when it is removed or replaced by a new request.
	 * </p>
	 *
	 * @return a {@link CompletableFuture} that will be completed with the list of {@link FriendRequest}s.
	 */
	CompletableFuture<List<FriendRequest>> getFriendRequests();

	/**
	 * Removes a friend request.
	 *
	 * @param id the identifier of the user associated with the request to remove.
	 * @return a {@link CompletableFuture} that completes with {@code true} if at least one matching
	 *         entry was removed, or {@code false} if nothing was removed because no matching entry
	 *         existed. The future completes exceptionally if the operation itself fails.
	 */
	CompletableFuture<Boolean> removeFriendRequest(Id id);

	/**
	 * Removes multiple friend requests.
	 *
	 * @param ids the identifiers of the users associated with the requests to remove.
	 * @return a {@link CompletableFuture} that completes with {@code true} if at least one matching
	 *         entry was removed, or {@code false} if nothing was removed because no matching entry
	 *         existed. The future completes exceptionally if the operation itself fails.
	 */
	CompletableFuture<Boolean> removeFriendRequests(Collection<Id> ids);

	/**
	 * Clears all friend requests.
	 *
	 * @return a {@link CompletableFuture} that completes when all requests are cleared.
	 */
	CompletableFuture<Void> clearFriendRequests();

	/**
	 * Adds a user as a friend manually using their session key.
	 *
	 * @param id the identifier of the user.
	 * @param sessionKey the shared session key for the contact.
	 * @return a {@link CompletableFuture} that will be completed with the created {@link Contact}.
	 */
	default CompletableFuture<Contact> addFriend(Id id, byte[] sessionKey) {
		return addFriend(id, sessionKey, null);
	}

	/**
	 * Adds a user as a friend manually using their session key and an optional remark.
	 *
	 * @param id the identifier of the user.
	 * @param sessionKey the shared session key for the contact.
	 * @param remark an optional remark or alias for the friend.
	 * @return a {@link CompletableFuture} that will be completed with the created {@link Contact}.
	 */
	CompletableFuture<Contact> addFriend(Id id, byte[] sessionKey, @Nullable String remark);

	/**
	 * Blocks a user, whether or not they are a contact.
	 * <p>
	 * Blocking uses the contact's existing blocked state: a friend is updated to blocked, and a user who
	 * is not a contact yet gets a new {@link Contact.Type#AUTO AUTO} contact marked blocked. Either way
	 * the change goes through contact synchronization, so all of the user's devices honour it. From
	 * then on, friend requests, friend request acceptances and direct messages from the blocked user are
	 * dropped. Their messages in channels are still delivered, since a channel is a conversation shared
	 * with its other members. Friend request records already stored are left as they are.
	 * </p>
	 * <p>
	 * Unblocking is an ordinary contact update: {@code updateContact(contact.edit().setBlocked(false).build())}.
	 * </p>
	 *
	 * @param id the identifier of the user to block.
	 * @return a {@link CompletableFuture} that completes with the blocked contact (the existing one if
	 *         the user was already blocked), or fails with {@link IllegalArgumentException} if the id is
	 *         the current user's own or a channel's.
	 */
	CompletableFuture<Contact> blockUser(Id id);

	////////////////////////////////////////////////////////////////////////////
	// channel APIs
	////////////////////////////////////////////////////////////////////////////
	/**
	 * Creates a new channel with default settings.
	 *
	 * @param name the name of the channel.
	 * @return a {@link CompletableFuture} that will be completed with the created {@link Channel}.
	 */
	default CompletableFuture<Channel> createChannel(String name) {
		return createChannel(Channel.Permission.OWNER_INVITE, name, null, true);
	}

	/**
	 * Creates a new channel with a specific name and notice.
	 *
	 * @param name the name of the channel.
	 * @param notice the channel notice or description.
	 * @return a {@link CompletableFuture} that will be completed with the created {@link Channel}.
	 */
	default CompletableFuture<Channel> createChannel(String name, String notice) {
		return createChannel(Channel.Permission.OWNER_INVITE, name, notice, true);
	}

	/**
	 * Creates a new channel with advanced configuration.
	 *
	 * @param permission the join permission level for the channel.
	 * @param name the name of the channel.
	 * @param notice the channel notice or description.
	 * @param announce whether to announce the channel to the network.
	 * @return a {@link CompletableFuture} that will be completed with the created {@link Channel}.
	 */
	CompletableFuture<Channel> createChannel(Channel.Permission permission, String name, @Nullable String notice, boolean announce);

	/**
	 * Removes a channel. Only the owner can remove a channel.
	 *
	 * @param channelId the identifier of the channel to remove.
	 * @return a {@link CompletableFuture} that completes with {@code true} if at least one matching
	 *         entry was removed, or {@code false} if nothing was removed because no matching entry
	 *         existed. The future completes exceptionally if the operation itself fails.
	 */
	CompletableFuture<Boolean> removeChannel(Id channelId);

	/**
	 * Joins a channel using an invitation ticket.
	 *
	 * @param ticket the {@link InviteTicket} used to join the channel.
	 * @return a {@link CompletableFuture} that will be completed with the joined {@link Channel}.
	 */
	CompletableFuture<Channel> joinChannel(InviteTicket ticket);

	/**
	 * Leaves a channel.
	 *
	 * @param channelId the identifier of the channel to leave.
	 * @return a {@link CompletableFuture} that completes with {@code true} if at least one matching
	 *         entry was removed, or {@code false} if nothing was removed because no matching entry
	 *         existed. The future completes exceptionally if the operation itself fails.
	 */
	CompletableFuture<Boolean> leaveChannel(Id channelId);

	/**
	 * Creates an invitation ticket for a channel that can be used by anyone.
	 *
	 * @param channelId the identifier of the channel.
	 * @return a {@link CompletableFuture} that will be completed with the {@link InviteTicket}.
	 */
	default CompletableFuture<InviteTicket> createInviteTicket(Id channelId) {
		return createInviteTicket(channelId, null);
	}

	/**
	 * Creates an invitation ticket for a channel targeting a specific user.
	 *
	 * @param channelId the identifier of the channel.
	 * @param invitee the identifier of the target user.
	 * @return a {@link CompletableFuture} that will be completed with the {@link InviteTicket}.
	 */
	CompletableFuture<InviteTicket> createInviteTicket(Id channelId, @Nullable Id invitee);

	/**
	 * Transfers the ownership of a channel to another user.
	 *
	 * @param channelId the identifier of the channel.
	 * @param newOwner the identifier of the new owner.
	 * @return a {@link CompletableFuture} that completes when the ownership has been transferred.
	 */
	CompletableFuture<Void> transferChannelOwnership(Id channelId, Id newOwner);

	/**
	 * Rotates the session key for a channel using a randomly generated key pair.
	 *
	 * @param channelId the identifier of the channel.
	 * @return a {@link CompletableFuture} that completes when the key has been rotated.
	 */
	default CompletableFuture<Void> rotateChannelSessionKey(Id channelId) {
		return rotateChannelSessionKey(channelId, Signature.KeyPair.random());
	}

	/**
	 * Rotates the session key for a channel using the specified key pair.
	 *
	 * @param channelId the identifier of the channel.
	 * @param sessionKeypair the new key pair to use for the channel session.
	 * @return a {@link CompletableFuture} that completes when the key has been rotated.
	 */
	CompletableFuture<Void> rotateChannelSessionKey(Id channelId, Signature.KeyPair sessionKeypair);

	/**
	 * Updates the channel metadata (e.g., name, notice).
	 *
	 * @param channel the {@link Channel} object containing updated information.
	 * @return a {@link CompletableFuture} that completes when the update is finished.
	 */
	CompletableFuture<Void> updateChannelInfo(Channel channel);

	/**
	 * Sets the role (e.g., MODERATOR, MEMBER) for specific members of a channel.
	 *
	 * @param channelId the identifier of the channel.
	 * @param members the list of member identifiers.
	 * @param role the new role to be assigned.
	 * @return a {@link CompletableFuture} that completes when the roles have been updated.
	 */
	CompletableFuture<Void> setChannelMembersRole(Id channelId, Collection<Id> members, Channel.Role role);

	/**
	 * Bans specific members from a channel.
	 *
	 * @param channelId the identifier of the channel.
	 * @param members the list of identifiers of members to ban.
	 * @return a {@link CompletableFuture} that completes when the members have been banned.
	 */
	CompletableFuture<Void> banChannelMembers(Id channelId, Collection<Id> members);

	/**
	 * Unbans specific members from a channel.
	 *
	 * @param channelId the identifier of the channel.
	 * @param members the list of identifiers of members to unban.
	 * @return a {@link CompletableFuture} that completes when the members have been unbanned.
	 */
	CompletableFuture<Void> unbanChannelMembers(Id channelId, Collection<Id> members);

	/**
	 * Removes (kicks) specific members from a channel.
	 *
	 * @param channelId the identifier of the channel.
	 * @param members the list of identifiers of members to remove.
	 * @return a {@link CompletableFuture} that completes when the members have been removed.
	 */
	CompletableFuture<Void> removeChannelMembers(Id channelId, Collection<Id> members);

	////////////////////////////////////////////////////////////////////////////
	// Generic contact APIs
	////////////////////////////////////////////////////////////////////////////
	/**
	 * Retrieves a generic contact (friend or channel) by its identifier.
	 *
	 * @param contactId the identifier of the contact.
	 * @return a {@link CompletableFuture} that completes with an {@link Optional} holding the
	 *         {@link Contact}, or an empty {@code Optional} if no contact exists for the given
	 *         identifier.
	 */
	CompletableFuture<Optional<Contact>> getContact(Id contactId);

	/**
	 * Retrieves all generic contacts.
	 *
	 * @return a {@link CompletableFuture} that will be completed with the list of {@link Contact}s.
	 */
	CompletableFuture<List<Contact>> getContacts();

	/**
	 * Updates the settings of a generic contact (e.g., remark, muted status).
	 *
	 * @param contact the {@link Contact} object containing updated information.
	 * @return a {@link CompletableFuture} that will be completed with the updated contact object.
	 */
	CompletableFuture<Contact> updateContact(Contact contact);

	/**
	 * Removes a specific contact.
	 *
	 * @param contactId the identifier of the contact to remove.
	 * @return a {@link CompletableFuture} that completes with {@code true} if at least one matching
	 *         entry was removed, or {@code false} if nothing was removed because no matching entry
	 *         existed. The future completes exceptionally if the operation itself fails.
	 */
	default CompletableFuture<Boolean> removeContact(Id contactId) {
		return removeContacts(List.of(contactId));
	}

	/**
	 * Removes multiple contacts.
	 *
	 * @param contactIds the identifiers of the contacts to remove.
	 * @return a {@link CompletableFuture} that completes with {@code true} if at least one matching
	 *         entry was removed, or {@code false} if nothing was removed because no matching entry
	 *         existed. The future completes exceptionally if the operation itself fails.
	 */
	CompletableFuture<Boolean> removeContacts(Collection<Id> contactIds);

	/**
	 * Clears all generic contacts.
	 *
	 * @return a {@link CompletableFuture} that completes when all contacts are cleared.
	 */
	CompletableFuture<Void> clearContacts();

	/**
	 * Creates a new {@link MessagingClient} instance.
	 *
	 * <p>A Vert.x instance is mandatory: it is taken from {@code vertx} when non-null,
	 * otherwise from {@code node}, otherwise from the current Vert.x context. If none of
	 * these can supply a Vert.x instance, construction fails fast with an
	 * {@link IllegalArgumentException}.
	 *
	 * @param vertx the {@link Vertx} instance to use for asynchronous operations, or
	 *        {@code null} to derive one from {@code node} or the current Vert.x context.
	 * @param node the {@link Node} instance representing the local DHT node, or {@code null}
	 *        when the {@link Configuration} carries a fixed service endpoint.
	 * @param config the {@link Configuration} settings for the client.
	 * @return a new {@link MessagingClient} instance.
	 * @throws IllegalArgumentException if no Vert.x instance can be resolved.
	 */
	static MessagingClient create(@Nullable Vertx vertx, @Nullable Node node, Configuration config) {
		return new PhotonMessagingClient(vertx, node, config);
	}

	/**
	 * Creates a new {@link MessagingClient} instance without an explicit Vertx.
	 *
	 * <p>The Vert.x instance is derived from {@code node} (or the current Vert.x context); a
	 * usable Vert.x source is therefore required. Construction fails fast with an
	 * {@link IllegalArgumentException} if none is available.
	 *
	 * @param node the {@link Node} instance representing the local DHT node.
	 * @param config the {@link Configuration} settings for the client.
	 * @return a new {@link MessagingClient} instance.
	 * @throws IllegalArgumentException if no Vert.x instance can be resolved.
	 */
	static MessagingClient create(@Nullable Node node, Configuration config) {
		return new PhotonMessagingClient(null, node, config);
	}
}