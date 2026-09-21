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

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.jspecify.annotations.Nullable;

import io.bosonnetwork.Id;
import io.bosonnetwork.photonmessaging.Channel;
import io.bosonnetwork.photonmessaging.Contact;
import io.bosonnetwork.photonmessaging.ContentDisposition;
import io.bosonnetwork.photonmessaging.Conversation;
import io.bosonnetwork.photonmessaging.FriendRequest;
import io.bosonnetwork.photonmessaging.Message;
import io.bosonnetwork.photonmessaging.MessagingStore;
import io.bosonnetwork.photonmessaging.MessagingStore.StoredChannel;
import io.bosonnetwork.photonmessaging.MessagingStore.StoredChannelMember;
import io.bosonnetwork.photonmessaging.MessagingStore.StoredContact;
import io.bosonnetwork.photonmessaging.MessagingStore.StoredConversation;
import io.bosonnetwork.photonmessaging.MessagingStore.StoredFriendRequest;
import io.bosonnetwork.photonmessaging.MessagingStore.StoredMessage;

/**
 * The internal messaging repository, expressed in impl domain types. It is implemented by adapting a
 * {@link MessagingStore} (expressed in plain data records): every operation translates the impl types
 * to/from the store's records. Because this class lives in the {@code impl} package, it can construct
 * the impl domain types and holds the {@code sessionContextFactory} needed to build conversations.
 *
 * <p>The same instance backs both supported store flavors: a platform-supplied {@link MessagingStore}
 * and the built-in {@link DatabaseStore}. The record &lt;-&gt; impl mapping mirrors {@code DatabaseStore}'s
 * {@code rowToX}/{@code paramsFromX} helpers exactly, so the two backends are behaviorally interchangeable.
 */
class MessagingRepository {
	private final MessagingStore store;
	private @Nullable Function<PhotonContact, SessionContext> sessionContextFactory;

	MessagingRepository(MessagingStore store) {
		this.store = Objects.requireNonNull(store, "store");
	}

	public Future<Integer> initialize(Vertx vertx, Function<PhotonContact, SessionContext> sessionContextFactory) {
		this.sessionContextFactory = Objects.requireNonNull(sessionContextFactory);
		if (store instanceof DatabaseStore dbStore)
			return dbStore.open(vertx);
		else
			return store.open().map(v -> 1);
	}

	public Future<Void> close() {
		return store.close();
	}

	// ------------------------------------------------------------------------
	// Messages
	// ------------------------------------------------------------------------

	public Future<Void> putMessage(PhotonMessage<MessageContent> message) {
		return store.putMessage(toRecord(message)).map(rid -> {
			message.setRid(rid);
			return null;
		});
	}

	public Future<Void> updateMessageSentTime(PhotonMessage<MessageContent> message) {
		return store.updateMessageSentTime(message.getId(), message.getSentAt());
	}

	public Future<List<PhotonMessage<MessageContent>>> getMessagesInRange(Id conversationId, long begin, long end) {
		return store.getMessagesInRange(conversationId, begin, end).map(this::toMessages);
	}

	public Future<List<PhotonMessage<MessageContent>>> getMessagesBefore(Id conversationId, long until, int limit, int offset) {
		return store.getMessagesBefore(conversationId, until, limit, offset).map(this::toMessages);
	}

	public Future<Boolean> removeMessage(long id) {
		return store.removeMessage(id);
	}

	public Future<Boolean> removeMessages(Collection<Long> rids) {
		return store.removeMessages(rids);
	}

	public Future<Boolean> removeMessagesByConversation(Id conversationId) {
		return store.removeMessagesByConversation(conversationId);
	}

	public Future<Void> clearMessages() {
		return store.clearMessages();
	}

	// ------------------------------------------------------------------------
	// Friend requests
	// ------------------------------------------------------------------------

	public Future<Void> putFriendRequest(FriendRequest friendRequest) {
		return store.putFriendRequest(toRecord((PhotonFriendRequest) friendRequest));
	}

	public Future<@Nullable FriendRequest> getFriendRequest(Id userId) {
		return store.getFriendRequest(userId).<@Nullable FriendRequest>map(r -> r == null ? null : toFriendRequest(r));
	}

	public Future<List<FriendRequest>> getFriendRequests() {
		return store.getFriendRequests()
				.map(list -> list.stream().<FriendRequest>map(this::toFriendRequest).collect(Collectors.toList()));
	}

	public Future<Boolean> removeFriendRequest(Id userId) {
		return store.removeFriendRequest(userId);
	}

	public Future<Boolean> removeFriendRequests(Collection<Id> userIds) {
		return store.removeFriendRequests(userIds);
	}

	public Future<Void> clearFriendRequests() {
		return store.clearFriendRequests();
	}

	// ------------------------------------------------------------------------
	// Conversations
	// ------------------------------------------------------------------------

	public Future<@Nullable Conversation> getConversation(Id conversationId) {
		return store.getConversation(conversationId).<@Nullable Conversation>map(r -> r == null ? null : toConversation(r));
	}

	public Future<List<Conversation>> getAllConversations() {
		return store.getAllConversations()
				.map(list -> list.stream().<Conversation>map(this::toConversation).collect(Collectors.toList()));
	}

	public Future<Boolean> removeConversation(Id conversationId) {
		return store.removeConversation(conversationId);
	}

	public Future<Boolean> removeConversations(Collection<Id> conversationIds) {
		return store.removeConversations(conversationIds);
	}

	// ------------------------------------------------------------------------
	// Contacts
	// ------------------------------------------------------------------------

	public Future<Integer> getContactsRevision() {
		return store.getContactsRevision();
	}

	public Future<Void> putContactLocally(Contact contact) {
		return store.putContactLocally(toRecord(contact))
				.andThen(ar -> wireMembersLoader(contact));
	}

	public Future<Boolean> removeContactLocally(Id contactId) {
		return store.removeContactLocally(contactId);
	}

	public Future<Void> putContact(int revision, Contact contact) {
		return store.putContact(revision, toRecord(contact))
				.andThen(ar -> wireMembersLoader(contact));
	}

	public Future<Void> putContacts(int revision, Collection<Contact> updated) {
		return store.putContacts(revision, updated.stream().map(this::toRecord).collect(Collectors.toList()))
				.andThen(ar -> updated.forEach(this::wireMembersLoader));
	}

	public Future<Boolean> removeContact(int revision, Id contactId) {
		return store.removeContact(revision, contactId);
	}

	public Future<Boolean> removeContacts(int revision, Collection<Id> contactIds) {
		return store.removeContacts(revision, contactIds);
	}

	public Future<Void> clearContacts(int revision) {
		return store.clearContacts(revision);
	}

	public Future<@Nullable Contact> getContact(Id contactId) {
		return store.getContact(contactId).<@Nullable Contact>map(r -> r == null ? null : toContact(r));
	}

	public Future<List<Contact>> getContacts(Collection<Id> contactIds) {
		return store.getContacts(contactIds)
				.map(list -> list.stream().<Contact>map(this::toContact).collect(Collectors.toList()));
	}

	public Future<List<Contact>> getAllContacts() {
		return store.getAllContacts()
				.map(list -> list.stream().<Contact>map(this::toContact).collect(Collectors.toList()));
	}

	public Future<Boolean> existsContact(Id contactId) {
		return store.existsContact(contactId);
	}

	// ------------------------------------------------------------------------
	// Channel members
	// ------------------------------------------------------------------------

	public Future<Void> updateChannelOwnership(Id channelId, Id oldOwnerId, Id newOwnerId) {
		return store.updateChannelOwnership(channelId, oldOwnerId, newOwnerId);
	}

	public Future<Void> putChannelMember(Id channelId, Channel.Member member) {
		return store.putChannelMember(channelId, toMemberRecord(channelId, member));
	}

	public Future<Void> putChannelMembers(Id channelId, Collection<Channel.Member> members) {
		return store.putChannelMembers(channelId, toMemberRecords(channelId, members));
	}

	public Future<Void> refillChannelMembers(Id channelId, Collection<Channel.Member> members) {
		return store.refillChannelMembers(channelId, toMemberRecords(channelId, members));
	}

	public Future<Channel.@Nullable Member> getChannelMember(Id channelId, Id memberId) {
		return store.getChannelMember(channelId, memberId).<Channel.@Nullable Member>map(r -> r == null ? null : toMember(r));
	}

	public Future<List<Channel.Member>> getChannelMembers(Id channelId, Collection<Id> memberIds) {
		return store.getChannelMembers(channelId, memberIds)
				.map(list -> list.stream().<Channel.Member>map(this::toMember).collect(Collectors.toList()));
	}

	public Future<List<Channel.Member>> getAllChannelMembers(Id channelId) {
		return store.getAllChannelMembers(channelId)
				.map(list -> list.stream().<Channel.Member>map(this::toMember).collect(Collectors.toList()));
	}

	public Future<Boolean> updateChannelMembersRole(Id channelId, Collection<Id> memberIds, Channel.Role role) {
		return store.updateChannelMembersRole(channelId, memberIds, role.value());
	}

	public Future<Boolean> removeChannelMember(Id channelId, Id memberId) {
		return store.removeChannelMember(channelId, memberId);
	}

	public Future<Boolean> removeChannelMembers(Id channelId, Collection<Id> memberIds) {
		return store.removeChannelMembers(channelId, memberIds);
	}

	// ------------------------------------------------------------------------
	// Mapping: impl -> record
	// ------------------------------------------------------------------------

	private StoredMessage toRecord(PhotonMessage<MessageContent> message) {
		MessageContent content = message.getPayload();
		return new StoredMessage(
				message.getRid(),
				message.getId(),
				message.getConversationId().orElseThrow(
						() -> new IllegalArgumentException("Message has no conversation id")),
				message.getVersion(),
				message.getRecipient(),
				message.getType().value(),
				message.getFrom().orElse(null),
				message.getCreatedAt(),
				content.getContentType(),
				content.getContentDisposition().map(ContentDisposition::getValue).orElse(null),
				content.serialize(),
				message.getSentAt(),
				message.getReceivedAt());
	}

	private StoredContact toRecord(Contact contact) {
		StoredChannel channelRecord = null;
		if (contact instanceof Channel channel) {
			channelRecord = new StoredChannel(
					channel.getId(),
					channel.getOwnerId(),
					channel.getPermission().value(),
					channel.getNotice().orElse(null),
					channel.isAnnounce());
		}
		byte[] sessionKey = contact instanceof PhotonContact pc ? pc.getSessionKey() : null;
		return new StoredContact(
				contact.getId(),
				contact.getType().value(),
				sessionKey,
				contact.getName().orElse(null),
				contact.getRemark().orElse(null),
				contact.getTags().orElse(null),
				contact.isMuted(),
				contact.isBlocked(),
				contact.getRevision(),
				contact.getCreatedAt(),
				contact.getUpdatedAt(),
				channelRecord);
	}

	private StoredFriendRequest toRecord(PhotonFriendRequest request) {
		return new StoredFriendRequest(
				request.getUserId(),
				request.getInitiatorId(),
				request.getHello(),
				request.getCreatedAt(),
				request.getUpdatedAt(),
				request.isAccepted(),
				request.getAcceptedAt());
	}

	private StoredChannelMember toMemberRecord(Id channelId, Channel.Member member) {
		return new StoredChannelMember(member.getId(), channelId, member.getRole().value(), member.getJoined());
	}

	private List<StoredChannelMember> toMemberRecords(Id channelId, Collection<Channel.Member> members) {
		return members.stream()
				.map(m -> toMemberRecord(channelId, m))
				.collect(Collectors.toList());
	}

	// ------------------------------------------------------------------------
	// Mapping: record -> impl
	// ------------------------------------------------------------------------

	private List<PhotonMessage<MessageContent>> toMessages(List<StoredMessage> records) {
		return records.stream().map(this::toMessage).collect(Collectors.toList());
	}

	private PhotonMessage<MessageContent> toMessage(StoredMessage r) {
		MessageContent content = r.payload() == null ? MessageContent.EMPTY : MessageContent.parse(r.payload());
		return new PhotonMessage<>(r.rid(), r.conversationId(), r.version(), r.id(), r.recipient(),
				Message.Type.valueOf(r.type()), r.from(), r.createdAt(), content, r.sentAt(), r.receivedAt());
	}

	// Several contact column values (name/remark/tags/notice) are nullable in the schema, but a
	// few impl constructors do not annotate those parameters @Nullable; the nulls are valid and stored
	// as-is. Mirrors DatabaseStore.rowToContact, which relied on Vert.x's non-null-typed Row accessors.
	@SuppressWarnings("NullAway")
	private Contact toContact(StoredContact r) {
		Contact.Type type = Contact.Type.valueOf(r.type());
		return switch (type) {
			case FRIEND -> new Friend(r.id(), Objects.requireNonNull(r.sessionKey()), r.name(),
					r.remark(), r.tags(), r.muted(), r.blocked(), r.createdAt(), r.updatedAt(), r.revision());
			case CHANNEL -> {
				StoredChannel ch = Objects.requireNonNull(r.channel(), "channel record missing for CHANNEL contact");
				PhotonChannel channel = new PhotonChannel(r.id(), Objects.requireNonNull(r.sessionKey()),
						ch.owner(), Channel.Permission.valueOf(ch.permission()), r.name(), ch.notice(), ch.announce(),
						r.remark(), r.tags(), r.muted(), r.blocked(), r.createdAt(), r.updatedAt(), r.revision());
				channel.setMembersLoader(this::loadMembers);
				yield channel;
			}
			case AUTO -> new AutoContact(r.id(), r.name(), r.remark(), r.tags(),
					r.muted(), r.blocked(), r.createdAt(), r.updatedAt());
		};
	}

	private PhotonFriendRequest toFriendRequest(StoredFriendRequest r) {
		return new PhotonFriendRequest(r.id(), r.initiator(), r.hello(), r.createdAt(), r.updatedAt(),
				r.accepted(), r.acceptedAt());
	}

	private ChannelMember toMember(StoredChannelMember r) {
		return new ChannelMember(r.id(), Channel.Role.valueOf(r.role()), r.joined());
	}

	private Conversation toConversation(StoredConversation r) {
		Function<PhotonContact, SessionContext> factory = Objects.requireNonNull(sessionContextFactory,
				"INTERNAL ERROR: sessionContextFactory is null");
		PhotonContact contact = (PhotonContact) toContact(r.contact());
		StoredMessage last = r.lastMessage();
		PhotonMessage<MessageContent> lastMessage = last == null ? null : toMessage(last);
		return new PhotonConversation(contact, lastMessage, factory);
	}

	// Wires the lazy members loader on a channel contact so callers holding the supplied instance can
	// resolve its members on demand. Done unconditionally (like the legacy Database, which wired it
	// while building the upsert params): the loader merely enables lazy reads and is harmless even if
	// the underlying write failed.
	private void wireMembersLoader(Contact contact) {
		if (contact instanceof PhotonChannel pc)
			pc.setMembersLoader(this::loadMembers);
	}

	private Future<List<ChannelMember>> loadMembers(Id channelId) {
		return store.getAllChannelMembers(channelId)
				.map(list -> list.stream().map(this::toMember).collect(Collectors.toList()));
	}
}