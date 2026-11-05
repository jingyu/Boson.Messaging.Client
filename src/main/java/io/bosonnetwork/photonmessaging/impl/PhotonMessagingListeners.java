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

import java.util.List;
import java.util.Objects;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.bosonnetwork.Id;
import io.bosonnetwork.photonmessaging.Channel;
import io.bosonnetwork.photonmessaging.ChannelListener;
import io.bosonnetwork.photonmessaging.ConnectionListener;
import io.bosonnetwork.photonmessaging.Contact;
import io.bosonnetwork.photonmessaging.ContactListener;
import io.bosonnetwork.photonmessaging.FriendRequestListener;
import io.bosonnetwork.photonmessaging.Message;
import io.bosonnetwork.photonmessaging.MessageDeliveryReceipt;
import io.bosonnetwork.photonmessaging.MessageListener;
import io.bosonnetwork.photonmessaging.SessionInfo;
import io.bosonnetwork.photonmessaging.SessionListener;

public class PhotonMessagingListeners implements ConnectionListener, MessageListener, ContactListener, ChannelListener, SessionListener, FriendRequestListener {
	// Listeners are stored in the CopyOnWriteArrayList-backed *ListenerArray wrappers (even a
	// single listener), so dispatch is always exception-isolated (one bad listener cannot break
	// the pipeline). Fields are volatile so the event-loop reader sees the latest reference;
	// add/remove mutate them under listenersLock so registration (which may happen on any
	// thread, including before start() when no Vert.x context exists yet) is race-free.
	private final Object listenersLock = new Object();
	private @Nullable volatile ConnectionListener connectionListener;
	private @Nullable volatile MessageListener messageListener;
	private @Nullable volatile ContactListener contactListener;
	private @Nullable volatile ChannelListener channelListener;
	private @Nullable volatile SessionListener sessionListener;
	private @Nullable volatile FriendRequestListener friendRequestListener;

	private static final Logger log = LoggerFactory.getLogger(PhotonMessagingListeners.class);

	public void addConnectionListener(ConnectionListener listener) {
		Objects.requireNonNull(listener, "listener");
		synchronized (listenersLock) {
			if (connectionListener instanceof ConnectionListenerArray listeners)
				listeners.add(listener);
			else
				connectionListener = new ConnectionListenerArray(listener, log);
		}
	}

	public void removeConnectionListener(ConnectionListener listener) {
		Objects.requireNonNull(listener, "listener");
		synchronized (listenersLock) {
			if (connectionListener instanceof ConnectionListenerArray listeners) {
				listeners.remove(listener);
				if (listeners.isEmpty())
					connectionListener = null;
			}
		}
	}

	@Override
	public void onConnecting() {
		ConnectionListener listener = connectionListener;
		if (listener != null) {
			try {
				listener.onConnecting();
			} catch (Throwable t) {
				log.error("Error dispatching onConnecting to listener: {}", listener, t);
			}
		}
	}

	@Override
	public void onConnected() {
		ConnectionListener listener = connectionListener;
		if (listener != null) {
			try {
				listener.onConnected();
			} catch (Throwable t) {
				log.error("Error dispatching onConnected to listener: {}", listener, t);
			}
		}
	}

	@Override
	public void onContactSynced() {
		ConnectionListener listener = connectionListener;
		if (listener != null) {
			try {
				listener.onContactSynced();
			} catch (Throwable t) {
				log.error("Error dispatching onReady to listener: {}", listener, t);
			}
		}
	}

	@Override
	public void onDisconnected() {
		ConnectionListener listener = connectionListener;
		if (listener != null) {
			try {
				listener.onDisconnected();
			} catch (Throwable t) {
				log.error("Error dispatching onDisconnected to listener: {}", listener, t);
			}
		}
	}

	public void addMessageListener(MessageListener listener) {
		Objects.requireNonNull(listener, "listener");
		synchronized (listenersLock) {
			if (messageListener instanceof MessageListenerArray listeners)
				listeners.add(listener);
			else
				messageListener = new MessageListenerArray(listener, log);
		}
	}

	public void removeMessageListener(MessageListener listener) {
		Objects.requireNonNull(listener, "listener");
		synchronized (listenersLock) {
			if (messageListener instanceof MessageListenerArray listeners) {
				listeners.remove(listener);
				if (listeners.isEmpty())
					messageListener = null;
			}
		}
	}

	@Override
	public void onMessage(Message message) {
		MessageListener listener = messageListener;
		if (listener != null) {
			try {
				listener.onMessage(message);
			} catch (Throwable t) {
				log.error("Error dispatching onMessage to listener: {}", listener, t);
			}
		}
	}

	@Override
	public void onSent(Message message) {
		MessageListener listener = messageListener;
		if (listener != null) {
			try {
				listener.onSent(message);
			} catch (Throwable t) {
				log.error("Error dispatching onSent to listener: {}", listener, t);
			}
		}
	}

	@Override
	public void onDeliveryReceipt(MessageDeliveryReceipt receipt) {
		MessageListener listener = messageListener;
		if (listener != null) {
			try {
				listener.onDeliveryReceipt(receipt);
			} catch (Throwable t) {
				log.error("Error dispatching onDeliveryReceipt to listener: {}", listener, t);
			}
		}
	}

	public void addContactListener(ContactListener listener) {
		Objects.requireNonNull(listener, "listener");
		synchronized (listenersLock) {
			if (contactListener instanceof ContactListenerArray listeners)
				listeners.add(listener);
			else
				contactListener = new ContactListenerArray(listener, log);
		}
	}

	public void removeContactListener(ContactListener listener) {
		Objects.requireNonNull(listener, "listener");
		synchronized (listenersLock) {
			if (contactListener instanceof ContactListenerArray listeners) {
				listeners.remove(listener);
				if (listeners.isEmpty())
					contactListener = null;
			}
		}
	}

	@Override
	public void onContactAdded(Contact contact) {
		ContactListener listener = contactListener;
		if (listener != null) {
			try {
				listener.onContactAdded(contact);
			} catch (Throwable t) {
				log.error("Error dispatching onContactAdded to listener: {}", listener, t);
			}
		}
	}

	@Override
	public void onContactsUpdated(List<Contact> contacts) {
		ContactListener listener = contactListener;
		if (listener != null) {
			try {
				listener.onContactsUpdated(contacts);
			} catch (Throwable t) {
				log.error("Error dispatching onContactsUpdated to listener: {}", listener, t);
			}
		}
	}

	@Override
	public void onContactsRemoved(List<Id> contactIds) {
		ContactListener listener = contactListener;
		if (listener != null) {
			try {
				listener.onContactsRemoved(contactIds);
			} catch (Throwable t) {
				log.error("Error dispatching onContactsRemoved to listener: {}", listener, t);
			}
		}
	}

	@Override
	public void onContactsCleared() {
		ContactListener listener = contactListener;
		if (listener != null) {
			try {
				listener.onContactsCleared();
			} catch (Throwable t) {
				log.error("Error dispatching onContactsCleared to listener: {}", listener, t);
			}
		}
	}

	public void addChannelListener(ChannelListener listener) {
		Objects.requireNonNull(listener, "listener");
		synchronized (listenersLock) {
			if (channelListener instanceof ChannelListenerArray listeners)
				listeners.add(listener);
			else
				channelListener = new ChannelListenerArray(listener, log);
		}
	}

	public void removeChannelListener(ChannelListener listener) {
		Objects.requireNonNull(listener, "listener");
		synchronized (listenersLock) {
			if (channelListener instanceof ChannelListenerArray listeners) {
				listeners.remove(listener);
				if (listeners.isEmpty())
					channelListener = null;
			}
		}
	}

	@Override
	public void onChannelCreated(Channel channel) {
		ChannelListener listener = channelListener;
		if (listener != null) {
			try {
				listener.onChannelCreated(channel);
			} catch (Throwable t) {
				log.error("Error dispatching onChannelCreated to listener: {}", listener, t);
			}
		}
	}

	@Override
	public void onChannelDeleted(Channel channel) {
		ChannelListener listener = channelListener;
		if (listener != null) {
			try {
				listener.onChannelDeleted(channel);
			} catch (Throwable t) {
				log.error("Error dispatching onChannelDeleted to listener: {}", listener, t);
			}
		}
	}

	@Override
	public void onJoinedChannel(Channel channel) {
		ChannelListener listener = channelListener;
		if (listener != null) {
			try {
				listener.onJoinedChannel(channel);
			} catch (Throwable t) {
				log.error("Error dispatching onJoinedChannel to listener: {}", listener, t);
			}
		}
	}

	@Override
	public void onLeftChannel(Channel channel) {
		ChannelListener listener = channelListener;
		if (listener != null) {
			try {
				listener.onLeftChannel(channel);
			} catch (Throwable t) {
				log.error("Error dispatching onLeftChannel to listener: {}", listener, t);
			}
		}
	}

	@Override
	public void onChannelOwnershipTransferred(Channel channel, Id oldOwner, Id newOwner) {
		ChannelListener listener = channelListener;
		if (listener != null) {
			try {
				listener.onChannelOwnershipTransferred(channel, oldOwner, newOwner);
			} catch (Throwable t) {
				log.error("Error dispatching onChannelOwnershipTransferred to listener: {}", listener, t);
			}
		}
	}

	@Override
	public void onChannelSessionKeyRotated(Channel channel) {
		ChannelListener listener = channelListener;
		if (listener != null) {
			try {
				listener.onChannelSessionKeyRotated(channel);
			} catch (Throwable t) {
				log.error("Error dispatching onChannelSessionKeyRotated to listener: {}", listener, t);
			}
		}
	}

	@Override
	public void onChannelUpdated(Channel channel) {
		ChannelListener listener = channelListener;
		if (listener != null) {
			try {
				listener.onChannelUpdated(channel);
			} catch (Throwable t) {
				log.error("Error dispatching onChannelUpdated to listener: {}", listener, t);
			}
		}
	}

	@Override
	public void onChannelMemberJoined(Channel channel, Channel.Member member) {
		ChannelListener listener = channelListener;
		if (listener != null) {
			try {
				listener.onChannelMemberJoined(channel, member);
			} catch (Throwable t) {
				log.error("Error dispatching onChannelMemberJoined to listener: {}", listener, t);
			}
		}
	}

	@Override
	public void onChannelMemberLeft(Channel channel, Channel.Member member) {
		ChannelListener listener = channelListener;
		if (listener != null) {
			try {
				listener.onChannelMemberLeft(channel, member);
			} catch (Throwable t) {
				log.error("Error dispatching onChannelMemberLeft to listener: {}", listener, t);
			}
		}
	}

	@Override
	public void onChannelMembersRemoved(Channel channel, List<Channel.Member> members) {
		ChannelListener listener = channelListener;
		if (listener != null) {
			try {
				listener.onChannelMembersRemoved(channel, members);
			} catch (Throwable t) {
				log.error("Error dispatching onChannelMembersRemoved to listener: {}", listener, t);
			}
		}
	}

	@Override
	public void onChannelMembersBanned(Channel channel, List<Channel.Member> banned) {
		ChannelListener listener = channelListener;
		if (listener != null) {
			try {
				listener.onChannelMembersBanned(channel, banned);
			} catch (Throwable t) {
				log.error("Error dispatching onChannelMembersBanned to listener: {}", listener, t);
			}
		}
	}

	@Override
	public void onChannelMembersUnbanned(Channel channel, List<Channel.Member> unbanned) {
		ChannelListener listener = channelListener;
		if (listener != null) {
			try {
				listener.onChannelMembersUnbanned(channel, unbanned);
			} catch (Throwable t) {
				log.error("Error dispatching onChannelMembersUnbanned to listener: {}", listener, t);
			}
		}
	}

	@Override
	public void onChannelMembersRoleChanged(Channel channel, List<Channel.Member> changed, Channel.Role role) {
		ChannelListener listener = channelListener;
		if (listener != null) {
			try {
				listener.onChannelMembersRoleChanged(channel, changed, role);
			} catch (Throwable t) {
				log.error("Error dispatching onChannelMembersRoleChanged to listener: {}", listener, t);
			}
		}
	}

	public void addSessionListener(SessionListener listener) {
		Objects.requireNonNull(listener, "listener");
		synchronized (listenersLock) {
			if (sessionListener instanceof SessionListenerArray listeners)
				listeners.add(listener);
			else
				sessionListener = new SessionListenerArray(listener, log);
		}
	}

	public void removeSessionListener(SessionListener listener) {
		Objects.requireNonNull(listener, "listener");
		synchronized (listenersLock) {
			if (sessionListener instanceof SessionListenerArray listeners) {
				listeners.remove(listener);
				if (listeners.isEmpty())
					sessionListener = null;
			}
		}
	}

	@Override
	public void onNewSession(SessionInfo sessionInfo) {
		SessionListener listener = sessionListener;
		if (listener != null) {
			try {
				listener.onNewSession(sessionInfo);
			} catch (Throwable t) {
				log.error("Error dispatching onNewSession to listener: {}", listener, t);
			}
		}
	}

	public void addFriendRequestListener(FriendRequestListener listener) {
		Objects.requireNonNull(listener, "listener");
		synchronized (listenersLock) {
			if (friendRequestListener instanceof FriendRequestListenerArray listeners)
				listeners.add(listener);
			else
				friendRequestListener = new FriendRequestListenerArray(listener, log);
		}
	}

	public void removeFriendRequestListener(FriendRequestListener listener) {
		Objects.requireNonNull(listener, "listener");
		synchronized (listenersLock) {
			if (friendRequestListener instanceof FriendRequestListenerArray listeners) {
				listeners.remove(listener);
				if (listeners.isEmpty())
					friendRequestListener = null;
			}
		}
	}

	@Override
	public void onFriendRequest(Id userId, String hello) {
		FriendRequestListener listener = friendRequestListener;
		if (listener != null) {
			try {
				listener.onFriendRequest(userId, hello);
			} catch (Throwable t) {
				log.error("Error dispatching onFriendRequest to listener: {}", listener, t);
			}
		}
	}

	@Override
	public void onFriendRequestAccepted(Id userId) {
		FriendRequestListener listener = friendRequestListener;
		if (listener != null) {
			try {
				listener.onFriendRequestAccepted(userId);
			} catch (Throwable t) {
				log.error("Error dispatching onFriendRequestAccepted to listener: {}", listener, t);
			}
		}
	}

	public void removeAllListeners() {
		synchronized (listenersLock) {
			connectionListener = null;
			messageListener = null;
			channelListener = null;
			contactListener = null;
			sessionListener = null;
			friendRequestListener = null;
		}
	}
}