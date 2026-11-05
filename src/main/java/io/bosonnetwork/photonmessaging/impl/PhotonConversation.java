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

import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import org.jspecify.annotations.Nullable;

import io.bosonnetwork.photonmessaging.Contact;
import io.bosonnetwork.photonmessaging.Conversation;
import io.bosonnetwork.photonmessaging.InviteTicket;
import io.bosonnetwork.photonmessaging.Message;
import io.bosonnetwork.photonmessaging.MessageDeliveryReceipt;

public class PhotonConversation implements Conversation {
	public static final int MAX_SNIPPET_LENGTH = 128;

	private final PhotonContact contact;
	private final Function<PhotonContact, SessionContext> sessionContextFactory;

	private @Nullable Message lastMessage;
	private transient @Nullable String preview;

	private transient @Nullable SessionContext sessionContext;

	protected PhotonConversation(PhotonContact contact, @Nullable Message lastMessage,
								 Function<PhotonContact, SessionContext> sessionContextFactory) {
		this.contact = contact;
		this.sessionContextFactory = sessionContextFactory;
		if (lastMessage != null)
			update(lastMessage);
	}

	protected PhotonConversation(PhotonContact contact, Function<PhotonContact, SessionContext> sessionContextFactory) {
		this(contact, null, sessionContextFactory);
	}

	@Override
	public PhotonContact getContact() {
		return contact;
	}

	@Override
	public boolean isChannel() {
		return contact.getType() == Contact.Type.CHANNEL;
	}

	@Override
	public Optional<String> getPreview() {
		String snippet = preview;
		if (snippet == null && lastMessage != null) {
			Message.Content content = lastMessage.getPayloadAsContent();
			String contentType = content.getContentType();
			if (contentType.startsWith("text/")) {
				String body = content.asText().trim();
				snippet = body.length() < MAX_SNIPPET_LENGTH ? body : body.substring(0, MAX_SNIPPET_LENGTH - 3) + "...";
			} else if (contentType.startsWith("image/")) {
				snippet = "(Image)";
			} else if (contentType.startsWith("audio/")) {
				snippet = "(Audio)";
			} else if (contentType.startsWith("video/")) {
				snippet = "(Video)";
			} else if (contentType.startsWith(InviteTicket.CONTENT_TYPE)) {
				snippet = "(Invitation)";
			} else {
				if (content.getContentDisposition().isPresent())
					snippet = "(Attachment)";
				else
					snippet = "(Unknown)";
			}

			preview = snippet;
		}

		return Optional.ofNullable(snippet);
	}

	@Override
	public Optional<MessageDeliveryReceipt> getDeliveryFailure() {
		return lastMessage == null ? Optional.empty() : lastMessage.getDeliveryFailure();
	}

	@Override
	public long getUpdatedAt() {
		return lastMessage != null ? lastMessage.getReceivedAt() : 0;
	}

	protected void update(Message message) {
		Objects.requireNonNull(message, "message");
		if (message.getConversationId().map(convId -> !convId.equals(contact.getId())).orElse(true))
			throw new IllegalArgumentException("Message does not match the conversation");

		this.lastMessage = message;
		this.preview = null;
	}

	protected SessionContext getSessionContext() {
		SessionContext ctx = sessionContext;
		if (ctx == null) {
			ctx = sessionContextFactory.apply(contact);
			sessionContext = ctx;
		}

		return ctx;
	}

	public boolean is(Conversation conversation) {
		if (this == conversation)
			return true;

		return this.getId().equals(conversation.getId());
	}

	@Override public int hashCode() {
		return 0x6030AC0A + getId().hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;

		if (o instanceof Conversation that)
			return this.getId().equals(that.getId());

		return false;
	}

	@Override
	public int compareTo(Conversation o) {
		int r = Long.compare(getUpdatedAt(), o.getUpdatedAt());
		return r != 0 ? r : getId().compareTo(o.getId());
	}

	@Override
	public String toString() {
		return "Conversation: " +
				getId() +
				'[' +
				getPreview() + ", " +
				getUpdatedAt() +
				']';
	}
}