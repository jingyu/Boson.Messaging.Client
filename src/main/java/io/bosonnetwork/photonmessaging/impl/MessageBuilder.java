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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

import io.bosonnetwork.Id;
import io.bosonnetwork.photonmessaging.ContentDisposition;
import io.bosonnetwork.photonmessaging.ContentType;
import io.bosonnetwork.photonmessaging.Message;
import io.bosonnetwork.vertx.ContextualFuture;

public class MessageBuilder implements Message.Builder {
	private final PhotonMessagingClient client;
	private @Nullable Id recipient;
	private final Map<String, Object> headers;
	private MessageContent.@Nullable Format format;
	private @Nullable Object content;

	protected MessageBuilder(PhotonMessagingClient client, @Nullable Id recipient) {
		this.client = client;
		this.headers = new HashMap<>();
		this.recipient = recipient;
	}

	@Override
	public MessageBuilder to(Id to) {
		Objects.requireNonNull(to, "to");
		this.recipient = to;
		return this;
	}

	@Override
	public MessageBuilder header(String name, @Nullable Object value) {
		Objects.requireNonNull(name, "name");
		if (value != null)
			headers.put(name, value);
		else
			headers.remove(name);

		return this;
	}

	@Override
	public MessageBuilder contentType(String contentType) {
		Objects.requireNonNull(contentType, "contentType");
		header(ContentType.HEADER_NAME, contentType);
		return this;
	}

	@Override
	public MessageBuilder contentDisposition(String contentDisposition) {
		Objects.requireNonNull(contentDisposition, "contentDisposition");
		header(ContentDisposition.HEADER_NAME, contentDisposition);
		return this;
	}

	@Override
	public MessageBuilder contentDisposition(ContentDisposition contentDisposition) {
		Objects.requireNonNull(contentDisposition, "contentDisposition");
		header(ContentDisposition.HEADER_NAME, contentDisposition.getValue());
		return this;
	}

	@Override
	public MessageBuilder contentText(String text) {
		Objects.requireNonNull(text, "text");
		this.format = MessageContent.Format.TEXT;
		this.content = text;
		return this;
	}

	@Override
	public MessageBuilder contentBinary(byte[] binary) {
		Objects.requireNonNull(binary, "binary");
		this.format = MessageContent.Format.BINARY;
		// No clone here: MessageContent's constructor is the single owner that defensively copies
		// the binary body at build()/send() time.
		this.content = binary;
		if (!headers.containsKey(ContentType.HEADER_NAME))
			headers.put(ContentType.HEADER_NAME, ContentType.BINARY);

		return this;
	}

	@Override
	public MessageBuilder contentObject(Object object) {
		Objects.requireNonNull(object, "object");
		this.format = MessageContent.Format.OBJECT;
		this.content = object;
		if (!headers.containsKey(ContentType.HEADER_NAME))
			headers.put(ContentType.HEADER_NAME, ContentType.CBOR);

		return this;
	}

	private Message build() {
		if (recipient == null)
			throw new IllegalStateException("Recipient not set");

		long now = client.originTimestamp();
		Id messageId = DeviceOriginated.generateId(client.getDeviceId(), now);

		if (content == null || format == null)
			throw new IllegalStateException("content not set");

		MessageContent payload = switch (format) {
			case TEXT -> MessageContent.text(headers, (String) content);
			case BINARY -> MessageContent.binary(headers, (byte[]) content);
			case OBJECT -> MessageContent.object(headers, content);
		};
		return new PhotonMessage<>(messageId, recipient, Message.Type.CONTENT_MESSAGE, now, payload);
	}

	@Override
	public ContextualFuture<Message> send() {
		return ContextualFuture.of(client.sendMessage(build()));
	}
}