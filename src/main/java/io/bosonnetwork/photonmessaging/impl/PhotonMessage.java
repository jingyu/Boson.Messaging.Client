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
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.jspecify.annotations.Nullable;

import io.bosonnetwork.Id;
import io.bosonnetwork.json.Json;
import io.bosonnetwork.photonmessaging.DeliveryStatus;
import io.bosonnetwork.photonmessaging.Message;
import io.bosonnetwork.photonmessaging.MessageDeliveryReceipt;
import io.bosonnetwork.photonmessaging.impl.rpc.RpcRequest;
import io.bosonnetwork.photonmessaging.impl.rpc.RpcResponse;

@JsonPropertyOrder({"v", "id", "r", "y", "f", "c", "p"})
public class PhotonMessage<P> implements Message, DeviceOriginated {
	private static final int VERSION = 2;

	@JsonProperty(value = "v" , required = true)
	private final int version;
	@JsonProperty(value = "id", required = true)
	private final Id id;
	@JsonProperty(value = "r", required = true)
	private final Id recipient;
	@JsonProperty(value = "y", required = true)
	private final Type type;
	@JsonProperty(value = "f")
	@JsonInclude(JsonInclude.Include.NON_NULL)
	private @Nullable Id from;
	@JsonProperty(value = "c")
	@JsonInclude(JsonInclude.Include.NON_DEFAULT)
	private final long createdAt;

	@JsonProperty(value = "p", required = true)
	private final P payload;

	private long rid;
	private @Nullable Id conversationId;
	private long sentAt;
	private long receivedAt;

	private DeliveryStatus deliveryStatus;
	private @Nullable String deliveryReason;
	private long deliveryAt;

	private volatile @Nullable Promise<Void> sentPromise;

	// The 'from' field is required for incoming wire messages but omitted for outgoing ones.
	// This constructor is used by Jackson for deserialization, where 'from' must be present.
	@JsonCreator
	protected PhotonMessage(@JsonProperty(value = "v", required = true) int version,
	                        @JsonProperty(value = "id", required = true) Id id,
	                        @JsonProperty(value = "r", required = true) Id recipient,
	                        @JsonProperty(value = "y", required = true) Type type,
	                        @JsonProperty(value = "f", required = true) @Nullable Id from,
	                        @JsonProperty(value = "c", required = true) long createdAt,
	                        @JsonProperty(value = "p", required = true) P payload) {
		// Reject wire messages with an unrecognised version: we cannot correctly interpret them.
		if (version != VERSION)
			throw new IllegalArgumentException("Unsupported message version: " + version);

		this.version = version;
		this.id = id;
		this.recipient = recipient;
		this.from = from;
		this.type = type;
		this.createdAt = createdAt;
		this.payload = payload;
		this.deliveryStatus = DeliveryStatus.UNDELIVERED;
	}

	protected PhotonMessage(Id id, Id recipient, Type type, long createAt, P payload) {
		this(VERSION, id, recipient, type, null, createAt, payload);
	}

	protected PhotonMessage(long rid, @Nullable Id conversationId, int version, Id id, Id recipient, Type type, @Nullable Id from,
							long createdAt, P payload, long sentAt, long receivedAt) {
		this.rid = rid;
		this.conversationId = conversationId;
		this.version = version;
		this.id = id;
		this.recipient = recipient;
		this.type = type;
		this.from = from;
		this.createdAt = createdAt;
		this.payload = payload;
		this.sentAt = sentAt;
		this.receivedAt = receivedAt;
		this.deliveryStatus = DeliveryStatus.UNDELIVERED;
	}

	protected PhotonMessage(PhotonMessage<?> message, P newPayload) {
		this.version = message.version;
		this.id = message.id;
		this.recipient = message.recipient;
		this.type = message.type;
		this.from = message.from;
		this.createdAt = message.createdAt;
		this.payload = newPayload;

		this.conversationId = message.conversationId;
		this.deliveryFailure = message.deliveryFailure;
		this.sentAt = message.sentAt;
		this.receivedAt = message.receivedAt;
		this.deliveryStatus = message.deliveryStatus;
		this.deliveryReason = message.deliveryReason;
		this.deliveryAt = message.deliveryAt;
	}

	private @Nullable MessageDeliveryReceipt deliveryFailure;

	@Override
	public Optional<MessageDeliveryReceipt> getDeliveryFailure() { return Optional.ofNullable(deliveryFailure); }

	void setDeliveryFailure(@Nullable MessageDeliveryReceipt failure) { this.deliveryFailure = failure; }

	public int getVersion() {
		return version;
	}

	@Override
	public Id getId() {
		return id;
	}

	@Override
	public Optional<Id> getConversationId() {
		return Optional.ofNullable(conversationId);
	}

	Id getConversationIdOrThrow() {
		if (conversationId == null)
			throw new IllegalStateException("Conversation ID is not set");

		return conversationId;
	}

	protected void setConversationId(Id conversationId) {
		this.conversationId = conversationId;
	}

	@Override
	public Id getRecipient() {
		return recipient;
	}

	@Override
	public Type getType() {
		return type;
	}

	@Override
	public Optional<Id> getFrom() {
		return Optional.ofNullable(from);
	}

	Id getFromOrThrow() {
		if (from == null)
			throw new IllegalStateException("Message sender is not set");

		return from;
	}

	@Override
	public long getCreatedAt() {
		return createdAt;
	}

	@Override
	public long getReceivedAt() {
		return receivedAt;
	}

	@Override
	public long getTimestamp() {
		return createdAt;
	}

	public long getSentAt() {
		return sentAt;
	}

	public PhotonMessage<P> setFrom(@Nullable Id from) {
		this.from = from;
		return this;
	}

	protected void setSentAt() {
		setSentAt(System.currentTimeMillis());
	}

	public void setSentAt(long timestamp) {
		this.sentAt = timestamp;
	}

	protected void received() {
		received(System.currentTimeMillis());
	}

	public void received(long timestamp) {
		this.receivedAt = timestamp;
	}

	public long getRid() {
		return rid;
	}

	protected void setRid(long rid) {
		this.rid = rid;
	}

	@Override
	public byte[] getPayloadAsBytes() {
		if (payload instanceof byte[] bytes)
			return bytes;

		// Optimised serialization
		if (payload instanceof MessageContent c)
			return c.serialize();

		if (payload instanceof Notification n)
			return n.serialize();

		if (payload instanceof RpcRequest q)
			return q.serialize();

		if (payload instanceof RpcResponse r)
			return r.serialize();

		// general serialization
		try {
			return Json.cborMapper().writeValueAsBytes(payload);
		} catch (IOException e) {
			throw new IllegalStateException("Message payload serialization failed", e);
		}
	}

	@Override
	public Content getPayloadAsContent() {
		if (payload instanceof Content c)
			return c;

		throw new IllegalStateException("Message payload is not a Content");
	}

	protected P getPayload() {
		return payload;
	}

	public DeliveryStatus getDeliveryStatus() {
		return deliveryStatus;
	}

	public @Nullable String getDeliveryReason() {
		return deliveryReason;
	}

	public long getDeliveryAt() {
		return deliveryAt;
	}

	protected void prepareForSending() {
		this.sentPromise = Promise.promise();
	}

	protected void sent() {
		Promise<Void> promise = sentPromise;
		if (promise == null)
			throw new IllegalStateException("Message has not been sent yet");
		sentAt = System.currentTimeMillis();
		promise.tryComplete();
	}

	protected void failed(Throwable e) {
		Promise<Void> promise = sentPromise;
		if (promise == null)
			throw new IllegalStateException("Message has not been sent yet");
		promise.tryFail(e);
	}

	protected Future<Void> getFuture() {
		Promise<Void> promise = sentPromise;
		if (promise == null)
			throw new IllegalStateException("Message has not been sent yet");
		return promise.future();
	}
}