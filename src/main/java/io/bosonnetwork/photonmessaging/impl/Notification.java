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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;

import io.bosonnetwork.Id;
import io.bosonnetwork.photonmessaging.MessageDeliveryReceipt;
import io.bosonnetwork.json.Json;
import io.bosonnetwork.photonmessaging.SessionInfo;
import io.bosonnetwork.photonmessaging.impl.dto.ChannelInfo;
import io.bosonnetwork.photonmessaging.impl.dto.ChannelMembersRole;
import io.bosonnetwork.photonmessaging.impl.dto.IdList;

/**
 * Represents a binary-encoded notification message exchanged between clients and the server.
 *
 * <p>
 * A notification is a compact event envelope used in the messaging system. It contains:
 * <ul>
 *   <li>a unique message id</li>
 *   <li>the source user/device id</li>
 *   <li>a timestamp</li>
 *   <li>an event type</li>
 *   <li>a polymorphic event body</li>
 * </ul>
 *
 * <p>
 * The body field is encoded using Jackson polymorphic type resolution with:
 * {@code JsonTypeInfo.As.EXTERNAL_PROPERTY}, where the event type field {@code e}
 * determines the runtime type of {@code body}.
 *
 * <p>
 * This design is optimized for CBOR encoding (compact binary wire format) and is
 * fully compatible with the Rust implementation below.
 *
 * <h2>Wire Format</h2>
 * Example (CBOR-equivalent JSON representation):
 * <pre>
 * {
 *   "id": "...",
 *   "s": "...",
 *   "t": 1710000000000,
 *   "e": "cm",
 *   "b": { ... }
 * }
 * </pre>
 *
 * <h2>Rust Equivalent</h2>
 *
 * <pre>
 * use serde::{Serialize, Deserialize};
 *
 * #[derive(Debug, Serialize, Deserialize)]
 * pub struct Notification {
 *     #[serde(rename = "id")]
 *     pub id: Id,
 *     #[serde(rename = "s")]
 *     pub source: Id,
 *     #[serde(rename = "t")]
 *     pub timestamp: i64,
 *     #[serde(rename = "e")]
 *     pub event: Event,
 *     #[serde(rename = "b")]
 *     pub body: Option<Body>,
 * }
 *
 * #[derive(Debug, Serialize, Deserialize)]
 * #[serde(tag = "e", content = "b")]
 * pub enum Body {
 *     #[serde(rename = "sn")]
 *     SessionNew(SessionInfo),
 *     #[serde(rename = "cs")]
 *     ContactSync(ContactSync),
 *     #[serde(rename = "cm")]
 *     ContactMutate(ContactSync),
 *     #[serde(rename = "cc")]
 *     ChannelCreate(ChannelInfo),
 *     #[serde(rename = "cd")]
 *     ChannelDelete(()),
 *     #[serde(rename = "cj")]
 *     ChannelJoin(ChannelInfo),
 *     #[serde(rename = "cl")]
 *     ChannelLeave(()),
 *     #[serde(rename = "cot")]
 *     ChannelOwnershipTransfer(Id),
 *     #[serde(rename = "csr")]
 *     ChannelSessionKeyRotate(Vec<u8>),
 *     #[serde(rename = "ciu")]
 *     ChannelInfoUpdate(serde_json::Value),
 *     #[serde(rename = "cru")]
 *     ChannelMembersRoleUpdate(RpcChannelMembersRoleParams),
 *     #[serde(rename = "cmb")]
 *     ChannelMembersBan(Vec<Id>),
 *     #[serde(rename = "cmu")]
 *     ChannelMembersUnban(Vec<Id>),
 *     #[serde(rename = "cmr")]
 *     ChannelMembersRemove(Vec<Id>),
 *     #[serde(rename = "cmj")]
 *     ChannelMemberJoin(ChannelMember),
 *     #[serde(rename = "cml")]
 *     ChannelMemberLeave(Id),
 *     #[serde(rename = "fr")]
 *     FriendRequest(String),
 *     #[serde(rename = "fra")]
 *     FriendRequestAccept(Vec<u8>),
 * }
 * </pre>
 *
 * <h2>Design Notes</h2>
 * <ul>
 *   <li>Event field {@code e} acts as the external type discriminator</li>
 *   <li>Body field {@code b} is polymorphic and event-dependent</li>
 *   <li>Uses compact string tags (e.g. "cm", "fr") optimized for CBOR</li>
 *   <li>Rust and Java share identical wire format for interoperability</li>
 * </ul>
 */
public class Notification implements DeviceOriginated {
	private static final ObjectReader READER = Json.cborMapper().readerFor(Notification.class);
	private static final ObjectWriter WRITER = Json.cborMapper().writerFor(Notification.class);

	@JsonProperty(value = "id", required = true)
	private final Id id;
	@JsonProperty(value = "s", required = true)
	private final Id source;
	@JsonProperty(value = "t", required = true)
	private final long timestamp;
	@JsonProperty(value = "e", required = true)
	private final Event event;
	@JsonProperty(value = "b")
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	@JsonTypeInfo(
			use = JsonTypeInfo.Id.NAME,
			include = JsonTypeInfo.As.EXTERNAL_PROPERTY,
			property = "e",
			defaultImpl = Void.class
	)
	@JsonSubTypes({
			@JsonSubTypes.Type(value = SessionInfo.class, name = "sn"),
			@JsonSubTypes.Type(value = ContactSync.class, name = "cs"),
			@JsonSubTypes.Type(value = MessageDeliveryReceipt.class, name = "dr"),
			@JsonSubTypes.Type(value = ChannelInfo.class, name = "cc"),
			@JsonSubTypes.Type(value = Void.class, name = "cd"),
			@JsonSubTypes.Type(value = ChannelInfo.class, name = "cj"),
			@JsonSubTypes.Type(value = Void.class, name = "cl"),
			@JsonSubTypes.Type(value = Id.class, name = "cot"),
			@JsonSubTypes.Type(value = byte[].class, name = "csr"),
			@JsonSubTypes.Type(value = JsonNode.class, name = "ciu"),
			@JsonSubTypes.Type(value = ChannelMembersRole.class, name = "cru"),
			@JsonSubTypes.Type(value = IdList.class, name = "cmb"),
			@JsonSubTypes.Type(value = IdList.class, name = "cmu"),
			@JsonSubTypes.Type(value = IdList.class, name = "cmr"),
			@JsonSubTypes.Type(value = ChannelMember.class, name = "cmj"),
			@JsonSubTypes.Type(value = Id.class, name = "cml")
	})
	private final Object body;

	public enum Event {
		@JsonProperty("sn")
		SESSION_NEW(1),
		@JsonProperty("cs")
		CONTACT_SYNC(2),
		@JsonProperty("dr")
		MESSAGE_DELIVERY_RECEIPT(3),
		@JsonProperty("cc")
		CHANNEL_CREATE(21),
		@JsonProperty("cd")
		CHANNEL_DELETE(22),
		@JsonProperty("cj")
		CHANNEL_JOIN(23),
		@JsonProperty("cl")
		CHANNEL_LEAVE(24),
		@JsonProperty("cot")
		CHANNEL_OWNERSHIP_TRANSFER(25),
		@JsonProperty("csr")
		CHANNEL_SESSION_KEY_ROTATE(26),
		@JsonProperty("ciu")
		CHANNEL_INFO_UPDATE(27),
		@JsonProperty("cru")
		CHANNEL_MEMBERS_ROLE_UPDATE(28),
		@JsonProperty("cmb")
		CHANNEL_MEMBERS_BAN(29),
		@JsonProperty("cmu")
		CHANNEL_MEMBERS_UNBAN(30),
		@JsonProperty("cmr")
		CHANNEL_MEMBERS_REMOVE(31),
		@JsonProperty("cmj")
		CHANNEL_MEMBER_JOIN(32),
		@JsonProperty("cml")
		CHANNEL_MEMBER_LEAVE(33);

		private final int value;

		Event(int value) {
			this.value = value;
		}

		public int value() {
			return value;
		}

		public static Event valueOf(int value) {
			return switch (value) {
				case 1 -> SESSION_NEW;
				case 2 -> CONTACT_SYNC;
				case 3 -> MESSAGE_DELIVERY_RECEIPT;
				case 21 -> CHANNEL_CREATE;
				case 22 -> CHANNEL_DELETE;
				case 23 -> CHANNEL_JOIN;
				case 24 -> CHANNEL_LEAVE;
				case 25 -> CHANNEL_OWNERSHIP_TRANSFER;
				case 26 -> CHANNEL_SESSION_KEY_ROTATE;
				case 27 -> CHANNEL_INFO_UPDATE;
				case 28 -> CHANNEL_MEMBERS_ROLE_UPDATE;
				case 29 -> CHANNEL_MEMBERS_BAN;
				case 30 -> CHANNEL_MEMBERS_UNBAN;
				case 31 -> CHANNEL_MEMBERS_REMOVE;
				case 32 -> CHANNEL_MEMBER_JOIN;
				case 33 -> CHANNEL_MEMBER_LEAVE;
				default -> throw new IllegalArgumentException("Invalid event value: " + value);
			};
		}
	}

	@JsonCreator
	protected Notification(@JsonProperty(value = "id", required = true) Id id,
	                       @JsonProperty(value = "s", required = true) Id source,
	                       @JsonProperty(value = "t", required = true) long timestamp,
	                       @JsonProperty(value = "e", required = true) Event event,
	                       @JsonProperty(value = "b") Object body) {
		this.id = id;
		this.source = source;
		this.timestamp = timestamp;
		this.event = event;
		this.body = body;
	}

	@Override
	public Id getId() {
		return id;
	}

	public Event getEvent() {
		return event;
	}

	public Id getSource() {
		return source;
	}

	@Override
	public long getTimestamp() {
		return timestamp;
	}

	@SuppressWarnings("unchecked")
	public <T> T getBody() {
		return (T) body;
	}

	@Override
	public String toString() {
		return event.name() + ": id=" + id + ", source=" + source;
	}

	public byte[] serialize() {
		try {
			return WRITER.writeValueAsBytes(this);
		} catch (Exception e) {
			throw new IllegalStateException("INTERNAL ERROR: Notification serialization", e);
		}
	}

	public static Notification parse(byte[] data) {
		try {
			return READER.readValue(data);
		} catch (Exception e) {
			throw new IllegalStateException("INTERNAL ERROR: Notification parsing", e);
		}
	}
}