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

package io.bosonnetwork.photonmessaging.impl.rpc;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.jspecify.annotations.Nullable;

import io.bosonnetwork.Id;
import io.bosonnetwork.json.Json;
import io.bosonnetwork.photonmessaging.InviteTicket;
import io.bosonnetwork.photonmessaging.impl.dto.ChannelMembersRole;
import io.bosonnetwork.photonmessaging.impl.dto.ChannelSessionKeyRotation;
import io.bosonnetwork.photonmessaging.impl.ContactMutation;
import io.bosonnetwork.photonmessaging.impl.dto.IdList;
import io.bosonnetwork.photonmessaging.impl.dto.NewChannelInfo;

public class RpcRequest {
	private static final ObjectReader READER = Json.cborMapper().readerFor(RpcRequest.class);
	private static final ObjectWriter WRITER = Json.cborMapper().writerFor(RpcRequest.class);

	@JsonProperty(value = "id", required = true)
	protected final long id;

	@JsonProperty(value = "m", required = true)
	protected final RpcMethod method;

	@JsonProperty("p")
	@JsonInclude(JsonInclude.Include.NON_NULL)
	@JsonTypeInfo(
			use = JsonTypeInfo.Id.NAME,
			include = JsonTypeInfo.As.EXTERNAL_PROPERTY,
			property = "m",
			defaultImpl = Void.class
	)
	@JsonSubTypes({
			@JsonSubTypes.Type(value = Void.class, name = "sl"),
			@JsonSubTypes.Type(value = Id.class, name = "sr"),
			@JsonSubTypes.Type(value = ContactMutation.class, name = "cm"),
			@JsonSubTypes.Type(value = Integer.class, name = "cs"),
			@JsonSubTypes.Type(value = NewChannelInfo.class, name = "cc"),
			@JsonSubTypes.Type(value = Void.class, name = "cd"),
			@JsonSubTypes.Type(value = Id.class, name = "cot"),
			@JsonSubTypes.Type(value = ChannelSessionKeyRotation.class, name = "csr"),
			@JsonSubTypes.Type(value = JsonNode.class, name = "ciu"),
			@JsonSubTypes.Type(value = ChannelMembersRole.class, name = "cru"),
			@JsonSubTypes.Type(value = IdList.class, name = "cmb"),
			@JsonSubTypes.Type(value = IdList.class, name = "cmu"),
			@JsonSubTypes.Type(value = IdList.class, name = "cmr"),
			@JsonSubTypes.Type(value = InviteTicket.class, name = "cj"),
			@JsonSubTypes.Type(value = Void.class, name = "cl"),
			@JsonSubTypes.Type(value = Void.class, name = "ci")
	})
	protected final @Nullable Object params;

	@JsonProperty("c")
	@JsonInclude(JsonInclude.Include.NON_NULL)
	protected final byte @Nullable [] cookie;

	@JsonCreator
	protected RpcRequest(@JsonProperty(value = "id", required = true) long id,
						 @JsonProperty(value = "m", required = true) RpcMethod method,
						 @JsonProperty(value = "p") @Nullable Object params,
						 @JsonProperty(value = "c") byte @Nullable [] cookie) {
		this.id = id;
		this.method = method;
		this.params = params;
		this.cookie = cookie == null ? null : cookie.clone();
	}

	protected RpcRequest(long id, RpcMethod method, @Nullable Object params) {
		this(id, method, params, null);
	}

	public long getId() {
		return id;
	}

	public RpcMethod getMethod() {
		return method;
	}

	@SuppressWarnings("unchecked")
	public <T> @Nullable T getParams() {
		return (T) params;
	}

	/**
	 * Returns a copy of the request cookie.
	 *
	 * @return a defensive copy of the cookie bytes, or {@code null} if no cookie is present
	 */
	public byte @Nullable [] getCookie() {
		return cookie == null ? null : cookie.clone();
	}

	public byte[] serialize() {
		try {
			return Json.cborMapper().writeValueAsBytes(this);
		} catch (IOException e) {
			throw new IllegalStateException("INTERNAL ERROR: RpcRequest serialization", e);
		}
	}

	public static RpcRequest parse(byte[] data) {
		try {
			return READER.readValue(data);
		} catch (IOException e) {
			throw new IllegalStateException("INTERNAL ERROR: RpcRequest deserialization", e);
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;

		if (o instanceof RpcRequest that)
			return id == that.id &&
					method == that.method &&
					Objects.equals(params, that.params) &&
					Arrays.equals(cookie, that.cookie);

		return false;
	}

	@Override
	public int hashCode() {
		return Objects.hash(id, method, params, Arrays.hashCode(cookie));
	}

	public static RpcRequest listSessions(long id) {
		return new RpcRequest(id, RpcMethod.SESSION_LIST, null);
	}

	public static RpcRequest revokeSession(long id, Id deviceId) {
		return new RpcRequest(id, RpcMethod.SESSION_REVOKE, deviceId);
	}

	public static RpcRequest contactMutate(long id, ContactMutation mutation) {
		return new RpcRequest(id, RpcMethod.CONTACT_MUTATE, mutation);
	}

	public static RpcRequest contactSync(long id, int revision) {
		return new RpcRequest(id, RpcMethod.CONTACT_SYNC, revision);
	}

	public static RpcRequest createChannel(long id, NewChannelInfo params) {
		return new RpcRequest(id, RpcMethod.CHANNEL_CREATE, params);
	}

	public static RpcRequest deleteChannel(long id) {
		return new RpcRequest(id, RpcMethod.CHANNEL_DELETE, null);
	}

	public static RpcRequest joinChannel(long id, InviteTicket ticket) {
		return new RpcRequest(id, RpcMethod.CHANNEL_JOIN, ticket);
	}

	public static RpcRequest leaveChannel(long id) {
		return new RpcRequest(id, RpcMethod.CHANNEL_LEAVE, null);
	}

	public static RpcRequest transferChannelOwnership(long id, Id newOwner) {
		return new RpcRequest(id, RpcMethod.CHANNEL_OWNERSHIP_TRANSFER, newOwner);
	}

	public static RpcRequest rotateChannelSessionKey(long id, ChannelSessionKeyRotation params) {
		return new RpcRequest(id, RpcMethod.CHANNEL_SESSION_KEY_ROTATE, params);
	}

	public static RpcRequest updateChannelInfo(long id, JsonNode changes) {
		return new RpcRequest(id, RpcMethod.CHANNEL_INFO_UPDATE, changes);
	}

	public static RpcRequest updateChannelMembersRole(long id, ChannelMembersRole params) {
		return new RpcRequest(id, RpcMethod.CHANNEL_MEMBERS_ROLE_UPDATE, params);
	}

	public static RpcRequest banChannelMembers(long id, Collection<Id> memberIds) {
		return new RpcRequest(id, RpcMethod.CHANNEL_MEMBERS_BAN, List.copyOf(memberIds));
	}

	public static RpcRequest unbanChannelMembers(long id, Collection<Id> memberIds) {
		return new RpcRequest(id, RpcMethod.CHANNEL_MEMBERS_UNBAN, List.copyOf(memberIds));
	}

	public static RpcRequest removeChannelMembers(long id, Collection<Id> memberIds) {
		return new RpcRequest(id, RpcMethod.CHANNEL_MEMBERS_REMOVE, List.copyOf(memberIds));
	}

	public static RpcRequest getChannelInfo(long id) {
		return new RpcRequest(id, RpcMethod.CHANNEL_INFO, null);
	}
}