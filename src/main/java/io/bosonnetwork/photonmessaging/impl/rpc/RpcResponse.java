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
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.jspecify.annotations.Nullable;

import io.bosonnetwork.Id;
import io.bosonnetwork.json.Json;
import io.bosonnetwork.photonmessaging.SessionInfo;
import io.bosonnetwork.photonmessaging.impl.ContactSync;
import io.bosonnetwork.photonmessaging.impl.dto.ChannelInfo;
import io.bosonnetwork.photonmessaging.impl.dto.IdList;
import io.bosonnetwork.photonmessaging.impl.dto.SessionInfoList;

/**
 * RPC response envelope.
 * <p>
 * Protocol rules:
 * <ul>
 *   <li>{@code error == null} means the call succeeded</li>
 *   <li>{@code error != null} means the call failed</li>
 *   <li>{@code result} may be {@code null} for successful calls that do not return a value</li>
 *   <li>{@code result} and {@code error} must not both be non-null</li>
 * </ul>
 */
public class RpcResponse {
	private static final ObjectReader READER = Json.cborMapper().readerFor(RpcResponse.class);
	private static final ObjectWriter WRITER = Json.cborMapper().writerFor(RpcResponse.class);

	@JsonProperty(value = "id", required = true)
	private final long id;

	@JsonProperty(value = "m", required = true)
	private final RpcMethod method;

	@JsonProperty("r")
	@JsonInclude(JsonInclude.Include.NON_NULL)
	@JsonTypeInfo(
			use = JsonTypeInfo.Id.NAME,
			include = JsonTypeInfo.As.EXTERNAL_PROPERTY,
			property = "m",
			defaultImpl = Void.class
	)
	@JsonSubTypes({
			@JsonSubTypes.Type(value = SessionInfoList.class, name = "sl"),
			@JsonSubTypes.Type(value = Void.class, name = "sr"),
			@JsonSubTypes.Type(value = Integer.class, name = "cm"),
			@JsonSubTypes.Type(value = ContactSync.class, name = "cs"),
			@JsonSubTypes.Type(value = ChannelInfo.class, name = "cc"),
			@JsonSubTypes.Type(value = Void.class, name = "cd"),
			@JsonSubTypes.Type(value = Void.class, name = "cot"),
			@JsonSubTypes.Type(value = Void.class, name = "csr"),
			@JsonSubTypes.Type(value = Void.class, name = "ciu"),
			@JsonSubTypes.Type(value = IdList.class, name = "cru"),
			@JsonSubTypes.Type(value = IdList.class, name = "cmb"),
			@JsonSubTypes.Type(value = IdList.class, name = "cmu"),
			@JsonSubTypes.Type(value = IdList.class, name = "cmr"),
			@JsonSubTypes.Type(value = ChannelInfo.class, name = "cj"),
			@JsonSubTypes.Type(value = Void.class, name = "cl"),
			@JsonSubTypes.Type(value = ChannelInfo.class, name = "ci")
	})
	private final @Nullable Object result;

	@JsonProperty("e")
	@JsonInclude(JsonInclude.Include.NON_NULL)
	private final @Nullable RpcError error;

	/**
	 * Creates an RPC response.
	 *
	 * @throws IllegalArgumentException if both {@code result} and {@code error} are non-null
	 */
	@JsonCreator
	protected RpcResponse(@JsonProperty(value = "id", required = true) long id,
						  @JsonProperty(value = "m", required = true) RpcMethod method,
						  @JsonProperty(value = "r") @Nullable Object result,
						  @JsonProperty(value = "e") @Nullable RpcError error)  {
		if (result != null && error != null)
			throw new IllegalArgumentException("Cannot have both result and error");

		this.id = id;
		this.method = method;
		this.result = result;
		this.error = error;
	}

	public long getId() {
		return id;
	}

	public RpcMethod getMethod() {
		return method;
	}

	public boolean succeeded() {
		return error == null;
	}

	public boolean failed() {
		return error != null;
	}

	@SuppressWarnings("unchecked")
	public <T> @Nullable T getResult() {
		return (T) result;
	}

	public @Nullable RpcError getError() {
		return error;
	}

	public byte[] serialize() {
		try {
			return WRITER.writeValueAsBytes(this);
		} catch (IOException e) {
			throw new IllegalStateException("INTERNAL ERROR: RpcResponse serialization", e);
		}
	}

	public static RpcResponse parse(byte[] data) {
		try {
			return READER.readValue(data);
		} catch (IOException e) {
			throw new IllegalStateException("INTERNAL ERROR: RpcResponse deserialization", e);
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;

		if (o instanceof RpcResponse that)
			return id == that.id &&
					method == that.method &&
					Objects.equals(result, that.result) &&
					Objects.equals(error, that.error);

		return false;
	}

	@Override
	public int hashCode() {
		return Objects.hash(id, method, result, error);
	}

	@Override
	public String toString() {
		return id + ":" + method + (succeeded() ? " OK" : " FAIL");
	}

	public static RpcResponse listSessions(long id, List<SessionInfo> sessions) {
		return new RpcResponse(id, RpcMethod.SESSION_LIST, sessions, null);
	}

	public static RpcResponse revokeSession(long id) {
		return new RpcResponse(id, RpcMethod.SESSION_REVOKE, null, null);
	}

	public static RpcResponse contactMutate(long id, int revision) {
		return new RpcResponse(id, RpcMethod.CONTACT_MUTATE, revision, null);
	}

	public static RpcResponse contactSync(long id, ContactSync contactSync) {
		return new RpcResponse(id, RpcMethod.CONTACT_SYNC, contactSync, null);
	}

	public static RpcResponse createChannel(long id, ChannelInfo channelInfo) {
		return new RpcResponse(id, RpcMethod.CHANNEL_CREATE, channelInfo, null);
	}

	public static RpcResponse deleteChannel(long id) {
		return new RpcResponse(id, RpcMethod.CHANNEL_DELETE, null, null);
	}

	public static RpcResponse joinChannel(long id, ChannelInfo channelInfo) {
		return new RpcResponse(id, RpcMethod.CHANNEL_JOIN, channelInfo, null);
	}

	public static RpcResponse leaveChannel(long id) {
		return new RpcResponse(id, RpcMethod.CHANNEL_LEAVE, null, null);
	}

	public static RpcResponse transferChannelOwnership(long id) {
		return new RpcResponse(id, RpcMethod.CHANNEL_OWNERSHIP_TRANSFER, null, null);
	}

	public static RpcResponse rotateChannelSessionKey(long id) {
		return new RpcResponse(id, RpcMethod.CHANNEL_SESSION_KEY_ROTATE, null, null);
	}

	public static RpcResponse updateChannelInfo(long id) {
		return new RpcResponse(id, RpcMethod.CHANNEL_INFO_UPDATE, null, null);
	}

	public static RpcResponse updateChannelMembersRole(long id, List<Id> memberIds) {
		return new RpcResponse(id, RpcMethod.CHANNEL_MEMBERS_ROLE_UPDATE, memberIds, null);
	}

	public static RpcResponse banChannelMembers(long id, List<Id> memberIds) {
		return new RpcResponse(id, RpcMethod.CHANNEL_MEMBERS_BAN, memberIds, null);
	}

	public static RpcResponse unbanChannelMembers(long id, List<Id> memberIds) {
		return new RpcResponse(id, RpcMethod.CHANNEL_MEMBERS_UNBAN, memberIds, null);
	}

	public static RpcResponse removeChannelMembers(long id, List<Id> memberIds) {
		return new RpcResponse(id, RpcMethod.CHANNEL_MEMBERS_REMOVE, memberIds, null);
	}

	public static RpcResponse getChannelInfo(long id, ChannelInfo channelInfo) {
		return new RpcResponse(id, RpcMethod.CHANNEL_INFO, channelInfo, null);
	}

	public static RpcResponse error(long id, RpcMethod method, RpcError error) {
		return new RpcResponse(id, method, null, error);
	}

	public static RpcResponse error(long id, RpcMethod method, int errorCode, String errorMessage, @Nullable String errorData) {
		return error(id, method, new RpcError(errorCode, errorMessage, errorData));
	}

	public static RpcResponse error(long id, RpcMethod method, int errorCode, String errorMessage) {
		return error(id, method, errorCode, errorMessage, null);
	}
}