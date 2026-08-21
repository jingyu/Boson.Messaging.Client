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

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.databind.JsonNode;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.jspecify.annotations.Nullable;

import io.bosonnetwork.Id;
import io.bosonnetwork.photonmessaging.InviteTicket;
import io.bosonnetwork.photonmessaging.SessionInfo;
import io.bosonnetwork.photonmessaging.exceptions.rpc.*;
import io.bosonnetwork.photonmessaging.impl.ContactMutation;
import io.bosonnetwork.photonmessaging.impl.dto.ChannelInfo;
import io.bosonnetwork.photonmessaging.impl.dto.ChannelMembersRole;
import io.bosonnetwork.photonmessaging.impl.dto.ChannelSessionKeyRotation;
import io.bosonnetwork.photonmessaging.impl.dto.NewChannelInfo;

public class RpcCall<R> {
	private static final long DEFAULT_TIMEOUT = 30 * 1000; // 30 seconds

	private final RpcRequest request;
	private @Nullable RpcResponse response;
	private final long timeout; // in milliseconds
	private final Promise<@Nullable R> responsePromise;

	private static final AtomicLong nextId = new AtomicLong(8192 + Math.abs(new Random().nextInt(32768)));

	protected RpcCall(RpcRequest request, long timeout) {
		this.request = request;
		this.timeout = timeout;
		this.responsePromise = Promise.<@Nullable R>promise();
	}

	protected RpcCall(RpcRequest request) {
		this(request, DEFAULT_TIMEOUT);
	}

	public long getId() {
		return request.getId();
	}

	public RpcMethod getMethod() {
		return request.getMethod();
	}

	public RpcRequest getRequest() {
		return request;
	}

	public @Nullable RpcResponse getResponse() {
		return response;
	}

	public long getTimeout() {
		return timeout;
	}

	public void setResponse(RpcResponse response) {
		Objects.requireNonNull(response);
		if (this.response != null)
			throw new IllegalStateException("Response already set");
		if (response.getId() != request.getId() || response.getMethod() != request.getMethod())
			throw new IllegalArgumentException("Mismatched response");

		this.response = response;

		if (response.getError() != null) {
			RpcError error = response.getError();
			responsePromise.tryFail(rpcErrorToException(error));
		} else {
			responsePromise.tryComplete(response.getResult());
		}
	}

	// A timeout <= 0 means "wait indefinitely" (no timeout); a positive timeout fails the
	// future with RpcTimeoutException after that many milliseconds.
	public Future<@Nullable R> getFuture() {
		Future<@Nullable R> future = responsePromise.future().<@Nullable R>map(r -> r);
		if (timeout <= 0)
			return future;

		return future.timeout(timeout, TimeUnit.MILLISECONDS)
				.recover(err -> {
					if (err instanceof TimeoutException)
						return Future.failedFuture(new RpcTimeoutException("RPC call timeout"));

					return Future.failedFuture(err);
				});
	}

	private static RpcException rpcErrorToException(RpcError error) {
		return switch (error.getCode()) {
			case RpcErrorCode.RPC_INTERNAL_ERROR -> new RpcInternalException(error.getMessage());
			case RpcErrorCode.MALFORMED_REQUEST -> new MalformedRpcRequestException(error.getMessage());
			case RpcErrorCode.MALFORMED_RESPONSE -> new MalformedRpcResponseException(error.getMessage());
			case RpcErrorCode.INVALID_METHOD -> new InvalidRpcMethodException(error.getMessage());
			case RpcErrorCode.UNIMPLEMENTED_METHOD -> new UnimplementedRpcMethodException(error.getMessage());
			case RpcErrorCode.INVALID_PARAMS -> new InvalidRpcParametersException(error.getMessage());
			case RpcErrorCode.INVALID_RESULT -> new InvalidRpcResultException(error.getMessage());
			case RpcErrorCode.FORBIDDEN -> new ForbiddenRpcRequestException(error.getMessage());
			case RpcErrorCode.TIMEOUT -> new RpcTimeoutException(error.getMessage());

			case RpcErrorCode.SESSION_NOT_EXISTS -> new SessionNotExistsException(error.getMessage());
			case RpcErrorCode.REVOKE_CURRENT_SESSION -> new RevokeCurrentSessionException(error.getMessage());

			case RpcErrorCode.CONTACT_STORE_ERROR -> new ContactStoreException(error.getMessage());
			case RpcErrorCode.REVISION_OUT_DATE -> new RevisionOutdateException(error.getMessage());
			case RpcErrorCode.MALFORMED_MUTATION_DATA -> new MalformedMutationDataException(error.getMessage());
			case RpcErrorCode.CONTACT_NOT_EXISTS -> new ContactNotExistsException(error.getMessage());
			case RpcErrorCode.CONTACT_ALREADY_EXISTS -> new ContactAlreadyExistsException(error.getMessage());

			case RpcErrorCode.CHANNEL_NOT_EXISTS -> new ChannelNotExistsException(error.getMessage());
			case RpcErrorCode.CHANNEL_ALREADY_EXISTS -> new ChannelAlreadyExistsException(error.getMessage());
			case RpcErrorCode.ALREADY_JOINED_CHANNEL -> new AlreadyJoinedException(error.getMessage());
			case RpcErrorCode.INVALID_INVITE_TICKET -> new InvalidInviteTicket(error.getMessage());
			case RpcErrorCode.FORBIDDEN_NON_CHANNEL_MEMBER -> new ForbiddenNonChannelMemberException(error.getMessage());
			case RpcErrorCode.FORBIDDEN_BANNED_CHANNEL_MEMBER -> new ForbiddenBannedMemberException(error.getMessage());
			case RpcErrorCode.CHANNEL_LIMIT_EXCEEDED -> new ChannelLimitExceededException(error.getMessage());
			case RpcErrorCode.CHANNEL_MEMBER_LIMIT_EXCEEDED -> new ChannelMemberLimitExceededException(error.getMessage());

			default -> new RpcException(error.getCode(), error.getMessage());
		};
	}

	private static long nextId() {
		return nextId.getAndIncrement();
	}

	public static RpcCall<List<SessionInfo>> listSessions() {
		return new RpcCall<>(RpcRequest.listSessions(nextId()));
	}

	public static RpcCall<Void> revokeSession(Id deviceId) {
		return new RpcCall<>(RpcRequest.revokeSession(nextId(), deviceId));
	}

	public static RpcCall<Integer> contactMutate(ContactMutation mutation) {
		return new RpcCall<>(RpcRequest.contactMutate(nextId(), mutation));
	}

	public static RpcCall<ChannelInfo> createChannel(NewChannelInfo params) {
		return new RpcCall<>(RpcRequest.createChannel(nextId(), params));
	}

	public static RpcCall<Void> deleteChannel() {
		return new RpcCall<>(RpcRequest.deleteChannel(nextId()));
	}

	public static RpcCall<ChannelInfo> joinChannel(InviteTicket ticket) {
		return new RpcCall<>(RpcRequest.joinChannel(nextId(), ticket));
	}

	public static RpcCall<Void> leaveChannel() {
		return new RpcCall<>(RpcRequest.leaveChannel(nextId()));
	}

	public static RpcCall<Void> transferChannelOwnership(Id newOwner) {
		return new RpcCall<>(RpcRequest.transferChannelOwnership(nextId(), newOwner));
	}

	public static RpcCall<Void> rotateChannelSessionKey(ChannelSessionKeyRotation params) {
		return new RpcCall<>(RpcRequest.rotateChannelSessionKey(nextId(), params));
	}

	public static RpcCall<Void> updateChannelInfo(JsonNode changes) {
		return new RpcCall<>(RpcRequest.updateChannelInfo(nextId(), changes));
	}

	public static RpcCall<List<Id>> updateChannelMembersRole(ChannelMembersRole params) {
		return new RpcCall<>(RpcRequest.updateChannelMembersRole(nextId(), params));
	}

	public static RpcCall<List<Id>> banChannelMembers(Collection<Id> memberIds) {
		return new RpcCall<>(RpcRequest.banChannelMembers(nextId(), memberIds));
	}

	public static RpcCall<List<Id>> unbanChannelMembers(Collection<Id> memberIds) {
		return new RpcCall<>(RpcRequest.unbanChannelMembers(nextId(), memberIds));
	}

	public static RpcCall<List<Id>> removeChannelMembers(Collection<Id> memberIds) {
		return new RpcCall<>(RpcRequest.removeChannelMembers(nextId(), memberIds));
	}

	public static RpcCall<ChannelInfo> getChannelInfo() {
		return new RpcCall<>(RpcRequest.getChannelInfo(nextId()));
	}
}