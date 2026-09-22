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

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The RPC methods supported by the photon messaging service.
 */
public enum RpcMethod {
	// Service RPC methods
	/**
	 * Lists all active user sessions.
	 */
	@JsonProperty("sl")
	SESSION_LIST,

	/**
	 * Revokes a user session.
	 */
	@JsonProperty("sr")
	SESSION_REVOKE,

	/**
	 * Update the contact mutations.
	 */
	@JsonProperty("cm")
	CONTACT_MUTATE,

	/**
	 * Fetches the contact changes since a revision (a delta or a snapshot), for a device that
	 * missed a contact sync notification.
	 */
	@JsonProperty("cs")
	CONTACT_SYNC,

	/**
	 * Creates a channel.
	 */
	@JsonProperty("cc")
	CHANNEL_CREATE,

	// Channel RPC methods
	// Privileged channel RPC methods
	/**
	 * Deletes the channel.
	 */
	@JsonProperty("cd")
	CHANNEL_DELETE,

	/**
	 * Transfers channel ownership.
	 */
	@JsonProperty("cot")
	CHANNEL_OWNERSHIP_TRANSFER,

	/**
	 * Rotates the channel session key.
	 */
	@JsonProperty("csr")
	CHANNEL_SESSION_KEY_ROTATE,

	/**
	 * Updates the channel info.
	 */
	@JsonProperty("ciu")
	CHANNEL_INFO_UPDATE,

	/**
	 * Updates channel members' roles.
	 */
	@JsonProperty("cru")
	CHANNEL_MEMBERS_ROLE_UPDATE,

	/**
	 * Bans channel members.
	 */
	@JsonProperty("cmb")
	CHANNEL_MEMBERS_BAN,

	/**
	 * Unbans channel members.
	 */
	@JsonProperty("cmu")
	CHANNEL_MEMBERS_UNBAN,

	/**
	 * Removes channel members.
	 */
	@JsonProperty("cmr")
	CHANNEL_MEMBERS_REMOVE,

	// Unprivileged channel RPC methods
	/**
	 * Joins the channel.
	 */
	@JsonProperty("cj")
	CHANNEL_JOIN,

	/**
	 * Leaves the channel.
	 */
	@JsonProperty("cl")
	CHANNEL_LEAVE,

	/**
	 * Retrieves channel information.
	 */
	@JsonProperty("ci")
	CHANNEL_INFO;

	/**
	 * Returns the serialized integer value of this RPC method.
	 *
	 * @return the integer value associated with this method
	 */
	public int value() {
		return ordinal() + 1;
	}

	/**
	 * Resolves an RPC method from its serialized integer value.
	 *
	 * @param value the integer value to resolve
	 * @return the corresponding RPC method
	 * @throws IllegalArgumentException if the value does not map to any RPC method
	 */
	public static RpcMethod valueOf(int value) {
		if (value <= 0 || value > values().length)
			throw new IllegalArgumentException("Invalid method: " + value);

		return values()[value - 1];
	}

	/**
	 * Returns whether this method is a service RPC method.
	 *
	 * @return {@code true} if this method is in the service RPC range; {@code false} otherwise
	 */
	public boolean isServiceRpc() {
		return compareTo(CHANNEL_CREATE) <= 0;
	}

	/**
	 * Returns whether this method is a channel RPC method.
	 *
	 * @return {@code true} if this method is in the channel RPC range; {@code false} otherwise
	 */
	public boolean isChannelRpc() {
		return compareTo(CHANNEL_DELETE) >= 0 && compareTo(CHANNEL_INFO) <= 0;
	}

	/**
	 * Determines whether the current RPC method is an owner-privileged channel RPC.
	 *
	 * @return {@code true} if the method is in the owner-privileged channel RPC range;
	 *         {@code false} otherwise
	 */
	public boolean isOwnerPrivilegedChannelRpc() {
		return compareTo(CHANNEL_DELETE) >= 0 && compareTo(CHANNEL_INFO_UPDATE) <= 0;
	}

	/**
	 * Determines whether the current RPC method is a moderator-privileged channel RPC.
	 *
	 * @return {@code true} if the method is in the moderator-privileged channel RPC range;
	 *         {@code false} otherwise
	 */
	public boolean isModeratorPrivilegedChannelRpc() {
		return compareTo(CHANNEL_MEMBERS_ROLE_UPDATE) >= 0 && compareTo(CHANNEL_MEMBERS_REMOVE) <= 0;
	}

	/**
	 * Returns whether this method is an unprivileged channel RPC method.
	 *
	 * @return {@code true} if this method is in the unprivileged channel RPC range; {@code false} otherwise
	 */
	public boolean isUnprivilegedChannelRpc() {
		return compareTo(CHANNEL_JOIN) >= 0 && compareTo(CHANNEL_INFO) <= 0;
	}
}