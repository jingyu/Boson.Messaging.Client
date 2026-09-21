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

package io.bosonnetwork.photonmessaging;

import org.jspecify.annotations.Nullable;

import io.bosonnetwork.Id;

/**
 * Represents a friend request in the messaging system.
 * A friend request captures the negotiation between two users to establish
 * a friendship, including the initiator, any greeting message, and its status.
 */
public interface FriendRequest {
	/**
	 * Returns the identifier of the user involved in the friend request (the other side).
	 *
	 * @return the user {@link Id}.
	 */
	Id getUserId();

	/**
	 * Returns the identifier of the user who initiated the friend request.
	 *
	 * @return the initiator's {@link Id}.
	 */
	Id getInitiatorId();

	/**
	 * Returns the greeting message (hello message) sent with the request.
	 *
	 * @return the greeting message, or {@code null} if none was provided.
	 */
	@Nullable String getHello();

	/**
	 * Checks whether this is an outgoing request, one the current user sent.
	 * <p>
	 * A request is outgoing when the current user is its initiator and incoming when the other user
	 * is. Since {@link #getUserId()} is always the other user, the current user initiated the request
	 * exactly when the initiator is not the other user.
	 * </p>
	 *
	 * @return {@code true} if the current user sent the request, {@code false} if it was received.
	 */
	default boolean isOutgoing() {
		return !getInitiatorId().equals(getUserId());
	}

	/**
	 * Checks if the friend request has been accepted.
	 * <p>
	 * Accepted is a final state: an accepted request cannot be accepted again, and only a new
	 * request between the same two users (or removing the record) changes it.
	 * </p>
	 *
	 * @return {@code true} if the request was accepted; {@code false} otherwise.
	 */
	boolean isAccepted();

	/**
	 * Checks if the friend request has expired.
	 * <p>
	 * A request that is still pending a week after it was sent expires. Both sides count the week
	 * from the moment it was sent. Expired is a final state: an expired request can no longer be
	 * accepted, and only a new request between the same two users (or removing the record) changes it.
	 * </p>
	 *
	 * @return {@code true} if the request is expired; {@code false} otherwise.
	 */
	boolean isExpired();

	/**
	 * Returns the timestamp when the friend request was created.
	 *
	 * @return the creation timestamp in milliseconds since the epoch.
	 */
	long getCreatedAt();

	/**
	 * Returns the timestamp when the friend request was accepted.
	 *
	 * @return the acceptance timestamp in milliseconds since the epoch, or 0 if not accepted.
	 */
	long getAcceptedAt();

	/**
	 * Returns the timestamp when the friend request was last updated.
	 *
	 * @return the update timestamp in milliseconds since the epoch.
	 */
	long getUpdatedAt();
}
