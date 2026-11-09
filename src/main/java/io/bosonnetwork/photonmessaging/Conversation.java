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

import java.util.Optional;

import io.bosonnetwork.Id;

/**
 * Represents a messaging conversation between the local user and a participant.
 */
public interface Conversation extends Comparable<Conversation> {
	/**
	 * Returns the unique identifier of the conversation, which is the same as the participant's ID.
	 *
	 * @return the conversation ID
	 */
	default Id getId() {
		return getContact().getId();
	}

	/**
	 * Returns the participant involved in this conversation.
	 *
	 * @return the contact representing the participant
	 */
	Contact getContact();

	/**
	 * Checks if the conversation is a channel-type conversation.
	 *
	 * @return {@code true} if the conversation is a channel; {@code false} otherwise
	 */
	boolean isChannel();

	/**
	 * Returns the most recent message, including its last known delivery outcome.
	 *
	 * @return the latest message, or empty when the conversation has no message
	 */
	Optional<Message> getLastMessage();

	/**
	 * Returns a short text preview of the most recent message or activity in the conversation.
	 *
	 * @return an {@link Optional} holding the preview text, or an empty {@code Optional} if the
	 *         conversation has no message to preview yet.
	 */
	Optional<String> getPreview();

	/**
	 * Returns the timestamp of when the conversation was last updated.
	 *
	 * @return the update timestamp in milliseconds
	 */
	long getUpdatedAt();
}
