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

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.jspecify.annotations.Nullable;

import io.bosonnetwork.Id;

/**
 * Represents a delivery receipt for a message.
 * <p>
 * This receipt indicates whether a message was successfully delivered or failed
 * to be delivered. It contains the identifier of the message, the delivery status,
 * an optional reason describing a failure, and the timestamp when the receipt
 * was generated.
 */
public class MessageDeliveryReceipt {
	@JsonProperty("id")
	private final Id messageId;
	@JsonProperty("s")
	private final DeliveryStatus status;
	@JsonProperty("r")
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private final @Nullable String reason;
	@JsonProperty("t")
	private final long timestamp;

	/**
	 * Constructs a new message delivery receipt with the specified message ID, delivery status, reason, and timestamp.
	 *
	 * @param messageId the ID of the message being delivered
	 * @param status    the delivery status of the message
	 * @param reason    the reason for the delivery failure, if applicable
	 * @param timestamp the timestamp of the delivery attempt
	 */
	@JsonCreator
	public MessageDeliveryReceipt(@JsonProperty(value = "id", required = true) Id messageId,
									 @JsonProperty(value = "s", required = true) DeliveryStatus status,
									 @JsonProperty(value = "r") @Nullable String reason,
									 @JsonProperty(value = "t", required = true) long timestamp) {
		this.messageId = Objects.requireNonNull(messageId);
		this.status = Objects.requireNonNull(status);
		this.reason = reason;
		this.timestamp = timestamp;
	}

	/**
	 * Returns the ID of the message being delivered.
	 *
	 * @return the ID of the message being delivered
	 */
	public Id getMessageId() {
		return messageId;
	}

	/**
	 * Returns the delivery status of the message.
	 *
	 * @return the delivery status of the message
	 */
	public DeliveryStatus getStatus() {
		return status;
	}

	/**
	 * Returns the reason for the delivery failure, if applicable.
	 *
	 * @return the reason for the delivery failure, or null if the message was delivered successfully
	 */
	public @Nullable String getReason() {
		return reason;
	}

	/**
	 * Returns the timestamp of the delivery attempt.
	 *
	 * @return the timestamp of the delivery attempt
	 */
	public long getTimestamp() {
		return timestamp;
	}
}