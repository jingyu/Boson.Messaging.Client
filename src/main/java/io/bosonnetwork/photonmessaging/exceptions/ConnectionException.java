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

package io.bosonnetwork.photonmessaging.exceptions;

/**
 * Represents an exception that occurs when a connection error is encountered
 * within the messaging system. This exception serves as a specialized subtype
 * of {@link MessagingException}, intended to signal issues specifically
 * related to connection failures.
 */
public class ConnectionException extends MessagingException {
	private static final long serialVersionUID = 8535651073237001959L;

	/**
	 * The specific, unrecoverable reason the messaging server refused the connection. Lets callers
	 * react without parsing the human-readable detail message (which is subject to change).
	 */
	public enum Reason {
		/** The server rejected the client's MQTT protocol version; the client is likely outdated. */
		UNACCEPTABLE_PROTOCOL_VERSION,
		/** The server rejected the supplied credentials. */
		INVALID_CREDENTIALS,
		/** The client authenticated but is not authorized to establish this session. */
		NOT_AUTHORIZED,
		/**
		 * The user already holds the maximum number of concurrent messaging sessions (each running
		 * client instance counts as one session). Another session must be closed before retrying.
		 */
		SESSION_LIMIT_EXCEEDED,
		/** The connection failed for an unrecoverable reason that was not further classified. */
		UNKNOWN,
	}

	/**
	 * The classified reason the server refused the connection, or {@link Reason#UNKNOWN} for an
	 * unclassified failure.
	 */
	private final Reason reason;

	/**
	 * Constructs a new {@code ConnectionException} with the specified detail message.
	 * This exception indicates that a connection-related error has occurred within the
	 * messaging system.
	 *
	 * @param message the detail message describing the reason for the connection failure.
	 */
	public ConnectionException(String message) {
		this(Reason.UNKNOWN, message, null);
	}

	/**
	 * Constructs a new {@code ConnectionException} with the specified cause of the error.
	 * This exception indicates that a connection-related issue has occurred
	 * within the messaging system.
	 *
	 * @param cause the underlying cause of the connection failure (saved for later retrieval
	 *              by the {@link #getCause()} method). A {@code null} value indicates that
	 *              the cause is nonexistent or unknown.
	 */
	public ConnectionException(Throwable cause) {
		this(Reason.UNKNOWN, null, cause);
	}

	/**
	 * Constructs a new {@code ConnectionException} with the specified detail message
	 * and cause. This exception indicates that a connection-related error has occurred
	 * within the messaging system.
	 *
	 * @param message the detail message describing the reason for the connection failure.
	 * @param cause the underlying cause of the connection failure (saved for later retrieval
	 *              by the {@link #getCause()} method). A {@code null} value indicates that
	 *              the cause is nonexistent or unknown.
	 */
	public ConnectionException(String message, Throwable cause) {
		this(Reason.UNKNOWN, message, cause);
	}

	/**
	 * Constructs a new {@code ConnectionException} carrying a classified {@link Reason} together with
	 * the specified detail message and cause.
	 *
	 * @param reason the specific reason the connection was refused. A {@code null} value is treated
	 *               as {@link Reason#UNKNOWN}.
	 * @param message the detail message describing the reason for the connection failure.
	 * @param cause the underlying cause of the connection failure (saved for later retrieval by the
	 *              {@link #getCause()} method).
	 */
	public ConnectionException(Reason reason, String message, Throwable cause) {
		super(message, cause);
		this.reason = reason == null ? Reason.UNKNOWN : reason;
	}

	/**
	 * Returns the classified reason the messaging server refused the connection.
	 *
	 * @return the connection failure reason; never {@code null}.
	 */
	public Reason reason() {
		return reason;
	}
}
