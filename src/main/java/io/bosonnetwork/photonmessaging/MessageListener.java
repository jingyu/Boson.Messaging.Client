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

/**
 * Listener for message-related events.
 * <p>
 * Events report observed messaging activity. Local persistence is attempted before dispatch where
 * applicable, but a storage failure does not suppress the event. A callback does not guarantee that
 * the message or receipt was saved locally.
 */
public interface MessageListener {
	/**
	 * Called when a new message is received.
	 *
	 * @param message the received message
	 */
	void onMessage(Message message);

	/**
	 * Called when a message has been successfully sent.
	 *
	 * @param message the sent message
	 */
	void onSent(Message message);

	/**
	 * Called when a message delivery receipt is received.
	 *
	 * @param receipt the delivery receipt containing the message delivery status
	 */
	void onDeliveryReceipt(MessageDeliveryReceipt receipt);
}