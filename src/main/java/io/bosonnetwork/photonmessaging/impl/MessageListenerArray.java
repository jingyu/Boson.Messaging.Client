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

import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;

import io.bosonnetwork.photonmessaging.Message;
import io.bosonnetwork.photonmessaging.MessageDeliveryReceipt;
import io.bosonnetwork.photonmessaging.MessageListener;

public class MessageListenerArray extends CopyOnWriteArrayList<MessageListener> implements MessageListener {
	private static final long serialVersionUID = 4311687361012466140L;
	private final Logger log;

	public MessageListenerArray(MessageListener listener, Logger log) {
		super();
		this.log = log;
		add(listener);
	}

	@Override
	public void onMessage(Message message) {
		for (MessageListener listener : this) {
			try {
				listener.onMessage(message);
			} catch (Throwable t) {
				log.error("Error dispatching onMessage to listener: {}", listener, t);
			}
		}
	}

	@Override
	public void onSent(Message message) {
		for (MessageListener listener : this) {
			try {
				listener.onSent(message);
			} catch (Throwable t) {
				log.error("Error dispatching onSent to listener: {}", listener, t);
			}
		}
	}
	@Override
	public void onDeliveryReceipt(MessageDeliveryReceipt receipt) {
		for (MessageListener listener : this) {
			try { listener.onDeliveryReceipt(receipt); }
			catch (Throwable t) { log.error("Error dispatching delivery failure to listener: {}", listener, t); }
		}
	}
}