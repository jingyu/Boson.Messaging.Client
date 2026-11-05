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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.bosonnetwork.Id;
import io.bosonnetwork.json.Json;

class MessageDeliveryReceiptTests {
	@Test
	void plainWirePayloadRoundTrips() throws Exception {
		Id message = Id.random();
		Id recipient = Id.random();
		MessageDeliveryReceipt receipt = new MessageDeliveryReceipt(message, DeliveryStatus.RECIPIENT_UNRESOLVED, null, 123456);
		byte[] wire = Json.toBytes(receipt);
		Map<String, Object> json = Json.parse(wire);
		assertEquals(3, json.size());
		assertFalse(json.containsKey("r"));
		MessageDeliveryReceipt restored = Json.parse(wire, MessageDeliveryReceipt.class);
		assertFalse(restored.getClass().isRecord());
		assertEquals(message, restored.getMessageId());
		assertEquals(DeliveryStatus.RECIPIENT_UNRESOLVED, restored.getStatus());
		assertNull(restored.getReason());
		assertEquals(123456, restored.getTimestamp());
	}
}