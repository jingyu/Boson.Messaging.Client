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

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import io.bosonnetwork.photonmessaging.impl.PhotonMessagingClient.MutationOrder;

public class ContactSyncOrderTests {
	@Test
	void appliesTheMutationMadeAgainstTheLocalRevision() {
		assertEquals(MutationOrder.APPLY, PhotonMessagingClient.mutationOrder(7, 7));
		assertEquals(MutationOrder.APPLY, PhotonMessagingClient.mutationOrder(0, 0));
	}

	// A redelivered notification, or a change stored for the device that its connect-time sync
	// already included.
	@Test
	void skipsTheMutationMadeAgainstAnOlderRevision() {
		assertEquals(MutationOrder.ALREADY_APPLIED, PhotonMessagingClient.mutationOrder(6, 7));
		assertEquals(MutationOrder.ALREADY_APPLIED, PhotonMessagingClient.mutationOrder(0, 7));
	}

	// Applying it would jump the revision over the missed changes and lose them for good.
	@Test
	void reportsAGapForTheMutationMadeAgainstANewerRevision() {
		assertEquals(MutationOrder.GAP, PhotonMessagingClient.mutationOrder(8, 7));
		assertEquals(MutationOrder.GAP, PhotonMessagingClient.mutationOrder(1, 0));
	}
}
