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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.bosonnetwork.Id;
import io.bosonnetwork.crypto.Random;
import io.bosonnetwork.photonmessaging.Contact;

public class ContactSnapshotMergeTests {
	private static Contact friend(Id id, long updatedAt) {
		return new Friend(id, Random.randomBytes(PhotonContact.ENCRYPTED_SESSION_KEY_BYTES), null, null, null,
				false, false, 1000, updatedAt, 0);
	}

	private static List<Id> ids(List<Contact> contacts) {
		return contacts.stream().map(Contact::getId).toList();
	}

	@Test
	void snapshotFromAServiceAheadRemovesWhatItDoesNotHave() {
		Id kept = Id.random();
		Id changed = Id.random();
		Id stale = Id.random();
		Id added = Id.random();

		PhotonMessagingClient.SnapshotChanges changes = PhotonMessagingClient.snapshotChanges(
				List.of(friend(kept, 2000), friend(changed, 2000), friend(stale, 2000)),
				List.of(friend(kept, 2000), friend(changed, 3000), friend(added, 3000)));

		assertEquals(List.of(stale), changes.removed());
		assertEquals(List.of(added), ids(changes.added()));
		assertEquals(List.of(changed), ids(changes.updated()));
	}

	@Test
	void snapshotFromAServiceBehindKeepsEveryDeviceContact() {
		Id deviceOnly = Id.random();
		Id serviceOnly = Id.random();
		Id deviceNewer = Id.random();
		Id serviceNewer = Id.random();
		Id same = Id.random();

		PhotonMessagingClient.Reconciliation plan = PhotonMessagingClient.reconciliation(
				List.of(friend(deviceOnly, 2000), friend(deviceNewer, 5000), friend(serviceNewer, 2000), friend(same, 2000)),
				List.of(friend(serviceOnly, 2000), friend(deviceNewer, 3000), friend(serviceNewer, 4000), friend(same, 2000)));

		assertEquals(List.of(deviceOnly), ids(plan.uploadAdds()));
		assertEquals(List.of(deviceNewer), ids(plan.uploadUpdates()));
		assertEquals(5000, plan.uploadUpdates().get(0).getUpdatedAt());

		// A tie keeps the service's copy, without an update event for it.
		assertEquals(List.of(serviceOnly, serviceNewer, same), ids(plan.fromService()));
		assertEquals(List.of(serviceOnly), ids(plan.added()));
		assertEquals(List.of(serviceNewer), ids(plan.updated()));
	}

	@Test
	void snapshotFromAnEmptyServiceUploadsEverything() {
		Id first = Id.random();
		Id second = Id.random();

		PhotonMessagingClient.Reconciliation plan = PhotonMessagingClient.reconciliation(
				List.of(friend(first, 2000), friend(second, 2000)), List.of());

		assertEquals(2, plan.uploadAdds().size());
		assertTrue(plan.uploadAdds().stream().map(Contact::getId).toList().containsAll(List.of(first, second)));
		assertTrue(plan.fromService().isEmpty());
		assertTrue(plan.uploadUpdates().isEmpty());
	}
}
