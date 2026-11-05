package io.bosonnetwork.photonmessaging.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.bosonnetwork.Id;
import io.bosonnetwork.crypto.Random;
import io.bosonnetwork.json.Json;
import io.bosonnetwork.photonmessaging.Channel;
import io.bosonnetwork.photonmessaging.DeliveryStatus;
import io.bosonnetwork.photonmessaging.MessageDeliveryReceipt;
import io.bosonnetwork.photonmessaging.SessionInfo;
import io.bosonnetwork.photonmessaging.impl.dto.ChannelInfo;
import io.bosonnetwork.photonmessaging.impl.dto.ChannelMembersRole;

public class NotificationTests {
	@Test
	void testNotificationEvents() {
		Set<Notification.Event> events = EnumSet.allOf(Notification.Event.class);

		for (Notification.Event event : events) {
			Notification.Event e = Notification.Event.valueOf(event.value());
			assertSame(event, e);
		}
	}

	private static Stream<Arguments> notificationProvider() {
		List<Arguments> notifications = new ArrayList<>();

		ChannelInfo channelInfo = new ChannelInfo(Id.random(), Id.random(),
				Id.random(), Random.randomBytes(64), Channel.Permission.MEMBER_INVITE, "Test Channel", null,
				false, System.currentTimeMillis(), System.currentTimeMillis(), null);

		ContactMutation mutation = ContactMutation.remove(9, List.of(Id.random(), Id.random()));

		ChannelMembersRole membersRole = new ChannelMembersRole(
				List.of(Id.random(), Id.random()), Channel.Role.MODERATOR);

		notifications.add(Arguments.of(Notification.Event.SESSION_NEW,
				new Notification(Id.random(), Id.random(), System.currentTimeMillis(), Notification.Event.SESSION_NEW,
						new SessionInfo(Id.random(), true, System.currentTimeMillis(), "203.0.113.8"))));

		notifications.add(Arguments.of(Notification.Event.CONTACT_SYNC,
				new Notification(Id.random(), Id.random(), System.currentTimeMillis(), Notification.Event.CONTACT_SYNC,
						new ContactSync(10, ContactSync.Type.DELTA, List.of(mutation), null))));

		notifications.add(Arguments.of(Notification.Event.MESSAGE_DELIVERY_RECEIPT,
				new Notification(Id.random(), Id.random(), System.currentTimeMillis(), Notification.Event.MESSAGE_DELIVERY_RECEIPT,
						new MessageDeliveryReceipt(Id.random(), DeliveryStatus.RECIPIENT_UNRESOLVED, null, 123456))));

		notifications.add(Arguments.of(Notification.Event.CHANNEL_CREATE,
				new Notification(Id.random(), Id.random(), System.currentTimeMillis(), Notification.Event.CHANNEL_CREATE, channelInfo)));
		notifications.add(Arguments.of(Notification.Event.CHANNEL_DELETE,
				new Notification(Id.random(), Id.random(), System.currentTimeMillis(), Notification.Event.CHANNEL_DELETE, null)));
		notifications.add(Arguments.of(Notification.Event.CHANNEL_JOIN,
				new Notification(Id.random(), Id.random(), System.currentTimeMillis(), Notification.Event.CHANNEL_JOIN, channelInfo)));
		notifications.add(Arguments.of(Notification.Event.CHANNEL_LEAVE,
				new Notification(Id.random(), Id.random(), System.currentTimeMillis(), Notification.Event.CHANNEL_LEAVE, null)));
		notifications.add(Arguments.of(Notification.Event.CHANNEL_OWNERSHIP_TRANSFER,
				new Notification(Id.random(), Id.random(), System.currentTimeMillis(), Notification.Event.CHANNEL_OWNERSHIP_TRANSFER, Id.random())));
		notifications.add(Arguments.of(Notification.Event.CHANNEL_SESSION_KEY_ROTATE,
				new Notification(Id.random(), Id.random(), System.currentTimeMillis(), Notification.Event.CHANNEL_SESSION_KEY_ROTATE, Random.randomBytes(64))));
		notifications.add(Arguments.of(Notification.Event.CHANNEL_INFO_UPDATE,
				new Notification(Id.random(), Id.random(), System.currentTimeMillis(), Notification.Event.CHANNEL_INFO_UPDATE,
						Json.cborMapper().createObjectNode().put("test", "value").put("foo", 123))));
		notifications.add(Arguments.of(Notification.Event.CHANNEL_MEMBERS_ROLE_UPDATE,
				new Notification(Id.random(), Id.random(), System.currentTimeMillis(), Notification.Event.CHANNEL_MEMBERS_ROLE_UPDATE, membersRole)));
		notifications.add(Arguments.of(Notification.Event.CHANNEL_MEMBERS_BAN,
				new Notification(Id.random(), Id.random(), System.currentTimeMillis(), Notification.Event.CHANNEL_MEMBERS_BAN, List.of(Id.random(), Id.random()))));
		notifications.add(Arguments.of(Notification.Event.CHANNEL_MEMBERS_UNBAN,
				new Notification(Id.random(), Id.random(), System.currentTimeMillis(), Notification.Event.CHANNEL_MEMBERS_UNBAN, List.of(Id.random()))));
		notifications.add(Arguments.of(Notification.Event.CHANNEL_MEMBERS_REMOVE,
				new Notification(Id.random(), Id.random(), System.currentTimeMillis(), Notification.Event.CHANNEL_MEMBERS_REMOVE, List.of(Id.random(), Id.random()))));
		notifications.add(Arguments.of(Notification.Event.CHANNEL_MEMBER_JOIN,
				new Notification(Id.random(), Id.random(), System.currentTimeMillis(), Notification.Event.CHANNEL_MEMBER_JOIN,
						new ChannelMember(Id.random(), Channel.Role.MEMBER, System.currentTimeMillis()))));
		notifications.add(Arguments.of(Notification.Event.CHANNEL_MEMBER_LEAVE,
				new Notification(Id.random(), Id.random(), System.currentTimeMillis(), Notification.Event.CHANNEL_MEMBER_LEAVE, Id.random())));

		return notifications.stream();
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("notificationProvider")
	void testSerialization(Notification.Event event, Notification notif) {
		assertEquals(event, notif.getEvent()); // make sure the test data is correct
		System.out.println(Json.toString(notif));

		Notification parsed = Notification.parse(notif.serialize());

		assertEquals(notif.getId(), parsed.getId());
		assertEquals(notif.getSource(), parsed.getSource());
		assertEquals(notif.getTimestamp(), parsed.getTimestamp());
		assertEquals(event, parsed.getEvent());

		Object body = notif.getBody();
		Object parsedBody = parsed.getBody();
		assertThat(parsedBody)
				.usingRecursiveComparison()
				.withComparatorForType(Id::compare, Id.class)
				.isEqualTo(body);
	}
}