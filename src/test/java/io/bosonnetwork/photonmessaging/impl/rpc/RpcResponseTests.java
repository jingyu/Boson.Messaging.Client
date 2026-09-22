package io.bosonnetwork.photonmessaging.impl.rpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.bosonnetwork.Id;
import io.bosonnetwork.crypto.Random;
import io.bosonnetwork.json.Json;
import io.bosonnetwork.photonmessaging.Channel;
import io.bosonnetwork.photonmessaging.SessionInfo;
import io.bosonnetwork.photonmessaging.impl.ContactMutation;
import io.bosonnetwork.photonmessaging.impl.ContactSync;
import io.bosonnetwork.photonmessaging.impl.dto.ChannelInfo;

public class RpcResponseTests {
	private static long nextId() {
		return Random.random().nextLong();
	}

	private static Stream<Arguments> responseProvider() {
		List<Arguments> responses = new ArrayList<>();

		ChannelInfo channelInfo = new ChannelInfo(Id.random(), Id.random(), Id.random(), Random.randomBytes(64),
				Channel.Permission.MODERATOR_INVITE, "Foo Bar", "Hello world", true,
				System.currentTimeMillis(), System.currentTimeMillis(), null);

		responses.add(Arguments.of(RpcMethod.SESSION_LIST,
				RpcResponse.listSessions(nextId(), List.of(new SessionInfo(Id.random(), true, System.currentTimeMillis(), "203.0.113.5")))));
		responses.add(Arguments.of(RpcMethod.SESSION_REVOKE,
				RpcResponse.revokeSession(nextId())));
		responses.add(Arguments.of(RpcMethod.CONTACT_MUTATE,
				RpcResponse.contactMutate(nextId(), 67)));
		responses.add(Arguments.of(RpcMethod.CONTACT_SYNC,
				RpcResponse.contactSync(nextId(), ContactSync.upToDate(42))));
		responses.add(Arguments.of(RpcMethod.CONTACT_SYNC,
				RpcResponse.contactSync(nextId(), ContactSync.delta(11, List.of(ContactMutation.clear(10))))));
		responses.add(Arguments.of(RpcMethod.CHANNEL_CREATE,
				RpcResponse.createChannel(nextId(), channelInfo)));
		responses.add(Arguments.of(RpcMethod.CHANNEL_DELETE,
				RpcResponse.deleteChannel(nextId())));
		responses.add(Arguments.of(RpcMethod.CHANNEL_OWNERSHIP_TRANSFER,
				RpcResponse.transferChannelOwnership(nextId())));
		responses.add(Arguments.of(RpcMethod.CHANNEL_SESSION_KEY_ROTATE,
				RpcResponse.rotateChannelSessionKey(nextId())));
		responses.add(Arguments.of(RpcMethod.CHANNEL_MEMBERS_ROLE_UPDATE,
				RpcResponse.updateChannelMembersRole(nextId(), List.of(Id.random(), Id.random()))));
		responses.add(Arguments.of(RpcMethod.CHANNEL_MEMBERS_BAN,
				RpcResponse.banChannelMembers(nextId(), List.of(Id.random(), Id.random()))));
		responses.add(Arguments.of(RpcMethod.CHANNEL_MEMBERS_UNBAN,
				RpcResponse.unbanChannelMembers(nextId(), List.of(Id.random()))));
		responses.add(Arguments.of(RpcMethod.CHANNEL_MEMBERS_REMOVE,
				RpcResponse.removeChannelMembers(nextId(), List.of(Id.random(), Id.random()))));
		responses.add(Arguments.of(RpcMethod.CHANNEL_JOIN,
				RpcResponse.joinChannel(nextId(), channelInfo)));
		responses.add(Arguments.of(RpcMethod.CHANNEL_LEAVE,
				RpcResponse.leaveChannel(nextId())));
		responses.add(Arguments.of(RpcMethod.CHANNEL_INFO,
				RpcResponse.getChannelInfo(nextId(), channelInfo)));
		responses.add(Arguments.of(RpcMethod.CHANNEL_JOIN,
				RpcResponse.error(nextId(), RpcMethod.CHANNEL_JOIN, 10000, "Test error")));

		return responses.stream();
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("responseProvider")
	void testSerialization(RpcMethod method, RpcResponse response) {
		assertEquals(method, response.getMethod()); // make sure the test data is correct
		System.out.println(Json.toString(response));
		RpcResponse parsed = RpcResponse.parse(response.serialize());

		assertEquals(response.getId(), parsed.getId());
		assertEquals(response.getMethod(), parsed.getMethod());

		Object result = response.getResult();
		Object parsedResult = parsed.getResult();
		assertThat(parsedResult)
				.usingRecursiveComparison()
				.withComparatorForType(Id::compare, Id.class)
				.isEqualTo(result);

		RpcError error = response.getError();
		assertEquals(error, parsed.getError());

		assertEquals(response.failed(), parsed.failed());
		assertEquals(response.succeeded(), parsed.succeeded());
	}
}