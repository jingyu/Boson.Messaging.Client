package io.bosonnetwork.photonmessaging.impl.rpc;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.EnumSet;
import java.util.Set;

import org.junit.jupiter.api.Test;

public class RpcMethodTests {
	@Test
	void testValueOf() {
		Set<RpcMethod> methods = EnumSet.allOf(RpcMethod.class);

		for (RpcMethod method : methods) {
			RpcMethod m = RpcMethod.valueOf(method.value());
			assertSame(method, m);
		}
	}

	@Test
	void testMethodCategory() {
		Set<RpcMethod> methods = EnumSet.allOf(RpcMethod.class);

		for (RpcMethod method : methods) {
			switch (method) {
				case SESSION_LIST, SESSION_REVOKE, CONTACT_MUTATE, CONTACT_SYNC, CHANNEL_CREATE -> {
					assertTrue(method.isServiceRpc());
					assertFalse(method.isChannelRpc());
					assertFalse(method.isOwnerPrivilegedChannelRpc());
					assertFalse(method.isModeratorPrivilegedChannelRpc());
					assertFalse(method.isUnprivilegedChannelRpc());
				}

				case CHANNEL_DELETE, CHANNEL_OWNERSHIP_TRANSFER, CHANNEL_SESSION_KEY_ROTATE, CHANNEL_INFO_UPDATE -> {
					assertFalse(method.isServiceRpc());
					assertTrue(method.isChannelRpc());
					assertTrue(method.isOwnerPrivilegedChannelRpc());
					assertFalse(method.isModeratorPrivilegedChannelRpc());
					assertFalse(method.isUnprivilegedChannelRpc());
				}

				case CHANNEL_MEMBERS_ROLE_UPDATE, CHANNEL_MEMBERS_BAN, CHANNEL_MEMBERS_UNBAN, CHANNEL_MEMBERS_REMOVE  -> {
					assertFalse(method.isServiceRpc());
					assertTrue(method.isChannelRpc());
					assertFalse(method.isOwnerPrivilegedChannelRpc());
					assertTrue(method.isModeratorPrivilegedChannelRpc());
					assertFalse(method.isUnprivilegedChannelRpc());
				}

				case CHANNEL_JOIN, CHANNEL_LEAVE, CHANNEL_INFO -> {
					assertFalse(method.isServiceRpc());
					assertTrue(method.isChannelRpc());
					assertFalse(method.isOwnerPrivilegedChannelRpc());
					assertFalse(method.isModeratorPrivilegedChannelRpc());
					assertTrue(method.isUnprivilegedChannelRpc());
				}
			}
		}
	}
}