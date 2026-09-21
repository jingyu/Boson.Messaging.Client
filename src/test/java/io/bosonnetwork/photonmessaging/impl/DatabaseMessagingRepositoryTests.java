package io.bosonnetwork.photonmessaging.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import net.datafaker.Faker;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

import io.bosonnetwork.Id;
import io.bosonnetwork.crypto.Random;
import io.bosonnetwork.photonmessaging.Channel;
import io.bosonnetwork.photonmessaging.Contact;
import io.bosonnetwork.photonmessaging.FriendRequest;
import io.bosonnetwork.photonmessaging.Message;
import io.bosonnetwork.photonmessaging.impl.database.PostgresDatabase;
import io.bosonnetwork.photonmessaging.impl.database.SqliteDatabase;
import io.bosonnetwork.utils.FileUtils;

@ExtendWith(VertxExtension.class)
@SuppressWarnings("CodeBlock2Expr")
public class DatabaseMessagingRepositoryTests {
	private static final Path testRoot = Paths.get(System.getProperty("java.io.tmpdir"), "boson");
	private static final Path testDir = testRoot.resolve("photon-messaging-client").resolve("DatabaseMessagingRepositoryTests");

	private static PostgresqlServer pgServer;
	private static MessagingRepository postgres;
	private static MessagingRepository sqlite;
	private static final Faker faker = new Faker();

	private static final List<Arguments> dbs = new ArrayList<>();

	@BeforeAll
	static void setup(Vertx vertx, VertxTestContext context) throws Exception {
		FileUtils.deleteFile(testDir);
		Files.createDirectories(testDir);

		// Initialize Postgres
		Future<Integer> f1;
		try {
			pgServer = PostgresqlServer.start("photon_client", "test", "secret");
		} catch (Exception e) {
			System.err.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
			System.err.println("Start PostgreSQL container failed: " + e.getMessage());
			System.err.println("Check your Docker installation.");
			System.err.println("Skipping Postgres tests.");
			System.err.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
		}

		if (pgServer != null) {
			DatabaseStore pgStore = new PostgresDatabase(pgServer.getDatabaseUri(), 4, "photon_test");
			postgres = new MessagingRepository(pgStore);
			f1 = postgres.initialize(vertx, c -> null)
					.onSuccess(v -> dbs.add(Arguments.of("postgres", postgres)));
		} else {
			f1 = Future.succeededFuture(0);
		}

		// Initialize SQLite
		String sqliteUri = "jdbc:sqlite:" + testDir.resolve("client.db");
		DatabaseStore sqliteStore = new SqliteDatabase(sqliteUri, 1);
		sqlite = new MessagingRepository(sqliteStore);
		Future<Integer> f2 = sqlite.initialize(vertx, c -> null)
				.onSuccess(v -> dbs.add(Arguments.of("sqlite", sqlite)));

		Future.all(f1, f2).onComplete(context.succeedingThenComplete());
	}

	@AfterAll
	static void teardown(VertxTestContext context) {
		List<Future<Void>> futures = new ArrayList<>();
		if (sqlite != null) futures.add(sqlite.close());
		if (postgres != null) futures.add(postgres.close());

		Future.all(futures).onComplete(ar -> {
			if (pgServer != null)
				pgServer.stop();
			context.completeNow();
		});
	}

	static Stream<Arguments> databaseProvider() {
		return dbs.stream();
	}

	@BeforeEach
	void clearDatabase(Vertx vertx, VertxTestContext context) {
		List<Future<Void>> futures = new ArrayList<>();
		for (Arguments arg : dbs) {
			MessagingRepository repo = (MessagingRepository) arg.get()[1];
			futures.add(repo.clearContacts(0)
					.compose(v -> repo.clearFriendRequests())
					.compose(v -> repo.clearMessages()));
		}
		Future.all(futures).onComplete(context.succeedingThenComplete());
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("databaseProvider")
	@Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
	void testContactsRevision(String name, MessagingRepository repo, VertxTestContext context) {
		repo.getContactsRevision().onComplete(context.succeeding(rev -> {
			context.verify(() -> assertEquals(0, rev));
			repo.putContacts(123, List.of()).onComplete(context.succeeding(v -> {
				repo.getContactsRevision().onComplete(context.succeeding(rev2 -> {
					context.verify(() -> assertEquals(123, rev2));
					context.completeNow();
				}));
			}));
		}));
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("databaseProvider")
	@Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
	void testFriendLifecycle(String name, MessagingRepository repo, VertxTestContext context) {
		Id friendId = Id.random();
		Friend friend = new Friend(friendId, Random.randomBytes(PhotonContact.ENCRYPTED_SESSION_KEY_BYTES),
				"Alice", "Remark", null, false, false,
				System.currentTimeMillis(), System.currentTimeMillis(), 1);

		repo.putContactLocally(friend).onComplete(context.succeeding(v1 -> {
			repo.getContact(friendId).onComplete(context.succeeding(c -> {
				context.verify(() -> {
					assertNotNull(c);
					assertTrue(c instanceof Friend);
					assertEquals("Alice", c.getName().orElseThrow());
				});

				repo.removeContactLocally(friendId).onComplete(context.succeeding(v2 -> {
					repo.getContact(friendId).onComplete(context.succeeding(c2 -> {
						context.verify(() -> assertNull(c2));
						context.completeNow();
					}));
				}));
			}));
		}));
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("databaseProvider")
	@Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
	void testChannelAndMembers(String name, MessagingRepository repo, VertxTestContext context) {
		Id channelId = Id.random();
		PhotonChannel channel = new PhotonChannel(channelId, Random.randomBytes(PhotonContact.ENCRYPTED_SESSION_KEY_BYTES),
				Id.random(), Channel.Permission.PUBLIC, "Channel 1", "Notice", false,
				System.currentTimeMillis(), System.currentTimeMillis());

		Id m1 = Id.random();
		Id m2 = Id.random();
		Channel.Member member1 = new ChannelMember(m1, Channel.Role.MEMBER, System.currentTimeMillis());
		Channel.Member member2 = new ChannelMember(m2, Channel.Role.MEMBER, System.currentTimeMillis());

		repo.putContactLocally(channel)
				.compose(v -> repo.putChannelMembers(channelId, List.of(member1, member2)))
				.compose(v -> repo.getAllChannelMembers(channelId))
				.onComplete(context.succeeding(members -> {
					context.verify(() -> assertEquals(2, members.size()));
					
					repo.refillChannelMembers(channelId, List.of(member1))
							.compose(v -> repo.getAllChannelMembers(channelId))
							.onComplete(context.succeeding(members2 -> {
								context.verify(() -> assertEquals(1, members2.size()));
								context.completeNow();
							}));
				}));
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("databaseProvider")
	@Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
	void testMessageHistory(String name, MessagingRepository repo, VertxTestContext context) {
		Id convId = Id.random();
		long baseTime = System.currentTimeMillis();

		// Insert 3 messages with same timestamp to test 'rid' stability
		List<Future<Void>> futures = new ArrayList<>();
		for (int i = 0; i < 3; i++) {
			Id mid = Id.random();
			MessageContent content = MessageContent.text("Msg " + i);
			PhotonMessage<MessageContent> msg = new PhotonMessage<>(mid, Id.random(), Message.Type.CONTENT_MESSAGE, baseTime, content);
			msg.setConversationId(convId);
			futures.add(repo.putMessage(msg));
		}

		Future.all(futures)
				.compose(v -> repo.getMessagesBefore(convId, System.currentTimeMillis(), 10, 0))
				.onComplete(context.succeeding(messages -> {
					context.verify(() -> {
						assertEquals(3, messages.size());
						// Verify rid-based ordering (stable)
						assertTrue(messages.get(0).getRid() < messages.get(1).getRid());
						assertTrue(messages.get(1).getRid() < messages.get(2).getRid());
					});
					context.completeNow();
				}));
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("databaseProvider")
	@Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
	void testConversationJoin(String name, MessagingRepository repo, VertxTestContext context) {
		Id contactId = Id.random();
		Friend friend = new Friend(contactId, Random.randomBytes(PhotonContact.ENCRYPTED_SESSION_KEY_BYTES),
				"Bob", null, null, false, false,
				System.currentTimeMillis(), System.currentTimeMillis(), 1);

		Id msgId = Id.random();
		MessageContent content = MessageContent.text("Last");
		PhotonMessage<MessageContent> msg = new PhotonMessage<>(msgId, Id.random(), Message.Type.CONTENT_MESSAGE, System.currentTimeMillis(), content);
		msg.setConversationId(contactId);

		repo.putContactLocally(friend)
				.compose(v -> repo.putMessage(msg))
				.compose(v -> repo.getConversation(contactId))
				.onComplete(context.succeeding(conv -> {
					context.verify(() -> {
						assertNotNull(conv);
						assertEquals("Bob", conv.getContact().getName().orElseThrow());
						assertEquals(msg.getReceivedAt(), conv.getUpdatedAt());
					});
					context.completeNow();
				}));
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("databaseProvider")
	@Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
	void testCascadeDelete(String name, MessagingRepository repo, VertxTestContext context) {
		Id channelId = Id.random();
		PhotonChannel channel = new PhotonChannel(channelId, Random.randomBytes(PhotonContact.ENCRYPTED_SESSION_KEY_BYTES),
				Id.random(), Channel.Permission.PUBLIC, "Cascade", "Notice", false,
				System.currentTimeMillis(), System.currentTimeMillis());
		Id m1 = Id.random();
		Channel.Member member1 = new ChannelMember(m1, Channel.Role.MEMBER, System.currentTimeMillis());

		repo.putContactLocally(channel)
				.compose(v -> repo.putChannelMembers(channelId, List.of(member1)))
				.compose(v -> {
					PhotonMessage<MessageContent> msg = randomMessage(channelId);
					return repo.putMessage(msg);
				})
				.compose(v -> {
					// Verify members and messages exist
					return repo.getAllChannelMembers(channelId).compose(members -> {
						assertEquals(1, members.size());
						return repo.getMessagesBefore(channelId, System.currentTimeMillis(), 10, 0);
					}).onSuccess(messages -> assertEquals(1, messages.size()));
				})
				.compose(v -> repo.removeContactLocally(channelId))
				.compose(v -> repo.getAllChannelMembers(channelId))
				.compose(members -> {
					context.verify(() -> assertEquals(0, members.size(), "Channel members should be Cascaded deleted"));
					return repo.getMessagesBefore(channelId, System.currentTimeMillis(), 10, 0);
				})
				.onComplete(context.succeeding(messages -> {
					context.verify(() -> assertEquals(0, messages.size(), "Messages should be Cascaded deleted"));
					context.completeNow();
				}));
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("databaseProvider")
	@Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
	void testMessageManagement(String name, MessagingRepository repo, VertxTestContext context) {
		Id convId = Id.random();
		PhotonMessage<MessageContent> m1 = randomMessage(convId);
		PhotonMessage<MessageContent> m2 = randomMessage(convId);
		PhotonMessage<MessageContent> m3 = randomMessage(convId);

		repo.putMessage(m1)
				.compose(v -> repo.putMessage(m2))
				.compose(v -> repo.putMessage(m3))
				.compose(v -> {
					m1.setSentAt(System.currentTimeMillis() + 1000);
					return repo.updateMessageSentTime(m1);
				})
				.compose(v -> repo.getMessagesBefore(convId, System.currentTimeMillis(), 10, 0))
				.compose(messages -> {
					context.verify(() -> {
						assertEquals(3, messages.size());
						Message sentM1 = messages.stream().filter(m -> m.getId().equals(m1.getId())).findFirst().orElseThrow();
						assertEquals(m1.getSentAt(), sentM1.getSentAt());
					});
					return repo.removeMessages(List.of(m1.getRid()));
				})
				.compose(removed -> {
					context.verify(() -> assertTrue(removed));
					return repo.getMessagesBefore(convId, System.currentTimeMillis(), 10, 0);
				})
				.compose(messages -> {
					context.verify(() -> assertEquals(2, messages.size()));
					return repo.removeMessagesByConversation(convId);
				})
				.compose(removed -> {
					context.verify(() -> assertTrue(removed));
					return repo.getMessagesBefore(convId, System.currentTimeMillis(), 10, 0);
				})
				.onComplete(context.succeeding(messages -> {
					context.verify(() -> assertEquals(0, messages.size()));
					context.completeNow();
				}));
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("databaseProvider")
	@Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
	void testChannelManagement(String name, MessagingRepository repo, Vertx vertx, VertxTestContext context) {
		PhotonChannel channel = randomChannel();
		Id c1Id = Id.random();
		Id c2Id = Id.random();
		Channel.Member m1 = new ChannelMember(c1Id, Channel.Role.MEMBER, System.currentTimeMillis());
		Channel.Member m2 = new ChannelMember(c2Id, Channel.Role.MEMBER, System.currentTimeMillis());

		repo.putContactLocally(channel)
				.compose(v -> repo.putChannelMembers(channel.getId(), List.of(m1, m2)))
				.compose(v -> repo.updateChannelMembersRole(channel.getId(), List.of(c1Id), Channel.Role.MODERATOR))
				.compose(success -> {
					context.verify(() -> assertTrue(success));
					return repo.getChannelMember(channel.getId(), c1Id);
				})
				.compose(member -> {
					context.verify(() -> {
						assertNotNull(member);
						assertEquals(Channel.Role.MODERATOR, member.getRole());
					});
					Id newOwner = Id.random();
					return repo.updateChannelOwnership(channel.getId(), channel.getOwnerId(), newOwner);
				})
				.compose(v -> repo.getContact(channel.getId()))
				.compose(c -> {
					context.verify(() -> {
						assertNotNull(c);
						assertEquals(Channel.Permission.PUBLIC, ((Channel) c).getPermission());
					});
					return repo.removeChannelMembers(channel.getId(), List.of(c2Id));
				})
				.compose(success -> {
					context.verify(() -> assertTrue(success));
					return repo.getAllChannelMembers(channel.getId());
				})
				.onComplete(context.succeeding(members -> {
					context.verify(() -> {
						assertTrue(members.size() >= 1);
					});
					context.completeNow();
				}));
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("databaseProvider")
	@Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
	void testConversationManagement(String name, MessagingRepository repo, VertxTestContext context) {
		Id c1Id = Id.random();
		Id c2Id = Id.random();
		Friend f1 = new Friend(c1Id, Random.randomBytes(PhotonContact.ENCRYPTED_SESSION_KEY_BYTES), "Alice", null, null, false, false, System.currentTimeMillis(), System.currentTimeMillis(), 1);
		Friend f2 = new Friend(c2Id, Random.randomBytes(PhotonContact.ENCRYPTED_SESSION_KEY_BYTES), "Bob", null, null, false, false, System.currentTimeMillis(), System.currentTimeMillis(), 1);

		repo.putContactLocally(f1)
				.compose(v -> repo.putContactLocally(f2))
				.compose(v -> repo.putMessage(randomMessage(c1Id)))
				.compose(v -> repo.putMessage(randomMessage(c2Id)))
				.compose(v -> repo.getAllConversations())
				.compose(conversations -> {
					context.verify(() -> assertEquals(2, conversations.size()));
					return repo.removeConversations(List.of(c1Id));
				})
				.compose(success -> {
					context.verify(() -> assertTrue(success));
					return repo.getConversation(c1Id);
				})
				.compose(conv -> {
					context.verify(() -> assertNull(conv));
					return repo.removeConversation(c2Id);
				})
				.compose(success -> {
					context.verify(() -> assertTrue(success));
					return repo.getAllConversations();
				})
				.onComplete(context.succeeding(conversations -> {
					context.verify(() -> assertEquals(0, conversations.size()));
					context.completeNow();
				}));
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("databaseProvider")
	@Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
	void testChannelMemberRetrieval(String name, MessagingRepository repo, VertxTestContext context) {
		PhotonChannel channel = randomChannel();
		Id m1Id = Id.random();
		Id m2Id = Id.random();
		Channel.Member m1 = new ChannelMember(m1Id, Channel.Role.MEMBER, System.currentTimeMillis());
		Channel.Member m2 = new ChannelMember(m2Id, Channel.Role.MEMBER, System.currentTimeMillis());

		repo.putContactLocally(channel)
				.compose(v -> repo.putChannelMembers(channel.getId(), List.of(m1, m2)))
				.compose(v -> repo.getChannelMember(channel.getId(), m1Id))
				.compose(member -> {
					context.verify(() -> {
						assertNotNull(member);
						assertEquals(m1Id, member.getId());
					});
					return repo.getChannelMembers(channel.getId(), List.of(m2Id));
				})
				.onComplete(context.succeeding(members -> {
					context.verify(() -> {
						assertEquals(1, members.size());
						assertEquals(m2Id, members.get(0).getId());
					});
					context.completeNow();
				}));
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("databaseProvider")
	@Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
	void testConversationUpdateOnMessage(String name, MessagingRepository repo, VertxTestContext context) {
		Id convId = Id.random();
		Friend friend = new Friend(convId, Random.randomBytes(PhotonContact.ENCRYPTED_SESSION_KEY_BYTES), "Alice", null, null, false, false, System.currentTimeMillis(), System.currentTimeMillis(), 1);
		PhotonMessage<MessageContent> m1 = randomMessage(convId);
		long time1 = m1.getReceivedAt();

		repo.putContactLocally(friend)
				.compose(v -> repo.putMessage(m1))
				.compose(v -> repo.getConversation(convId))
				.compose(conv -> {
					context.verify(() -> {
						assertNotNull(conv);
						assertEquals(time1, conv.getUpdatedAt());
					});
					PhotonMessage<MessageContent> m2 = randomMessage(convId);
					m2.received(time1 + 5000);
					return repo.putMessage(m2).map(m2.getReceivedAt());
				})
				.compose(time2 -> repo.getConversation(convId).map(conv -> Map.entry(time2, conv)))
				.onComplete(context.succeeding(entry -> {
					context.verify(() -> {
						assertEquals(entry.getKey(), entry.getValue().getUpdatedAt());
					});
					context.completeNow();
				}));
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("databaseProvider")
	@Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
	void testFullContactSyncScenario(String name, MessagingRepository repo, VertxTestContext context) {
		Friend f1 = randomFriend();
		PhotonChannel c1 = randomChannel();
		List<Contact> contacts = List.of(f1, c1);
		int newRevision = 10;

		repo.putContacts(newRevision, contacts)
				.compose(v -> repo.getContactsRevision())
				.compose(rev -> {
					context.verify(() -> assertEquals(newRevision, rev));
					return repo.getAllContacts();
				})
				.onComplete(context.succeeding(all -> {
					context.verify(() -> {
						assertEquals(2, all.size());
						assertTrue(all.stream().anyMatch(c -> c.getId().equals(f1.getId())));
						assertTrue(all.stream().anyMatch(c -> c.getId().equals(c1.getId())));

						Contact fetchedC1 = all.stream().filter(c -> c.getId().equals(c1.getId())).findFirst().orElseThrow();
						assertTrue(fetchedC1 instanceof Channel);
						assertEquals(c1.getOwnerId(), ((Channel) fetchedC1).getOwnerId());
					});
					context.completeNow();
				}));
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("databaseProvider")
	@Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
	void testFriendRequestAcceptanceFlow(String name, MessagingRepository repo, VertxTestContext context) {
		Id userId = Id.random();
		Id initiatorId = Id.random();
		FriendRequest request = new PhotonFriendRequest(userId, initiatorId, faker.lorem().sentence());

		repo.putFriendRequest(request)
				.compose(v -> repo.getFriendRequest(userId))
				.compose(fr -> {
					context.verify(() -> {
						assertNotNull(fr);
						assertTrue(fr.getHello().length() > 0);
					});
					PhotonFriendRequest fri = (PhotonFriendRequest) fr;
					fri.accept();
					return repo.putFriendRequest(fri);
				})
				.compose(v -> {
					Friend friend = new Friend(userId, Random.randomBytes(PhotonContact.ENCRYPTED_SESSION_KEY_BYTES), "Alice", null, null, false, false, System.currentTimeMillis(), System.currentTimeMillis(), 1);
					return repo.putContacts(2, List.of(friend));
				})
				.compose(v -> repo.getContact(userId))
				.onComplete(context.succeeding(contact -> {
					context.verify(() -> {
						assertNotNull(contact);
						assertTrue(contact instanceof Friend);
						assertEquals(userId, contact.getId());
					});
					context.completeNow();
				}));
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("databaseProvider")
	@Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
	void testFriendRequestRoundTrip(String name, MessagingRepository repo, VertxTestContext context) {
		Id userId = Id.random();
		Id initiatorId = Id.random();
		PhotonFriendRequest request = new PhotonFriendRequest(userId, initiatorId, "Hello", 1000, 2000);
		PhotonFriendRequest noHello = new PhotonFriendRequest(Id.random(), initiatorId, null, 1500, 1500);

		repo.getFriendRequest(userId)
				.compose(missing -> {
					context.verify(() -> assertNull(missing));
					return repo.putFriendRequest(request);
				})
				.compose(v -> repo.putFriendRequest(noHello))
				.compose(v -> repo.getFriendRequest(userId))
				.compose(fr -> {
					context.verify(() -> {
						assertNotNull(fr);
						assertEquals(userId, fr.getUserId());
						assertEquals(initiatorId, fr.getInitiatorId());
						assertTrue(fr.isOutgoing());
						assertEquals("Hello", fr.getHello());
						assertEquals(1000, fr.getCreatedAt());
						assertEquals(2000, fr.getUpdatedAt());
						assertFalse(fr.isAccepted());
						assertEquals(0, fr.getAcceptedAt());
					});
					return repo.getFriendRequest(noHello.getUserId());
				})
				.compose(fr -> {
					context.verify(() -> {
						assertNotNull(fr);
						assertNull(fr.getHello());
					});
					// Putting an existing request again updates it in place.
					request.accept(3000);
					return repo.putFriendRequest(request);
				})
				.compose(v -> repo.getFriendRequest(userId))
				.compose(fr -> {
					context.verify(() -> {
						assertNotNull(fr);
						assertTrue(fr.isAccepted());
						assertEquals(3000, fr.getAcceptedAt());
						assertEquals(3000, fr.getUpdatedAt());
						assertEquals(1000, fr.getCreatedAt());
						assertEquals("Hello", fr.getHello());
					});
					// A new request from the other side replaces the accepted outgoing one entirely:
					// direction, greeting, times and state.
					return repo.putFriendRequest(new PhotonFriendRequest(userId, userId, "Hello back", 5000, 5000));
				})
				.compose(v -> repo.getFriendRequest(userId))
				.compose(fr -> {
					context.verify(() -> {
						assertNotNull(fr);
						assertEquals(userId, fr.getInitiatorId());
						assertFalse(fr.isOutgoing());
						assertEquals("Hello back", fr.getHello());
						assertEquals(5000, fr.getCreatedAt());
						assertEquals(5000, fr.getUpdatedAt());
						assertFalse(fr.isAccepted());
						assertEquals(0, fr.getAcceptedAt());
					});
					return repo.getFriendRequests();
				})
				.onComplete(context.succeeding(list -> {
					// Still one record per user.
					context.verify(() -> assertEquals(1,
							list.stream().filter(fr -> fr.getUserId().equals(userId)).count()));
					context.completeNow();
				}));
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("databaseProvider")
	@Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
	void testFriendRequestListAndRemoval(String name, MessagingRepository repo, VertxTestContext context) {
		Id initiatorId = Id.random();
		List<Id> ids = List.of(Id.random(), Id.random(), Id.random(), Id.random());
		List<Future<Void>> puts = new ArrayList<>();
		for (int i = 0; i < ids.size(); i++)
			puts.add(repo.putFriendRequest(new PhotonFriendRequest(ids.get(i), initiatorId, "Hello #" + i, 1000L * (i + 1), 0)));

		Future.all(puts)
				.compose(v -> repo.getFriendRequests())
				.compose(list -> {
					// Newest first.
					context.verify(() -> assertEquals(List.of(ids.get(3), ids.get(2), ids.get(1), ids.get(0)),
							list.stream().map(FriendRequest::getUserId).toList()));
					return repo.removeFriendRequest(ids.get(0));
				})
				.compose(removed -> {
					context.verify(() -> assertTrue(removed));
					return repo.removeFriendRequest(ids.get(0));
				})
				.compose(removed -> {
					context.verify(() -> assertFalse(removed));
					return repo.removeFriendRequests(List.of(ids.get(1), Id.random()));
				})
				.compose(removed -> {
					context.verify(() -> assertTrue(removed));
					return repo.removeFriendRequests(List.of(ids.get(1), Id.random()));
				})
				.compose(removed -> {
					context.verify(() -> assertFalse(removed));
					// Nothing asked, nothing removed.
					return repo.removeFriendRequests(List.of());
				})
				.compose(removed -> {
					context.verify(() -> assertFalse(removed));
					return repo.getFriendRequests();
				})
				.compose(list -> {
					context.verify(() -> assertEquals(List.of(ids.get(3), ids.get(2)),
							list.stream().map(FriendRequest::getUserId).toList()));
					return repo.clearFriendRequests();
				})
				.compose(v -> repo.getFriendRequests())
				.onComplete(context.succeeding(list -> {
					context.verify(() -> assertTrue(list.isEmpty()));
					context.completeNow();
				}));
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("databaseProvider")
	@Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
	void testChannelLifecycleScenario(String name, MessagingRepository repo, VertxTestContext context) {
		PhotonChannel channel = randomChannel();
		Channel.Member owner = new ChannelMember(channel.getOwnerId(), Channel.Role.OWNER, System.currentTimeMillis());

		repo.putContactLocally(channel)
				.compose(v -> repo.putChannelMembers(channel.getId(), List.of(owner)))
				.compose(v -> {
					PhotonMessage<MessageContent> msg = randomMessage(channel.getId());
					return repo.putMessage(msg);
				})
				.compose(v -> repo.getConversation(channel.getId()))
				.onComplete(context.succeeding(conv -> {
					context.verify(() -> {
						assertNotNull(conv);
						assertTrue(conv.getContact() instanceof Channel);
						assertEquals(channel.getName(), conv.getContact().getName());
					});
					context.completeNow();
				}));
	}

	// Helpers
	static Friend randomFriend() {
		return new Friend(Id.random(), Random.randomBytes(PhotonContact.ENCRYPTED_SESSION_KEY_BYTES),
				faker.name().fullName(), faker.name().name(), faker.olympicSport().ancientOlympics(),
				false, false, System.currentTimeMillis(), System.currentTimeMillis(), 1);
	}

	static PhotonChannel randomChannel() {
		return new PhotonChannel(Id.random(), Random.randomBytes(PhotonContact.ENCRYPTED_SESSION_KEY_BYTES),
				Id.random(), Channel.Permission.PUBLIC,
				faker.company().name(), faker.lorem().sentence(),
				false, System.currentTimeMillis(), System.currentTimeMillis());
	}

	static Channel.Member randomMember() {
		return new ChannelMember(Id.random(), Channel.Role.MEMBER, System.currentTimeMillis());
	}

	static PhotonMessage<MessageContent> randomMessage(Id conversationId) {
		Id mid = Id.random();
		long now = System.currentTimeMillis();
		MessageContent content = MessageContent.text(Map.of(), faker.lorem().paragraph());
		PhotonMessage<MessageContent> msg = new PhotonMessage<>(mid, Id.random(), Message.Type.CONTENT_MESSAGE, now, content);
		msg.setConversationId(conversationId);
		return msg;
	}
}