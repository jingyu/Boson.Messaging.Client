# Boson Messaging Client

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/Java-17%2B-orange.svg)](https://adoptium.net/)
[![Maven](https://img.shields.io/badge/Maven-3.8%2B-red.svg)](https://maven.apache.org/)

The Java client library for **Boson Messaging** (codename *Photon Messaging*) — a federated, end-to-end encrypted instant messaging service built on the Boson DHT network.

---

## Table of Contents

- [What Is Boson Messaging?](#what-is-boson-messaging)
- [How It Works](#how-it-works)
- [Prerequisites](#prerequisites)
- [Build](#build)
- [Adding as a Dependency](#adding-as-a-dependency)
- [Configuration](#configuration)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)

---

## What Is Boson Messaging?

Boson Messaging is a federated instant messaging system with the following properties:

- **End-to-end encryption** — message payloads are encrypted between sender and recipient; the super node sees only opaque ciphertext.
- **Message opacity** — the messaging service cannot read any message content.
- **DHT-based user discovery** — users and federation nodes are looked up via the Boson DHT; no central directory or DNS record is required.
- **Federation like email** — services interoperate across Boson super nodes; a user on one super node can message a user on any other super node.
- **Channel support** — group conversations with permission levels and role-based moderation.
- **Multi-device sign-in** — a single user identity (Ed25519 key pair) can be active on multiple devices simultaneously; each device has its own device key.

---

## How It Works

```
  Alice (client)                          Bob (client)
       │                                       │
       │                                       │
       ▼                                       ▼
 ┌──────────────────────┐            ┌──────────────────────┐
 │  Messaging Service A │ ◄────────► │  Messaging Service B │
 │  (Photon service)    │  federation│  (Photon service)    │
 └──────────────────────┘            └──────────────────────┘
       ▲                                       ▲
       │  DHT FIND_PEER lookup                 │
       └──────────── Boson DHT ────────────────┘
```

1. **User identity** — each user has an Ed25519 key pair (`userKey`). The user ID is the public key. All devices signed in with the same `userKey` share the same user identity.
2. **Device identity** — each device has its own Ed25519 key pair (`deviceKey`). The device ID is the device public key. Sessions can be revoked per device.
3. **Service discovery** — the client performs a `FIND_PEER` lookup on the Boson DHT using the configured `servicePeerId` to discover the service endpoint. The endpoint can also be pinned directly in the configuration.
4. **Transport** — the client connects to the messaging service over MQTTS (MQTT over TLS, port scheme `mqtts://`).
5. **Message delivery** — outgoing messages are encrypted client-side; the service routes and stores them as opaque blobs and delivers them to the recipient's devices.
6. **Federation** — when sender and recipient are on different super nodes, the originating super node performs a DHT `FIND_PEER` lookup to locate the destination node and forwards the message there.

---

## Prerequisites

| Requirement | Version |
|---|---|
| Java JDK | 17 or later |
| Apache Maven | 3.8 or later |
| Boson Core (`boson-api`, `boson-core-dht`) | same version or compatible |

---

## Build

```bash
git clone https://github.com/bosonnetwork/Boson.Messaging.Client.git
cd Boson.Messaging.Client
./mvnw clean package
```

The compiled JAR is placed in `target/lib/boson-messaging-client-<version>.jar`.

To skip tests:

```bash
./mvnw clean package -DskipTests
```

---

## Adding as a Dependency

Add the following to your Maven `pom.xml`:

```xml
<dependency>
    <groupId>io.bosonnetwork</groupId>
    <artifactId>boson-messaging-client</artifactId>
    <version>${boson.version}</version>
</dependency>
```

The library requires a running Boson `Node` (from `boson-core-dht`) to be provided by the caller. A Vert.x instance is optional — the library creates an internal one if none is supplied.

### Native transport

This library carries no platform-specific native libraries, so it runs on Java NIO wherever it is deployed. To use Netty's native transport (epoll on Linux, kqueue on macOS), add the native jars for the platform the application runs on, and create the Vert.x instance the client runs on with `new VertxOptions().setPreferNativeTransport(true)`. For Linux x86_64:

```xml
<dependency>
    <groupId>io.netty</groupId>
    <artifactId>netty-transport-native-epoll</artifactId>
    <classifier>linux-x86_64</classifier>
    <scope>runtime</scope>
</dependency>
<dependency>
    <groupId>io.netty</groupId>
    <artifactId>netty-transport-native-unix-common</artifactId>
    <classifier>linux-x86_64</classifier>
    <scope>runtime</scope>
</dependency>
```

The native jars must match the Netty version on the class path. See [Native Transport](https://docs.bosonnetwork.io/build-apps/java-sdk/native-transport) for every platform, Maven profiles and Gradle.

---

## Configuration

The client is configured via a YAML file or programmatically through `Configuration.Builder`.

### YAML configuration file

```yaml
# Messaging service peer.
# peerId is required. The client resolves the endpoint via DHT if omitted.
service:
  peerId: 7BXQciQfhFbJFjjxfJVU3gvNQG8se5ayM3xdZDix2bdK
  # endpoint: mqtts://messaging.example.com:8883  # optional: skip DHT lookup

# Client identity.
# userPrivateKey is the Ed25519 private key shared across all your devices.
# devicePrivateKey is unique to this device.
client:
  userPrivateKey: <Base58 or 0x-prefixed hex Ed25519 private key>
  devicePrivateKey: <Base58 or 0x-prefixed hex Ed25519 private key>

# Local data directory for cached state.
dataDir: "~/.local/share/boson/client/photon-messaging"

# Database for persistent message and contact storage.
# SQLite (embedded, default) or PostgreSQL.
database:
  uri: jdbc:sqlite:photon-messaging-client.db
  # uri: postgresql://localhost:5432/photonmessaging
  # poolSize: 10        # optional: connection pool size (PostgreSQL)
  # schema: myschema    # optional: PostgreSQL schema name ([a-z][a-z0-9_]{0,31})
```

### Configuration fields

| Section    | Field              | Required | Description                                                                                        |
|------------|--------------------|----------|----------------------------------------------------------------------------------------------------|
| `service`  | `peerId`           | Yes      | DHT peer ID of the messaging peer.                                                                 |
| `service`  | `endpoint`         | No       | Direct MQTTS endpoint URI. Skips DHT lookup when set. Must use `mqtt://` or `mqtts://` scheme.     |
| `client`   | `userPrivateKey`   | Yes      | Ed25519 private key (Base58 or `0x`-hex) shared by all devices of this user. Derives the user ID.  |
| `client`   | `devicePrivateKey` | Yes      | Ed25519 private key (Base58 or `0x`-hex) unique to this device. Derives the device ID.             |
| —          | `dataDir`          | No       | Path to the local data directory. Defaults to `~/.local/share/boson/client/photon-messaging`.      |
| `database` | `uri`              | Yes      | JDBC URI. Use `jdbc:sqlite:<file>` for SQLite or `postgresql://<host>:<port>/<db>` for PostgreSQL. |
| `database` | `poolSize`         | No       | Connection pool size. Relevant for PostgreSQL.                                                     |
| `database` | `schema`           | No       | PostgreSQL schema name. Must match `[a-z][a-z0-9_]{0,31}`.                                         |

### Programmatic configuration

```java
Configuration config = Configuration.builder()
    .servicePeerId(Id.of("7BXQciQfhFbJFjjxfJVU3gvNQG8se5ayM3xdZDix2bdK"))
    .serviceEndpoint("mqtts://messaging.example.com:8883")  // optional
    .userKey("<Base58-user-private-key>")
    .deviceKey("<Base58-device-private-key>")
    .dataDir("/var/lib/myapp/messaging")
    .databaseUri("jdbc:sqlite:photon-messaging-client.db")
    .build();
```

---

## Usage

```java
// Obtain a running Boson node (application-provided).
Node node = ...;

// Load configuration from a YAML map or build programmatically.
Configuration config = Configuration.fromMap(yamlMap);

// Create the client (Vertx is optional; omit to use an internal instance).
MessagingClient client = MessagingClient.create(node, config);

// Register listeners before starting.
client.addConnectionListener(new ConnectionListener() {
    @Override
    public void onReady() {
        System.out.println("Connected and ready. User: " + client.getUserId());
    }

    @Override
    public void onDisconnected() {
        System.out.println("Disconnected from messaging service.");
    }
});

client.addMessageListener(message -> {
    System.out.println("Message from " + message.getSender() + ": " + message.getContent());
});

// Start the client.
client.start().get();

// Send a message.
Id recipientId = Id.of("...");
client.message(recipientId)
    .content("Hello!")
    .send()
    .get();

// Retrieve recent conversations.
List<Conversation> conversations = client.getConversations().get();

// --- Channel example ---

// Create a channel (owner-invite only, announced to the DHT).
Channel channel = client.createChannel(Channel.Permission.OWNER_INVITE, "My Channel", "Welcome!", true).get();

// Create an invite ticket and share it out-of-band.
InviteTicket ticket = client.createInviteTicket(channel.getId()).get();

// Another user joins with the ticket.
Channel joined = client.joinChannel(ticket).get();

// Promote a member to moderator.
client.setChannelMembersRole(channel.getId(), List.of(memberId), Channel.Role.MODERATOR).get();

// --- Multi-device management ---
List<SessionInfo> sessions = client.getSessions().get();
// Revoke a specific device.
client.revokeSession(otherDeviceId).get();

// Stop when done.
client.stop().get();
```

---

## Contributing

We welcome contributions from the open-source community. To get started:

1. Fork this repository and create a feature branch.
2. Make your changes and add tests where applicable.
3. Ensure `./mvnw clean verify` passes.
4. Open a pull request with a clear description of the change.

Please read our [Code of Conduct](CODE_OF_CONDUCT.md) before contributing.

---

## License

This project is licensed under the [MIT License](LICENSE).