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

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import org.jspecify.annotations.NullUnmarked;

import io.bosonnetwork.Id;
import io.bosonnetwork.crypto.CryptoBox;
import io.bosonnetwork.crypto.Signature;
import io.bosonnetwork.photonmessaging.Channel;
import io.bosonnetwork.photonmessaging.Contact;

@NullUnmarked
@JsonPOJOBuilder
@JsonIgnoreProperties(ignoreUnknown = true)
public class ContactBuilder {
	// Generic contact fields
	private Id id;
	private Contact.Type type;
	private byte[] sessionKey;
	private String name;
	private String remark;
	private String tags;
	private boolean muted;
	private boolean blocked;
	private long createdAt;
	private long updatedAt;

	private int revision;

	// Channel fields
	private Id ownerId;
	private Channel.Permission permission;
	private String notice;
	private boolean announce;

	protected ContactBuilder() {
	}

	@JsonProperty("id")
	public ContactBuilder withId(Id id) {
		this.id = Objects.requireNonNull(id, "id");
		return this;
	}

	@JsonProperty("t")
	public ContactBuilder withType(Contact.Type type) {
		this.type = Objects.requireNonNull(type, "type");
		return this;
	}

	@JsonProperty("sk")
	public ContactBuilder withSessionKey(byte[] sessionKey) {
		Objects.requireNonNull(sessionKey, "sessionKey");
		// session key should be encrypted
		if (sessionKey.length != Signature.PrivateKey.BYTES &&
				sessionKey.length != CryptoBox.Nonce.BYTES + CryptoBox.MAC_BYTES + Signature.PrivateKey.BYTES)
			throw new IllegalArgumentException("invalid session key (encrypted)");

		this.sessionKey =sessionKey;
		return this;
	}

	@JsonProperty("n")
	public ContactBuilder withName(String name) {
		this.name = name;
		return this;
	}

	@JsonProperty("r")
	public ContactBuilder withRemark(String remark) {
		this.remark = remark;
		return this;
	}

	@JsonProperty("ts")
	public ContactBuilder withTags(String tags) {
		this.tags = tags == null || tags.isEmpty() ? null : tags;
		return this;
	}

	@JsonProperty("m")
	public ContactBuilder withMuted(boolean muted) {
		this.muted = muted;
		return this;
	}

	@JsonProperty("b")
	public ContactBuilder withBlocked(boolean blocked) {
		this.blocked = blocked;
		return this;
	}

	@JsonProperty("c")
	public ContactBuilder withCreatedAt(long createdAt) {
		this.createdAt = createdAt;
		return this;
	}

	@JsonProperty("u")
	public ContactBuilder withUpdatedAt(long updatedAt) {
		this.updatedAt = updatedAt;
		return this;
	}

	@JsonProperty("v")
	public ContactBuilder withRevision(int revision) {
		this.revision = revision;
		return this;
	}

	@JsonProperty("o")
	public ContactBuilder withOwnerId(Id ownerId) {
		this.ownerId = Objects.requireNonNull(ownerId, "ownerId");
		return this;
	}

	@JsonProperty("p")
	public ContactBuilder withPermission(Channel.Permission permission) {
		this.permission = Objects.requireNonNull(permission, "permission");
		return this;
	}

	@JsonProperty("nt")
	public ContactBuilder withNotice(String notice) {
		this.notice = notice == null || notice.isEmpty() ? null : notice;
		return this;
	}

	@JsonProperty("a")
	public ContactBuilder withAnnounce(boolean announce) {
		this.announce = announce;
		return this;
	}

	public Contact build() {
		if (id == null)
			throw new IllegalArgumentException("Missing id");

		if (type == null)
			throw new IllegalArgumentException("Missing type");

		if (sessionKey == null && type != Contact.Type.AUTO)
			throw new IllegalArgumentException("Missing session key");

		return switch (type) {
			case AUTO -> new AutoContact(id, name, remark, tags, muted, blocked, createdAt, updatedAt);
			case FRIEND -> new Friend(id, sessionKey, name, remark, tags, muted, blocked, createdAt, updatedAt, revision);
			case CHANNEL -> new PhotonChannel(id, sessionKey, ownerId, permission, name, notice, announce, remark, tags,
					muted, blocked, createdAt, updatedAt, revision);
		};
	}
}