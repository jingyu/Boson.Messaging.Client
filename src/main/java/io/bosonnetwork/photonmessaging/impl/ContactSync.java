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

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import org.jspecify.annotations.Nullable;

import io.bosonnetwork.photonmessaging.impl.dto.OpaqueContact;

/**
 * Represents a contact sync notification or response.
 */
public class ContactSync {
	// last revision of the server
	@JsonProperty(value = "v", required = true)
	private final int revision;

	@JsonProperty(value = "t", required = true)
	private final Type type;

	@JsonProperty("d")
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private final List<ContactMutation> mutations;

	@JsonProperty("s")
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private final List<OpaqueContact> contacts;

	public enum Type {
		UP_TO_DATE(0),
		DELTA(1),
		SNAPSHOT(2);

		private final int value;

		Type(int value) {
			this.value = value;
		}

		@JsonValue
		public int value() {
			return value;
		}

		@JsonCreator
		public static Type valueOf(int value) {
			return switch (value) {
				case 0 -> UP_TO_DATE;
				case 1 -> DELTA;
				case 2 -> SNAPSHOT;
				default -> throw new IllegalArgumentException("Invalid contact sync type: " + value);
			};
		}
	}

	@JsonCreator
	protected ContactSync(@JsonProperty(value = "v", required = true) int revision,
	                      @JsonProperty(value = "t", required = true) Type type,
	                      @JsonProperty("d") @Nullable List<ContactMutation> mutations,
	                      @JsonProperty("s") @Nullable List<OpaqueContact> contacts) {
		if ((mutations != null && !mutations.isEmpty()) && (contacts != null && !contacts.isEmpty()))
			throw new IllegalArgumentException("Mutations and contacts can not be both present");

		if (type == Type.UP_TO_DATE && mutations != null && contacts != null)
			throw new IllegalArgumentException("Up-to-date sync does not require mutations or contacts");

		if (type == Type.DELTA && (mutations == null || mutations.isEmpty()))
			throw new IllegalArgumentException("Delta sync requires mutations");

		// A snapshot may be empty: the service has none of the user's contacts.

		this.type = type;
		this.revision = revision;
		this.contacts = contacts == null || contacts.isEmpty() ? List.of() : contacts;
		this.mutations = mutations == null || mutations.isEmpty() ? List.of() : mutations;
	}

	public static ContactSync upToDate(int revision) {
		return new ContactSync(revision, Type.UP_TO_DATE, null, null);
	}

	public static ContactSync delta(int revision, List<ContactMutation> mutations) {
		return new ContactSync(revision, Type.DELTA, mutations, null);
	}

	public static ContactSync snapshot(int revision, List<OpaqueContact> contacts) {
		return new ContactSync(revision, Type.SNAPSHOT, null, contacts);
	}

	public Type getType() {
		return type;
	}

	public int getRevision() {
		return revision;
	}

	public List<ContactMutation> getMutations() {
		return mutations;
	}

	public List<OpaqueContact> getContacts() {
		return contacts;
	}
}