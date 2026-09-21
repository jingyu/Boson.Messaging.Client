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

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.bosonnetwork.Id;

/**
 * Represents session information for a device, including its identifier,
 * online status, and last activity timestamp.
 * <p>
 * A plain class rather than a record: Jackson's record support calls
 * {@code Class.getRecordComponents()}, which the Android runtime does not implement, so a
 * record wire type fails to deserialize on-device. Accessors keep the record-style names so
 * call sites are unaffected.
 */
public final class SessionInfo {
	@JsonProperty(value = "id", required = true)
	private final Id deviceId;
	@JsonProperty("o")
	private final boolean online;
	@JsonProperty("lt")
	private final long lastActive;
	@JsonProperty("la")
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private final String lastAddress;

	/**
	 * Deserializes a session info from its wire representation.
	 *
	 * @param deviceId    the id of the device the session belongs to
	 * @param online      whether the device is currently online
	 * @param lastActive  the timestamp of the device's last activity, in milliseconds since epoch
	 * @param lastAddress the last known address of the device, or {@code null} if the service
	 *                    reported none
	 */
	@JsonCreator
	public SessionInfo(@JsonProperty(value = "id", required = true) Id deviceId,
			@JsonProperty("o") boolean online,
			@JsonProperty("lt") long lastActive,
			@JsonProperty("la") String lastAddress) {
		this.deviceId = deviceId;
		this.online = online;
		this.lastActive = lastActive;
		this.lastAddress = lastAddress;
	}

	/**
	 * Returns the id of the device this session belongs to.
	 *
	 * @return the device id
	 */
	public Id deviceId() {
		return deviceId;
	}

	/**
	 * Returns whether the device is currently online.
	 *
	 * @return {@code true} if the device holds a live session
	 */
	public boolean online() {
		return online;
	}

	/**
	 * Returns the timestamp of the device's last activity.
	 *
	 * @return the last activity time, in milliseconds since epoch
	 */
	public long lastActive() {
		return lastActive;
	}

	/**
	 * Returns the last known address of the device, as reported by the service.
	 *
	 * @return the device's last known address, or {@code null} if none was reported
	 */
	public String lastAddress() {
		return lastAddress;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (!(o instanceof SessionInfo that))
			return false;
		return online == that.online && lastActive == that.lastActive
				&& Objects.equals(deviceId, that.deviceId) && Objects.equals(lastAddress, that.lastAddress);
	}

	@Override
	public int hashCode() {
		return Objects.hash(deviceId, online, lastActive, lastAddress);
	}

	@Override
	public String toString() {
		return "SessionInfo[deviceId=" + deviceId + ", online=" + online
				+ ", lastActive=" + lastActive + ", lastAddress=" + lastAddress + "]";
	}
}
