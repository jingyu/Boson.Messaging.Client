package io.bosonnetwork.photonmessaging;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Defines the possible delivery states for a message delivery receipt.
 * <p>
 * The values describe successful delivery and the various failure conditions
 * that may occur while resolving a recipient, federating with a remote peer,
 * or confirming delivery within the allowed retry period.
 */
public enum DeliveryStatus {
	/**
	 * The message was not delivered.
	 */
	UNDELIVERED(0),
	/**
	 * The message was delivered successfully.
	 */
	DELIVERED(1),
	/**
	 * The recipient could not be resolved.
	 */
	RECIPIENT_UNRESOLVED(2),
	/**
	 * The remote peer explicitly reported that recipient as unknown.
	 */
	RECIPIENT_UNKNOWN(3),
	/**
	 * The resolved remote peer does not federate with home peer.
	 */
	NOT_FEDERATING(4),
	/**
	 * Home peer denies further federation attempts.
	 */
	FEDERATION_REFUSED(5),
	/**
	 * The remote peer could not be resolved.
	 */
	PEER_UNRESOLVED(6),
	/**
	 * The remote peer is not verified.
	 */
	PEER_UNVERIFIED(7),
	/**
	 * Delivery could not be confirmed before the retry lifetime ended.
	 */
	EXPIRED(8),
	/**
	 * Delivery failed due to an internal error.
	 */
	INTERNAL_ERROR(9);

	private final int value;

	DeliveryStatus(int value) {
		this.value = value;
	}

	/**
	 * Returns the numeric value of this delivery status.
	 *
	 * @return the integer value associated with this delivery status
	 */
	@JsonValue
	public int value() {
		return value;
	}

	/**
	 * Returns the delivery status corresponding to the given numeric value.
	 *
	 * @param value the numeric value of the delivery status
	 * @return the delivery status with the specified numeric value
	 * @throws IllegalArgumentException if the specified value is not a valid delivery status
	 */
	@JsonCreator
	public static DeliveryStatus valueOf(int value) {
		return switch (value) {
			case 0 -> UNDELIVERED;
			case 1 -> DELIVERED;
			case 2 -> RECIPIENT_UNRESOLVED;
			case 3 -> RECIPIENT_UNKNOWN;
			case 4 -> NOT_FEDERATING;
			case 5 -> FEDERATION_REFUSED;
			case 6 -> PEER_UNRESOLVED;
			case 7 -> PEER_UNVERIFIED;
			case 8 -> EXPIRED;
			case 9 -> INTERNAL_ERROR;
			default -> throw new IllegalArgumentException("Invalid delivery status: " + value);
		};
	}
}