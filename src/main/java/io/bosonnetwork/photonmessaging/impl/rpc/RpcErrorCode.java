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

package io.bosonnetwork.photonmessaging.impl.rpc;

public final class RpcErrorCode {
	// Common error codes
	public static final int RPC_INTERNAL_ERROR = -1;
	public static final int MALFORMED_REQUEST = -2;
	public static final int MALFORMED_RESPONSE = -3;
	public static final int INVALID_METHOD = -4;
	public static final int UNIMPLEMENTED_METHOD = -5;
	public static final int INVALID_PARAMS = -6;
	public static final int INVALID_RESULT = -7;
	public static final int FORBIDDEN = -8;
	public static final int TIMEOUT = -9;

	// Session error codes
	public static final int SESSION_NOT_EXISTS = -101;
	public static final int REVOKE_CURRENT_SESSION = -102;

	// Contact synchronization error codes
	public static final int CONTACT_STORE_ERROR = -201;
	public static final int REVISION_OUT_DATE = -202;
	public static final int MALFORMED_MUTATION_DATA = -203;
	public static final int CONTACT_NOT_EXISTS = -204;
	public static final int CONTACT_ALREADY_EXISTS = -205;

	// Channel error codes
	public static final int CHANNEL_NOT_EXISTS = -301;
	public static final int CHANNEL_ALREADY_EXISTS = -302;
	public static final int ALREADY_JOINED_CHANNEL = -303;
	public static final int INVALID_INVITE_TICKET = -304;
	public static final int FORBIDDEN_NON_CHANNEL_MEMBER = -305;
	public static final int FORBIDDEN_BANNED_CHANNEL_MEMBER = -306;
	public static final int CHANNEL_LIMIT_EXCEEDED = -307;
	public static final int CHANNEL_MEMBER_LIMIT_EXCEEDED = -308;
}