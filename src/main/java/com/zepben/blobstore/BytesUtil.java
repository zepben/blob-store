/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.zepben.blobstore;

import com.zepben.annotations.EverythingIsNonnullByDefault;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Various helper functions when working with serialisation.
 */
@EverythingIsNonnullByDefault
@SuppressWarnings("WeakerAccess")
public final class BytesUtil {

    private static byte[] toUtf8Bytes(String str) {
        return str.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Puts the given string into a byte buffer at its current position, UTF8 encoded.
     * The buffer's position will have incremented by the bytes required for the string.
     *
     * @param buffer the buffer to place the string bytes in
     * @param str    the string to place into the buffer
     */
    public static void putStringUtf8(ByteBuffer buffer, String str) {
        byte[] bytes = toUtf8Bytes(str);
        buffer.putInt(bytes.length);
        buffer.put(bytes);
    }

    /**
     * Reads a UTF8 encoded string from a buffer at its current position.
     * The buffer's position will have been incremented by the bytes required for the string.
     *
     * @param buffer The buffer to read the string bytes from
     * @return The decoded string
     */
    public static String getStringUtf8(ByteBuffer buffer) {
        int length = buffer.getInt();

        if (buffer.hasArray()) {
            buffer.position(buffer.position() + length);
            return new String(buffer.array(), buffer.position() - length, length, UTF_8);
        } else {
            byte[] bytes = new byte[length];
            buffer.get(bytes);
            return new String(bytes, UTF_8);
        }
    }

    /**
     * Zig-zag encodes an int: https://gist.github.com/mfuerstenau/ba870a29e16536fdbaba
     *
     * @param value The int to encode
     * @return the encoded int
     */
    public static int encodeZigZag(int value) {
        return (value << 1) ^ (value >> 31);
    }

    /**
     * Zig-zag decodes an int: https://gist.github.com/mfuerstenau/ba870a29e16536fdbaba
     *
     * @param value The int to decode
     * @return the decoded int
     */
    public static int decodeZigZag(int value) {
        return ((value >>> 1) ^ (-(value & 1)));
    }

    /**
     * Zig-zag encodes a long: https://gist.github.com/mfuerstenau/ba870a29e16536fdbaba
     *
     * @param value The long to encode
     * @return the encoded long
     */
    public static long encodeZigZag(long value) {
        return (value << 1) ^ (value >> 63);
    }

    /**
     * Zig-zag decodes a long: https://gist.github.com/mfuerstenau/ba870a29e16536fdbaba
     *
     * @param value The long to decode
     * @return the decoded long
     */
    public static long decodeZigZag(long value) {
        return ((value >>> 1) ^ (-(value & 1)));
    }

    /**
     * Encodes an int into "7 bit encoding" and places the bytes into a buffer at its current position.
     * This first encodes the value using a zig-zag encoding before doing the 7 bit encoding.
     * To decode use the corresponding {@link BytesUtil#decode7BitInt(ByteBuffer)} method.
     * <p>The buffer's position will have incremented by the amount of bytes required to store the encoded value.
     * See https://en.wikipedia.org/wiki/Variable-length_quantity}
     *
     * @param buffer the buffer to put the encoded bytes in
     * @param v      the value to encode.
     */
    @SuppressWarnings("Duplicates")
    public static void encode7BitInt(ByteBuffer buffer, int v) {
        v = encodeZigZag(v);

        do {
            byte lower7bits = (byte) (v & 0x7f);
            v >>>= 7;
            if (v != 0)
                lower7bits |= 128;

            buffer.put(lower7bits);
        } while (v > 0);
    }

    /**
     * Decodes an int from a "7 bit encoding" from the buffers current position.
     * This method requires the encoded value to have first been zig-zag encoded.
     * The buffer's position will have incremented by the amount of bytes required to read the encoded value.
     * See https://en.wikipedia.org/wiki/Variable-length_quantity}
     *
     * @param buffer the buffer to read the encoded bytes
     * @return The decoded value from the buffer
     */
    @SuppressWarnings("Duplicates")
    public static int decode7BitInt(ByteBuffer buffer) {
        boolean more = true;
        int shift = 0;
        int value = 0;

        while (more) {
            byte lower7bits = buffer.get();
            more = (lower7bits & 0x80) > 0;
            value |= (lower7bits & 0x7fL) << shift;
            shift += 7;
        }
        return decodeZigZag(value);
    }

    /**
     * Encodes a long into "7 bit encoding" and places the bytes into a buffer at its current position.
     * This first encodes the value using a zig-zag encoding before doing the 7 bit encoding.
     * To decode use the corresponding {@link BytesUtil#decode7BitInt(ByteBuffer)} method.
     * <p>The buffer's position will have incremented by the amount of bytes required to store the encoded value.
     * See https://en.wikipedia.org/wiki/Variable-length_quantity}
     *
     * @param buffer the buffer to put the encoded bytes in
     * @param v      the value to encode.
     */
    @SuppressWarnings("Duplicates")
    public static void encode7BitLong(ByteBuffer buffer, long v) {
        v = encodeZigZag(v);

        do {
            byte lower7bits = (byte) (v & 0x7f);
            v >>>= 7;
            if (v != 0)
                lower7bits |= 128;

            buffer.put(lower7bits);
        } while (v > 0);
    }

    /**
     * Decodes a long from a "7 bit encoding" from the buffers current position.
     * This method requires the encoded value to have first been zig-zag encoded.
     * The buffer's position will have incremented by the amount of bytes required to read the encoded value.
     * See https://en.wikipedia.org/wiki/Variable-length_quantity}
     *
     * @param buffer the buffer to read the encoded bytes
     * @return The decoded value from the buffer
     */
    @SuppressWarnings("Duplicates")
    public static long decode7BitLong(ByteBuffer buffer) {
        boolean more = true;
        int shift = 0;
        long value = 0;

        while (more) {
            byte lower7bits = buffer.get();
            more = (lower7bits & 0x80) > 0;
            value |= (lower7bits & 0x7fL) << shift;
            shift += 7;
        }
        return decodeZigZag(value);
    }

    private BytesUtil() {
    }

}
