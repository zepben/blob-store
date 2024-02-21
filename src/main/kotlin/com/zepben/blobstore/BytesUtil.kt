/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
package com.zepben.blobstore

import java.nio.ByteBuffer

/**
 * Various helper functions when working with serialisation.
 */
object BytesUtil {

    /**
     * Puts the given string into a byte buffer at its current position, UTF8 encoded.
     * The buffer's position will have incremented by the bytes required for the string.
     *
     * @receiver the buffer to place the string bytes in
     * @param str the string to place into the buffer
     */
    fun ByteBuffer.putStringUtf8(str: String) {
        val bytes = str.toByteArray()
        putInt(bytes.size)
        put(bytes)
    }

    /**
     * Reads a UTF8 encoded string from a buffer at its current position.
     * The buffer's position will have been incremented by the bytes required for the string.
     *
     * @receiver The buffer to read the string bytes from
     * @return The decoded string
     */
    fun ByteBuffer.getStringUtf8(): String {
        val length = getInt()
        return if (hasArray()) {
            position(position() + length)
            String(array(), position() - length, length)
        } else {
            val bytes = ByteArray(length).also { get(it) }
            String(bytes)
        }
    }

    /**
     * Zig-zag encodes an int: https://gist.github.com/mfuerstenau/ba870a29e16536fdbaba
     *
     * @param value The int to encode
     * @return the encoded int
     */
    fun encodeZigZag(value: Int): Int = value shl 1 xor (value shr 31)

    /**
     * Zig-zag decodes an int: https://gist.github.com/mfuerstenau/ba870a29e16536fdbaba
     *
     * @param value The int to decode
     * @return the decoded int
     */
    fun decodeZigZag(value: Int): Int = value ushr 1 xor -(value and 1)

    /**
     * Zig-zag encodes a long: https://gist.github.com/mfuerstenau/ba870a29e16536fdbaba
     *
     * @param value The long to encode
     * @return the encoded long
     */
    fun encodeZigZag(value: Long): Long = value shl 1 xor (value shr 63)

    /**
     * Zig-zag decodes a long: https://gist.github.com/mfuerstenau/ba870a29e16536fdbaba
     *
     * @param value The long to decode
     * @return the decoded long
     */
    fun decodeZigZag(value: Long): Long = value ushr 1 xor -(value and 1L)

    /**
     * Encodes an int into "7 bit encoding" and places the bytes into a buffer at its current position.
     * This first encodes the value using a zigzag encoding before doing the 7 bit encoding.
     * To decode use the corresponding [BytesUtil.decode7BitInt] method.
     *
     * The buffer's position will have incremented by the amount of bytes required to store the encoded value.
     * See https://en.wikipedia.org/wiki/Variable-length_quantity}
     *
     * @receiver the buffer to put the encoded bytes in
     * @param value the value to encode.
     */
    fun ByteBuffer.encode7BitInt(value: Int) {
        var v = encodeZigZag(value)

        do {
            var lower7bits = (v and 0x7f).toByte()
            v = v ushr 7

            if (v != 0)
                lower7bits = (lower7bits.toInt() or 128).toByte()

            put(lower7bits)
        } while (v > 0)
    }

    /**
     * Decodes an int from a "7 bit encoding" from the buffers current position.
     * This method requires the encoded value to have first been zigzag encoded.
     * The buffer's position will have incremented by the amount of bytes required to read the encoded value.
     * See https://en.wikipedia.org/wiki/Variable-length_quantity}
     *
     * @param buffer the buffer to read the encoded bytes
     * @return The decoded value from the buffer
     */
    fun decode7BitInt(buffer: ByteBuffer): Int {
        var more = true
        var shift = 0
        var value = 0
        while (more) {
            val lower7bits = buffer.get()
            more = lower7bits.toInt() and 0x80 > 0

            value = (value.toLong() or (lower7bits.toLong() and 0x7fL shl shift)).toInt()
            shift += 7
        }

        return decodeZigZag(value)
    }

    /**
     * Encodes a long into "7 bit encoding" and places the bytes into a buffer at its current position.
     * This first encodes the value using a zigzag encoding before doing the 7 bit encoding.
     * To decode use the corresponding [BytesUtil.decode7BitInt] method.
     *
     * The buffer's position will have incremented by the amount of bytes required to store the encoded value.
     * See https://en.wikipedia.org/wiki/Variable-length_quantity}
     *
     * @param buffer the buffer to put the encoded bytes in
     * @param value      the value to encode.
     */
    fun encode7BitLong(buffer: ByteBuffer, value: Long) {
        var v = encodeZigZag(value)
        do {
            var lower7bits = (v and 0x7fL).toByte()
            v = v ushr 7

            if (v != 0L)
                lower7bits = (lower7bits.toInt() or 128).toByte()

            buffer.put(lower7bits)
        } while (v > 0)
    }

    /**
     * Decodes a long from a "7 bit encoding" from the buffers current position.
     * This method requires the encoded value to have first been zigzag encoded.
     * The buffer's position will have incremented by the amount of bytes required to read the encoded value.
     * See https://en.wikipedia.org/wiki/Variable-length_quantity}
     *
     * @param buffer the buffer to read the encoded bytes
     * @return The decoded value from the buffer
     */
    fun decode7BitLong(buffer: ByteBuffer): Long {
        var more = true
        var shift = 0
        var value: Long = 0
        while (more) {
            val lower7bits = buffer.get()

            more = lower7bits.toInt() and 0x80 > 0

            value = value or (lower7bits.toLong() and 0x7fL shl shift)
            shift += 7
        }

        return decodeZigZag(value)
    }

}
