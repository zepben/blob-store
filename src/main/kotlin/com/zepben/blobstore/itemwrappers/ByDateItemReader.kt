/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
package com.zepben.blobstore.itemwrappers

import com.zepben.blobstore.*
import java.time.LocalDate
import java.time.ZoneId

class ByDateItemReader<T>(
    private val timeZone: ZoneId,
    private val readerProvider: ByDateBlobReaderProvider
) {

    private var tagDeserialisers = emptyMap<String, ByDateTagDeserialiser<*>>()
    private var itemDeserialiser = ByDateItemDeserialiser<T> { _, _, _ -> null }

    fun setDeserialisers(
        itemDeserialiser: ByDateItemDeserialiser<T>,
        tagDeserialisers: Map<String, ByDateTagDeserialiser<*>>
    ) {
        this.itemDeserialiser = itemDeserialiser
        this.tagDeserialisers = tagDeserialisers
    }

    operator fun get(id: String, date: LocalDate, onError: ByDateItemError): T? =
        Captor<T>().let { captor ->
            forEach(setOf(id), date, { _, _, item -> captor.capture(item) }, onError)
            captor.captured
        }

    fun forEach(ids: Collection<String>, date: LocalDate, onRead: ByDateItemHandler<T>, onError: ByDateItemError) {
        try {
            getBlobReader(date)?.forEach(ids, tagDeserialisers.keys, tagsHandler(date, onRead, onError))
        } catch (e: BlobStoreException) {
            onError.handle("", date, "Error reading data store", e)
        }
    }

    fun forAll(date: LocalDate, onRead: ByDateItemHandler<T>, onError: ByDateItemError) {
        try {
            getBlobReader(date)?.forAll(tagDeserialisers.keys, tagsHandler(date, onRead, onError))
        } catch (e: BlobStoreException) {
            onError.handle("", date, "Error reading data store", e)
        }
    }

    fun forAll(date: LocalDate, whereBlobs: List<WhereBlob>, onRead: ByDateItemHandler<T>, onError: ByDateItemError) {
        try {
            getBlobReader(date)?.forAll(tagDeserialisers.keys, whereBlobs, tagsHandler(date, onRead, onError))
        } catch (e: BlobStoreException) {
            onError.handle("", date, "Error reading data store", e)
        }
    }

    operator fun <R> get(id: String, date: LocalDate, tag: String, onError: ByDateItemError): R? =
        try {
            getBlobReader(date)?.get(id, tag)?.let { bytes ->
                deserialiseTag<R>(id, date, tag, bytes, onError)
            }
        } catch (e: BlobStoreException) {
            onError.handle("", date, "Error reading data store", e)
            null
        }

    fun <R> forEach(ids: Collection<String>, date: LocalDate, tag: String, onRead: ByDateItemHandler<R>, onError: ByDateItemError) {
        try {
            getBlobReader(date)?.forEach(ids, tag, blobHandler(date, onRead, onError))
        } catch (e: BlobStoreException) {
            onError.handle("", date, "Error reading data store", e)
        }
    }

    fun <R> forAll(date: LocalDate, tag: String, onRead: ByDateItemHandler<R>, onError: ByDateItemError) {
        try {
            getBlobReader(date)?.forAll(tag, blobHandler(date, onRead, onError))
        } catch (e: BlobStoreException) {
            onError.handle("", date, "Error reading data store", e)
        }
    }

    private fun tagsHandler(date: LocalDate, onRead: ByDateItemHandler<T>, onError: ByDateItemError): TagsHandler =
        { id, blobs ->
            try {
                itemDeserialiser.deserialise(id, date, blobs)?.also {
                    onRead.handle(id, date, it)
                }
            } catch (e: DeserialiseException) {
                onError.handle(id, date, e.message, e)
            }
        }

    private fun <R> blobHandler(date: LocalDate, onRead: ByDateItemHandler<R>, onError: ByDateItemError): BlobHandler =
        { id, tag, blob ->
            deserialiseTag<R>(id, date, tag, blob, onError)?.also {
                onRead.handle(id, date, it)
            }
        }

    private fun <R> deserialiseTag(id: String, date: LocalDate, tag: String, blob: ByteArray?, onError: ByDateItemError): R? {
        return try {
            val deserialiser = tagDeserialisers[tag] ?: return null
            @Suppress("UNCHECKED_CAST")
            deserialiser.deserialise(id, date, tag, blob) as R
        } catch (e: DeserialiseException) {
            onError.handle(id, date, e.message, e)
            null
        }
    }

    private fun getBlobReader(date: LocalDate): BlobReader? =
        readerProvider.getReader(date, timeZone)

}
