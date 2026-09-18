package io.github.cocosip.latchq.internal;

/**
 * Raw message handed from the scan thread to consumers through the bounded queue.
 *
 * @param data the raw serialized payload bytes
 * @param index the index of this message
 * @param nextIndex the index of the message that follows this one; always a real message index
 *     (stamped by the read-ahead scan), never derived arithmetically
 */
public record BufferedLogEntry(byte[] data, long index, long nextIndex) {}
