package com.inyeqai.tunnel.server;

import java.util.List;

import com.inyeqai.tunnel.common.StreamInfo;

/**
 * What one tunnel session looks like at a moment in time. Byte directions are named from the
 * server's point of view, and {@code lastPongMillis} is the age of the newest keepalive
 * reply &mdash; the number that says whether a session is actually alive or merely still
 * connected.
 *
 * @param streams       the live streams of this session
 * @param resumes       how many times this session's carrier had to be resumed; always zero for
 *                      a WebSocket, which has no such notion
 * @param holdingBytes  bytes written but not yet confirmed, kept in case they must be sent again
 */
public record SessionSnapshot(
        String id,
        String remote,
        long upMillis,
        long lastPongMillis,
        int openStreams,
        long streamsCarried,
        long bytesToClient,
        long bytesFromClient,
        List<Integer> reversePorts,
        List<StreamInfo> streams,
        long resumes,
        int holdingBytes) {
}
