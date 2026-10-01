package com.laptopkit.tunnel.server;

import java.util.List;

import com.laptopkit.tunnel.common.StreamInfo;

/**
 * What one tunnel session looks like at a moment in time. Byte directions are named from the
 * server's point of view, and {@code lastPongMillis} is the age of the newest keepalive
 * reply &mdash; the number that says whether a session is actually alive or merely still
 * connected.
 *
 * @param streams the live streams of this session
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
        List<StreamInfo> streams) {
}
