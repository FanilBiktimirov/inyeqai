package com.inyeqai.tunnel.common;

/**
 * One live stream, for logs and the status view.
 *
 * @param id        the wire stream id, exactly as both ends see it
 * @param dst       where it goes, as {@code host:port}
 * @param ageMillis how long it has been open
 * @param sent      bytes this side has put into the tunnel for it
 * @param received  bytes this side has taken out of the tunnel and written to its socket
 */
public record StreamInfo(int id, String dst, long ageMillis, long sent, long received) {

    /**
     * The id as a human should read it. Stream ids carry the originator in their top bit, so
     * every id the server allocates is a negative {@code int}; printing it unsigned keeps the
     * same token on both ends of the tunnel without the misleading minus sign.
     */
    public String label() {
        return Integer.toUnsignedString(id);
    }
}
