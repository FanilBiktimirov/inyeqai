package com.inyeqai.tl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

class ServerArgsTest {

    @Test
    void chiselStyleFlagsBecomeSpringProperties() {
        InyeqaiApplication.ServerArgs a = InyeqaiApplication.translate(new String[] {
                "--port", "8080", "--host", "0.0.0.0", "--auth", "TL:pass",
                "--path", "/t", "--keepalive", "25s", "--pong-timeout", "75s"});
        assertEquals(List.of(), a.unknown());
        assertEquals(List.of(
                "--server.port=8080",
                "--server.address=0.0.0.0",
                "--tl.auth=TL:pass",
                "--tl.path=/t",
                "--tl.keepalive=25s",
                "--tl.pong-timeout=75s"), a.springArgs());
    }

    @Test
    void allowIsRepeatableAndIndexed() {
        InyeqaiApplication.ServerArgs a = InyeqaiApplication.translate(new String[] {
                "--allow", "^127\\.0\\.0\\.1:3129$", "--allow", "^R:0\\.0\\.0\\.0:3130$"});
        assertEquals(List.of(
                "--tl.allow[0]=^127\\.0\\.0\\.1:3129$",
                "--tl.allow[1]=^R:0\\.0\\.0\\.0:3130$"), a.springArgs());
        assertEquals(List.of(), a.unknown());
    }

    @Test
    void springStylePropertiesPassStraightThrough() {
        // Without this, --logging.level.* was silently dropped: the server looked like it had
        // accepted the setting and then logged nothing extra.
        InyeqaiApplication.ServerArgs a = InyeqaiApplication.translate(new String[] {
                "--port", "9000",
                "--logging.level.com.inyeqai.tl=DEBUG",
                "--server.ssl.enabled=true"});
        assertEquals(List.of(), a.unknown());
        assertTrue(a.springArgs().contains("--logging.level.com.inyeqai.tl=DEBUG"));
        assertTrue(a.springArgs().contains("--server.ssl.enabled=true"));
    }

    @Test
    void verboseIsAShortcutForTheLogLevel() {
        assertEquals(List.of(InyeqaiApplication.VERBOSE),
                InyeqaiApplication.translate(new String[] {"-v"}).springArgs());
        assertEquals(List.of(InyeqaiApplication.VERBOSE),
                InyeqaiApplication.translate(new String[] {"--verbose"}).springArgs());
    }

    @Test
    void aMistypedFlagIsReportedNotIgnored() {
        // The dangerous one: "--alow PATTERN" used to vanish, leaving an empty allow list,
        // which means every address is reachable. That must never look like success.
        InyeqaiApplication.ServerArgs a = InyeqaiApplication.translate(new String[] {
                "--port", "8080", "--alow", "^127\\.0\\.0\\.1:3129$"});
        assertEquals(List.of("--alow", "^127\\.0\\.0\\.1:3129$"), a.unknown());
        assertEquals(List.of("--server.port=8080"), a.springArgs(),
                "the allow pattern must not have leaked into the accepted settings");
    }

    @Test
    void aFlagMissingItsValueFailsLoudly() {
        assertThrows(IllegalArgumentException.class,
                () -> InyeqaiApplication.translate(new String[] {"--port"}));
        assertThrows(IllegalArgumentException.class,
                () -> InyeqaiApplication.translate(new String[] {"--auth"}));
    }
}
