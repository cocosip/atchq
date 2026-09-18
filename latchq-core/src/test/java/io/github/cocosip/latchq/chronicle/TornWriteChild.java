package io.github.cocosip.latchq.chronicle;

import java.nio.charset.StandardCharsets;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.wire.DocumentContext;

/**
 * Writes N normal messages, then starts one huge message and hard-kills the JVM (halt) mid-write to
 * create a dirty tail.
 */
public final class TornWriteChild {

    public static void main(String[] args) throws Exception {
        String dir = args[0];
        int normal = Integer.parseInt(args[1]);
        try (ChronicleQueue queue = ChronicleQueue.singleBuilder(dir).build()) {
            try (ExcerptAppender appender = queue.createAppender()) {
                for (int i = 0; i < normal; i++) {
                    byte[] data =
                            ("{\"id\":" + i + ",\"pad\":\"" + "y".repeat(80) + "\"}")
                                    .getBytes(StandardCharsets.UTF_8);
                    try (DocumentContext ctx = appender.writingDocument()) {
                        ctx.wire().writeBytes(out -> out.write(data));
                    }
                }
            }
            System.out.println("child: wrote " + normal + " normal messages");
            System.out.flush();
            // Start a huge message and halt without closing the context, leaving a torn message on
            // disk.
            ExcerptAppender appender = queue.createAppender();
            DocumentContext torn = appender.writingDocument();
            torn.wire()
                    .writeBytes(
                            out ->
                                    out.write(
                                            ("TORN-" + "x".repeat(3_000_000))
                                                    .getBytes(StandardCharsets.UTF_8)));
            System.out.println("child: halting mid-write");
            System.out.flush();
            Runtime.getRuntime().halt(7);
        }
    }

    private TornWriteChild() {}
}
