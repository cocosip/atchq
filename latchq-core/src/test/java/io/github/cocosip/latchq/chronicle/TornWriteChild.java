package io.github.cocosip.latchq.chronicle;

import java.nio.charset.StandardCharsets;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.wire.DocumentContext;

/** 写入 N 条正常消息后,发起一条大消息写到一半时硬杀 JVM(halt),制造脏尾部。 */
public final class TornWriteChild {

    public static void main(String[] args) throws Exception {
        String dir = args[0];
        int normal = Integer.parseInt(args[1]);
        try (ChronicleQueue queue = ChronicleQueue.singleBuilder(dir).build()) {
            try (ExcerptAppender appender = queue.createAppender()) {
                for (int i = 0; i < normal; i++) {
                    byte[] data = ("{\"id\":" + i + ",\"pad\":\"" + "y".repeat(80) + "\"}")
                            .getBytes(StandardCharsets.UTF_8);
                    try (DocumentContext ctx = appender.writingDocument()) {
                        ctx.wire().writeBytes(out -> out.write(data));
                    }
                }
            }
            System.out.println("child: wrote " + normal + " normal messages");
            System.out.flush();
            // 开始一条大消息,不关闭 context 直接 halt → 磁盘上留下半条消息
            ExcerptAppender appender = queue.createAppender();
            DocumentContext torn = appender.writingDocument();
            torn.wire()
                    .writeBytes(out -> out.write(
                            ("TORN-" + "x".repeat(3_000_000)).getBytes(StandardCharsets.UTF_8)));
            System.out.println("child: halting mid-write");
            System.out.flush();
            Runtime.getRuntime().halt(7);
        }
    }

    private TornWriteChild() {}
}
