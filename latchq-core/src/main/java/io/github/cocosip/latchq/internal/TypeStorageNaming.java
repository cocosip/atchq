package io.github.cocosip.latchq.internal;

import io.github.cocosip.latchq.config.LatchQueueConfiguration;
import java.nio.file.Path;
import java.util.Locale;

/**
 * Derives on-disk locations for a queue, matching the FASTER reference layout: {@code
 * rootPath/<sanitized type full name>/<sanitized fileName>/}. The name only selects the
 * configuration and is not part of the path; the checkpoint file lives inside the queue directory
 * so queue and progress move together.
 */
public final class TypeStorageNaming {

    private static final String ILLEGAL_FILENAME_CHARS = "<>:\"/\\|?*";

    /**
     * Replaces characters that are invalid in file/directory names (Windows set plus separators).
     */
    public static String sanitize(String segment) {
        StringBuilder sb = new StringBuilder(segment.length());
        for (int i = 0; i < segment.length(); i++) {
            char c = segment.charAt(i);
            if (ILLEGAL_FILENAME_CHARS.indexOf(c) >= 0 || c < 32) {
                sb.append('_');
            } else {
                sb.append(c);
            }
        }
        return sb.length() == 0 ? "_" : sb.toString();
    }

    /** Directory of the queue: {@code root/<type>/<fileName>} with both segments sanitized. */
    public static Path queueDirectory(
            String rootPath, Class<?> type, LatchQueueConfiguration configuration) {
        Path root = Path.of(rootPath, sanitize(type.getName()));
        return root.resolve(sanitize(configuration.getFileName()).toLowerCase(Locale.ROOT));
    }

    private TypeStorageNaming() {}
}
