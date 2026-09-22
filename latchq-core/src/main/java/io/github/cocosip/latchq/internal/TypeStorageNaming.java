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

    /** Reserved Windows device names; a file named like one cannot be created on Windows. */
    private static final java.util.Set<String> WINDOWS_RESERVED_NAMES =
            java.util.Set.of(
                    "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6",
                    "COM7", "COM8", "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7",
                    "LPT8", "LPT9");

    /**
     * Replaces characters that are invalid in file/directory names (Windows set plus separators),
     * defuses reserved Windows device names and trailing dots/spaces (which Windows silently strips
     * or rejects), so distinct segments cannot collapse onto the same or an unusable directory
     * name.
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
        String result = sb.length() == 0 ? "_" : sb.toString();
        int dot = result.indexOf('.');
        String head = dot >= 0 ? result.substring(0, dot) : result;
        if (WINDOWS_RESERVED_NAMES.contains(head.toUpperCase(Locale.ROOT))) {
            result = "_" + result;
        }
        int end = result.length();
        while (end > 0 && (result.charAt(end - 1) == '.' || result.charAt(end - 1) == ' ')) {
            end--;
        }
        if (end < result.length()) {
            result = result.substring(0, end) + "_";
        }
        return result;
    }

    /** Directory of the queue: {@code root/<type>/<fileName>} with both segments sanitized. */
    public static Path queueDirectory(
            String rootPath, Class<?> type, LatchQueueConfiguration configuration) {
        Path root = Path.of(rootPath, sanitize(type.getName()));
        return root.resolve(sanitize(configuration.getFileName()).toLowerCase(Locale.ROOT));
    }

    private TypeStorageNaming() {}
}
