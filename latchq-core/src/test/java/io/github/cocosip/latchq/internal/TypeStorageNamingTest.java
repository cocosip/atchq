package io.github.cocosip.latchq.internal;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cocosip.latchq.config.LatchQueueConfiguration;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class TypeStorageNamingTest {

    @Test
    void sanitizesInvalidFilenameCharacters() {
        assertThat(TypeStorageNaming.sanitize("a<b>c:d\"e/f\\g|h?i*j"))
                .isEqualTo("a_b_c_d_e_f_g_h_i_j");
        assertThat(TypeStorageNaming.sanitize("line\nbreak")).isEqualTo("line_break");
    }

    @Test
    void mapsEmptySegmentsToPlaceholder() {
        assertThat(TypeStorageNaming.sanitize("")).isEqualTo("_");
    }

    @Test
    void buildsQueueDirectoryFromTypeAndFileName() {
        LatchQueueConfiguration configuration = new LatchQueueConfiguration();
        configuration.setFileName("My/Queue");
        Path dir = TypeStorageNaming.queueDirectory("root", java.util.Map.class, configuration);
        // java.util.Map has no invalid characters; the fileName separator is sanitized
        assertThat(dir.toString())
                .isEqualTo(Path.of("root", "java.util.Map", "my_queue").toString());
    }
}
