package io.github.cocosip.latchq;

import java.util.List;

/**
 * Result of an export operation.
 *
 * @param filePaths the list of files written (JSONL, one JSON document per line)
 * @param count the number of entries exported
 * @param fromIndex the index of the first entry actually exported (after clamping)
 * @param toIndex the exclusive upper bound reached by the export
 */
public record ExportResult(List<String> filePaths, long count, long fromIndex, long toIndex) {

    public ExportResult {
        filePaths = List.copyOf(filePaths);
    }
}
