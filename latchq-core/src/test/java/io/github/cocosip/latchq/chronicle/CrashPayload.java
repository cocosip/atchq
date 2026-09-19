package io.github.cocosip.latchq.chronicle;

/** Shared payload type so a crash-child process and its parent reopen the same queue directory. */
public record CrashPayload(int id) {}
