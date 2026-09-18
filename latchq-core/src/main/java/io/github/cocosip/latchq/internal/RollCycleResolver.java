package io.github.cocosip.latchq.internal;

import io.github.cocosip.latchq.exception.LatchQInvalidConfigurationException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.RollCycles;

/**
 * Resolves roll cycles by name from the {@link RollCycles} constants. Reflection is used on
 * purpose: the set and spelling of the constants changed between Chronicle releases (5.27ea5 has
 * neither {@code XLARGE_DAILY} nor an accessible alias we could rely on at compile time), and
 * configuration must survive dependency upgrades.
 */
public final class RollCycleResolver {

    /** Resolves a roll cycle by (case-insensitive) constant name, failing fast when unknown. */
    public static RollCycle resolve(String name) {
        for (Field field : RollCycles.class.getFields()) {
            if (isRollCycleField(field) && field.getName().equalsIgnoreCase(name)) {
                try {
                    return (RollCycle) field.get(null);
                } catch (IllegalAccessException e) {
                    throw new LatchQInvalidConfigurationException(
                            "roll cycle '" + field.getName() + "' is not accessible", e);
                }
            }
        }
        throw new LatchQInvalidConfigurationException(
                "unknown rollCycle '" + name + "', available: " + availableNames());
    }

    /** Lists the roll cycle constant names available in the current Chronicle version. */
    public static List<String> availableNames() {
        List<String> names = new ArrayList<>();
        for (Field field : RollCycles.class.getFields()) {
            if (isRollCycleField(field)) {
                names.add(field.getName());
            }
        }
        return names;
    }

    private static boolean isRollCycleField(Field field) {
        int modifiers = field.getModifiers();
        return Modifier.isStatic(modifiers)
                && Modifier.isPublic(modifiers)
                && RollCycle.class.isAssignableFrom(field.getType());
    }

    private RollCycleResolver() {}
}
