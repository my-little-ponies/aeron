package io.aeron.archive.client.util;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Util {

    private static final Logger LOG = LoggerFactory.getLogger(Util.class);

    private Util() {}

    public static <T extends Number & Comparable<T>> T minCheckedValue(
            final T value, final T minValue, final String paramName) {

        requireNonNull(value, paramName);
        requireNonNull(minValue, () -> "Minimum value for " + paramName);

        if (value.compareTo(minValue) < 0) {
            throw new IllegalArgumentException(
                    paramName + " = " + value + " is lesser than allowed minimum = " + minValue);
        }

        return value;
    }

    public static <T extends Number & Comparable<T>> T maxCheckedValue(
            final T value, final T maxValue, final String paramName) {

        requireNonNull(value, paramName);
        requireNonNull(maxValue, () -> "Maximum value for " + paramName);

        if (value.compareTo(maxValue) > 0) {
            throw new IllegalArgumentException(
                    paramName + " = " + value + " is greater than allowed maximum = " + maxValue);
        }

        return value;
    }

    public static boolean deleteDirectoryRecursively(
            final Path path, final int retryAttempts, final long retryIntervalInMillis) {
        for (int i = 0; i <= retryAttempts; i++) {
            try {
                Files.walkFileTree(
                        path,
                        new SimpleFileVisitor<Path>() {
                            @Override
                            public FileVisitResult visitFile(
                                    final Path file, final BasicFileAttributes attrs)
                                    throws IOException {
                                Files.delete(file);
                                return FileVisitResult.CONTINUE;
                            }

                            @Override
                            public FileVisitResult postVisitDirectory(
                                    final Path dir, final IOException ex) throws IOException {
                                Files.delete(dir);
                                return FileVisitResult.CONTINUE;
                            }
                        });

                return true;
            } catch (final IOException e) {
                LOG.warn("Unable to delete directory {}", path);

                try {
                    Thread.sleep(retryIntervalInMillis);
                } catch (final InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    LOG.warn("Interrupted on directory deletion: {}", path);
                    return false;
                }
            }
        }

        return false;
    }
}
