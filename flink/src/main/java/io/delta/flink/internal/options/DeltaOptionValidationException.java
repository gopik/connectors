package io.delta.flink.internal.options;

import java.util.Collection;
import java.util.Collections;

/**
 * Exception throw during validation of Delta connector options.
 */
public class DeltaOptionValidationException extends RuntimeException {

    /**
     * Path to Delta table for which exception was thrown. Can be null if exception was thrown on
     * missing path to Delta table.
     */
    private final String tablePath;

    /**
     * Collection with all validation error messages that were recorded for this exception.
     */
    private final Collection<String> validationMessages;

    public DeltaOptionValidationException(String tablePath, Collection<String> validationMessages) {
        this.tablePath = String.valueOf(tablePath);
        this.validationMessages =
            (validationMessages == null) ? Collections.emptyList() : validationMessages;
    }

    @Override
    public String getMessage() {

        String validationMessages = String.join(System.lineSeparator(), this.validationMessages);

        return "Invalid Delta connector definition detected."
            + System.lineSeparator()
            + "The reported issues are:"
            + System.lineSeparator()
            + validationMessages;
    }
}
