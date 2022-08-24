package io.delta.flink.internal.options;

import java.util.Map;

import io.delta.flink.source.internal.DeltaSourceOptions;

/**
 * Validator for delta source and sink connector configuration options.
 *
 * Setting of an option is allowed for known option names. For invalid options, the validation
 * throws {@link IllegalArgumentException}.
 */
public class OptionValidator {
    private final Map<String, DeltaConfigOption<?>> validOptions;
    private final DeltaConnectorConfiguration config;

    public OptionValidator(
        DeltaConnectorConfiguration config,
        Map<String, DeltaConfigOption<?>> validOptions) {
        this.config = config;
        this.validOptions = validOptions;
    }

    public void option(String optionName, String optionValue) {
        tryToSetOption(() -> {
            DeltaConfigOption<?> configOption = validateOptionName(optionName);
            configOption.setOnConfig(config, optionValue);
        });
    }

    /**
     * Sets a configuration option.
     */
    public void option(String optionName, boolean optionValue) {
        tryToSetOption(() -> {
            DeltaConfigOption<?> configOption = validateOptionName(optionName);
            configOption.setOnConfig(config, optionValue);
        });
    }

    /**
     * Sets a configuration option.
     */
    public void option(String optionName, int optionValue) {
        tryToSetOption(() -> {
            DeltaConfigOption<?> configOption = validateOptionName(optionName);
            configOption.setOnConfig(config, optionValue);
        });
    }

    /**
     * Sets a configuration option.
     */
    public void option(String optionName, long optionValue) {
        tryToSetOption(() -> {
            DeltaConfigOption<?> configOption = validateOptionName(optionName);
            configOption.setOnConfig(config, optionValue);
        });
    }

    private void tryToSetOption(Executable argument) {
        try {
            argument.execute();
        } catch (Exception e) {
            throw new IllegalArgumentException(e);

        }
    }

    @SuppressWarnings("unchecked")
    protected <TYPE> DeltaConfigOption<TYPE> validateOptionName(String optionName) {
        DeltaConfigOption<TYPE> option =
            (DeltaConfigOption<TYPE>) DeltaSourceOptions.USER_FACING_SOURCE_OPTIONS.get(optionName);
        if (option == null) {
            throw new IllegalArgumentException(optionName);
        }
        return option;
    }


    @FunctionalInterface
    private interface Executable {
        void execute();
    }
}
