import java.util.logging.Logger;

public class Variables {

    public static final String BOOTSTRAP_SERVERS = "BOOTSTRAP_SERVERS";
    public static final String SCHEMA_REGISTRY_HOST = "SCHEMA_REGISTRY_HOST";
    public static final String SCHEMA_REGISTRY_PORT = "SCHEMA_REGISTRY_PORT";

    private static final Logger LOGGER = Logger.getLogger(Variables.class.getName());

    public static String get(String env_var) {
        var value = System.getenv().get(env_var);

        if (value == null) {
            LOGGER.severe(String.format("Environment variable %s with no default not defined", env_var));
            System.exit(2);
        }

        return value;
    }

    public static String get_with_default(String env_var, String default_value) {
        var value = System.getenv().get(env_var);

        if (value == null) {
            return default_value;
        }
        return value;
    }

}
