import java.util.logging.Logger;

public class Variables {

    public static final String BOOTSTRAP_SERVERS = "BOOTSTRAP_SERVERS";

    private static final Logger LOGGER = Logger.getLogger(Variables.class.getName());

    public static String get(String env_var) {
        var value = System.getenv().get(env_var);

        if (value == null) {
            LOGGER.severe(String.format("Environment variable %s with no default not defined", env_var));
            System.exit(2);
        }

        return value;
    }

}
