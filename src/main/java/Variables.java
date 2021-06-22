import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class Variables {

    public static final String BOOTSTRAP_SERVERS = "BOOTSTRAP_SERVERS";
    public static final String DATABASES_UPLOAD_NOTIFICATIONS_TOPIC = "DATABASES_UPLOAD_NOTIFICATIONS_TOPIC";
    public static final String PIPELINES_SET_UPLOAD_NOTIFICATIONS_TOPIC_FORMAT = "PIPELINES_SET_UPLOAD_NOTIFICATIONS_TOPIC_FORMAT";
    public static final String DATA_TO_PARSE_TOPIC_FORMAT = "DATA_TO_PARSE_TOPIC_FORMAT";
    public static final String PIPELINES_SETS_DONE_TOPIC = "PIPELINES_SETS_DONE_TOPIC";
    private static final Map<String, String> defaults;
    private static final String DATABASES_UPLOAD_NOTIFICATIONS_TOPIC_DEFAULT = "DATABASES_UPLOAD_NOTIFICATIONS";
    private static final String PIPELINES_SET_UPLOAD_NOTIFICATIONS_TOPIC_FORMAT_DEFAULT = "PIPELINES_SET_%d_UPLOAD_NOTIFICATIONS";
    private static final String DATA_TO_PARSE_TOPIC_FORMAT_DEFAULT = "PIPELINES_SET_%d_DATA_TO_PARSE";
    private static final String PIPELINES_SETS_DONE_TOPIC_DEFAULT = "PIPELINES_SETS_DONE";

    private static final Logger LOGGER = Logger.getLogger(Variables.class.getName());

    static {
        defaults = new HashMap<>();
        defaults.put(DATABASES_UPLOAD_NOTIFICATIONS_TOPIC, DATABASES_UPLOAD_NOTIFICATIONS_TOPIC_DEFAULT);
        defaults.put(PIPELINES_SET_UPLOAD_NOTIFICATIONS_TOPIC_FORMAT, PIPELINES_SET_UPLOAD_NOTIFICATIONS_TOPIC_FORMAT_DEFAULT);
        defaults.put(DATA_TO_PARSE_TOPIC_FORMAT, DATA_TO_PARSE_TOPIC_FORMAT_DEFAULT);
        defaults.put(PIPELINES_SETS_DONE_TOPIC, PIPELINES_SETS_DONE_TOPIC_DEFAULT);
    }

    public static String get(String env_var) {
        var value = System.getenv().getOrDefault(env_var, defaults.get(env_var));

        if (value == null) {
            LOGGER.severe(String.format("Environment variable %s with no default not defined", env_var));
            System.exit(2);
        }

        return value;
    }

}
