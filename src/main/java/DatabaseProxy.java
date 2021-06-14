import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

public final class DatabaseProxy implements AutoCloseable {

    private final Connection database_connection;

    public DatabaseProxy() throws SQLException {
        // TODO get this parameters by env variables
        this.database_connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", "postgres", "postgres");
    }

    public final Set<Long> pipelines_ids() {
        final var ret_val = new HashSet<Long>();

        try {
            final Statement s = this.database_connection.createStatement();
            final var results = s.executeQuery(Queries.PIPELINES_IDS);

            while (results.next()) {
                ret_val.add(results.getLong("id"));
            }
        } catch (SQLException ignored) {
        }

        return ret_val;
    }

    @Override
    public void close() throws SQLException {
        database_connection.close();
    }

    private static class Queries {

        public static final String PIPELINES_IDS = "SELECT id FROM pipeline WHERE status = 'ACTIVE'";
    }
}
