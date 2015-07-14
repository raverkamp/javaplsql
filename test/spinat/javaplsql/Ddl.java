package spinat.javaplsql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.StringTokenizer;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.OracleStatement;

public class Ddl {

    public static void createType(OracleConnection c, String s) {
        OracleStatement stm;
        try {
            stm = (OracleStatement) c.createStatement();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
        StringTokenizer st = new StringTokenizer(s);
        String create = st.nextToken();
        String type = st.nextToken();
        String name = st.nextToken();

        try {
            stm.execute("drop type " + name + " force");
        } catch (SQLException x) {
            System.err.printf(x.getMessage());
        }

        try {
            stm.execute(s);
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static void call(OracleConnection con, String... s) throws SQLException {
        try (Statement stm = con.createStatement()) {
            stm.execute(String.join("\n", s));
        }
    }

    public static void dropSynonyms(OracleConnection con) throws SQLException {
        Statement stm = con.createStatement();
        ResultSet rs = stm.executeQuery("select synonym_name from user_synonyms");
        while (rs.next()) {
            Ddl.call(con, "drop synonym " + rs.getString(1));
        }
    }

    public static void dropGrants(OracleConnection con) throws SQLException {
        Statement stm = con.createStatement();
        ResultSet rs = stm.executeQuery("select grantee,table_name,privilege from USER_TAB_PRIVS_MADE");
        while (rs.next()) {
            Ddl.call(con, "revoke " + rs.getString("PRIVILEGE") + " on " + rs.getString("TABLE_NAME") + " from " + rs.getString("GRANTEE"));
        }
    }
}
