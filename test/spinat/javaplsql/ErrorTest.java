package spinat.javaplsql;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import oracle.jdbc.OracleConnection;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

public class ErrorTest {

    public ErrorTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }
    OracleConnection connection;

    @Before
    public void setUp() throws SQLException, IOException {
        Properties props = TestUtil.getProperties("config1.txt");
        String user = props.getProperty("user").toUpperCase();
        connection = (OracleConnection) DriverManager.getConnection(props.getProperty("url"),
                user, props.getProperty("pw"));
        HashMap<String, String> a = TestUtil.loadSnippets("snippets.txt");

        Ddl.call(connection, a.get("p1_spec"));
        Ddl.call(connection, a.get("p1_body"));
        Ddl.call(connection, a.get("proc1"));

        Ddl.createType(connection, "create type number_array as table of number;");
        Ddl.createType(connection, "create type varchar2_array as table of varchar2(32767);");
        Ddl.createType(connection, "create type date_array as table of date;");
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testRaiseApplicationError() throws SQLException {
        ProcedureCaller p = new ProcedureCaller(connection);
        Map<String, Object> a = new HashMap<>();
        a.put("ERRNUM", -20012);
        a.put("TXT", "schlimmer fehler");
        // the output looks OK, maybe there is nothing to be done
        try {
            Map<String, Object> res = p.call("p1.raise_error", a);
            System.out.println(res);
        } catch (SQLException ex) {
            System.out.println(ex);
        }
    }

    @Test
    public void testToLargeVarchar2() throws SQLException {
        Class cl = null;
        ProcedureCaller p = new ProcedureCaller(connection);
        Map<String, Object> a = new HashMap<>();
        Map<String, Object> r = new HashMap<>();
        r.put("X", 1);
        r.put("Y", new String(new char[201]));
        r.put("Z", null);
        a.put("A", r);
        try {
            Map<String, Object> res = p.call("p1.p2", a);
        } catch (Exception ex) {
            System.out.println(ex);
            cl = ex.getClass();
        }
        assertEquals(ProcedureCaller.ConversionException.class, cl);
    }

    @Test
    public void testSlotMissing() throws SQLException {
        Class cl = null;
        ProcedureCaller p = new ProcedureCaller(connection);
        Map<String, Object> a = new HashMap<>();
        Map<String, Object> r = new HashMap<>();
        r.put("X", 1);
        //r.put("Y", new String(new char[20]));
        r.put("Z", null);
        a.put("A", r);
        try {
            Map<String, Object> res = p.call("p1.p2", a);
        } catch (Exception ex) {
            System.out.println(ex);
            cl = ex.getClass();
        }
        assertEquals(ProcedureCaller.ConversionException.class, cl);
    }

    @Test
    public void testParamMissing() throws SQLException {
        Class cl = null;
        ProcedureCaller p = new ProcedureCaller(connection);
        Map<String, Object> a = new HashMap<>();
        try {
            Map<String, Object> res = p.call("p1.p2", a);
        } catch (Exception ex) {
            System.out.println(ex);
            cl = ex.getClass();
        }
        assertEquals(ProcedureCaller.ConversionException.class, cl);
    }

}
