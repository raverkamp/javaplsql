package spinat.javaplsql;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Date;
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

public class CrossSchemaTest {

    public CrossSchemaTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    OracleConnection getCon1() throws SQLException {
        Properties props = TestUtil.getProperties("config1.txt");
        String user = props.getProperty("user1").toUpperCase();
        return (OracleConnection) DriverManager.getConnection(props.getProperty("url"),
                user, props.getProperty("pw1"));
    }

    OracleConnection getCon2() throws SQLException {
        Properties props = TestUtil.getProperties("config1.txt");
        String user = props.getProperty("user2").toUpperCase();
        return (OracleConnection) DriverManager.getConnection(props.getProperty("url"),
                user, props.getProperty("pw2"));
    }

    String schema1 = TestUtil.getProperties("config1.txt").getProperty("user1").toUpperCase();

    @Before
    public void setUp() throws SQLException, IOException {
        try (OracleConnection connection = getCon1()) {
            HashMap<String, String> a = TestUtil.loadSnippets("snippets.txt");

            Ddl.call(connection, a.get("p1_spec"));
            Ddl.call(connection, a.get("p1_body"));
            Ddl.call(connection, a.get("proc1"));

            Ddl.createType(connection, "create type number_array as table of number;");
            Ddl.createType(connection, "create type varchar2_array as table of varchar2(32767);");
            Ddl.createType(connection, "create type date_array as table of date;");
        }
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testWithSyn() throws SQLException {
        OracleConnection con2;
        con2 = getCon2();
        Ddl.dropSynonyms(con2);
        try (OracleConnection con1 = getCon1()) {
            Ddl.dropGrants(con1);
            for (String s : new String[]{"p1", "number_array", "varchar2_array", "date_array"}) {
                Ddl.call(con1, " grant execute on " + s + " to public");
                Ddl.call(con2, " create or replace synonym " + s + " for " + schema1 + "." + s);
            }
        }

        ProcedureCaller p = new ProcedureCaller(con2);
        HashMap<String, Object> ar = new HashMap<>();
        ar.put("XI", 12);
        ar.put("YI", "x");
        ar.put("ZI", new Date());
        Map<String, Object> res = p.call("P1.P", ar);
        System.out.println(res);
        assertTrue(res.get("XO").equals(new BigDecimal(13)) && res.get("YO").equals("xx"));
    }

    // create array descriptors with set current_schema does not work
    @Test
    public void testWithCurrentSchemaBug() throws SQLException {
        OracleConnection con2;
        con2 = getCon2();
        Ddl.dropSynonyms(con2);
        try (OracleConnection con1 = getCon1()) {
            Ddl.dropGrants(con1);
            for (String s : new String[]{"p1", "number_array", "varchar2_array", "date_array"}) {
                Ddl.call(con1, " grant execute on " + s + " to public");
            }
        }
        // first check that we are not in the correct schema
        {
            Exception x = null;
            try {
                CallableStatement cstm = con2.prepareCall("begin p1.no_args; end;");
                cstm.execute();
            } catch (SQLException ex) {
                x = ex;
            }
            // it should have failed
            assertNotNull(x);
        }

        // now switch to right schema, now it works
        Ddl.call(con2, "alter session set current_schema =" + schema1);
        {
            CallableStatement cstm = con2.prepareCall("begin p1.no_args; end;");
            cstm.execute();
        }

        // but is is not possible to resolve number_array
        Exception ex = null;
        try {
            Object o = con2.createARRAY("NUMBER_ARRAY", new BigDecimal[0]);
        } catch (Exception e) {
            ex = e;
        }
        assertNotNull(ex);
        con2.close();

        // it works using with explicit schema
        con2 = getCon2();
        ex = null;
        try {
            Object o = con2.createARRAY(schema1 + ".NUMBER_ARRAY", new BigDecimal[0]);
        } catch (Exception e) {
            ex = e;
        }
        assertNull(ex);

        // very funny, lower case is not OK
        ex = null;
        try {
            Object o = con2.createARRAY(schema1 + ".number_array", new BigDecimal[0]);
        } catch (Exception e) {
            ex = e;
        }
        assertNotNull(ex);
        con2.close();

        // using a synonym works
        con2 = getCon2();
        ex = null;
        try {
            Ddl.call(con2, "create or replace synonym number_array for " + schema1 + ".number_array");
            Object o = con2.createARRAY("NUMBER_ARRAY", new BigDecimal[0]);
        } catch (RuntimeException e) {
            ex = e;
            System.out.println(ex);
        }
        assertNull(ex);
    }

    @Test
    public void testWithCurrentSchemaAndOwnerForTables() throws SQLException {
        OracleConnection con2;
        con2 = getCon2();
        Ddl.dropSynonyms(con2);
        try (OracleConnection con1 = getCon1()) {
            Ddl.dropGrants(con1);
            for (String s : new String[]{"p1", "number_array", "varchar2_array", "date_array"}) {
                Ddl.call(con1, " grant execute on " + s + " to public");
            }
        }
        Ddl.call(con2, "alter session set current_schema =" + schema1);
        ProcedureCaller p = new ProcedureCaller(con2);
        p.setNumberTableName(schema1 + "." + p.getNumberTableName());
        p.setDateTableName(schema1 + "." + p.getDateTableName());
        p.setVarchar2TableName(schema1 + "." + p.getVarchar2TableName());
        HashMap<String, Object> ar = new HashMap<>();
        ar.put("XI", 12);
        ar.put("YI", "x");
        ar.put("ZI", new Date());
        Map<String, Object> res = p.call("P1.P", ar);
        System.out.println(res);
        assertTrue(res.get("XO").equals(new BigDecimal(13)) && res.get("YO").equals("xx"));
    }

    @Test
    public void testWithCurrentSchema() throws SQLException {
        OracleConnection con2;
        con2 = getCon2();
        Ddl.dropSynonyms(con2);
        try (OracleConnection con1 = getCon1()) {
            Ddl.dropGrants(con1);
            for (String s : new String[]{"p1", "number_array", "varchar2_array", "date_array"}) {
                Ddl.call(con1, " grant execute on " + s + " to public");
            }
        }
        Ddl.call(con2, "alter session set current_schema =" + schema1);
        ProcedureCaller p = new ProcedureCaller(con2);
        HashMap<String, Object> ar = new HashMap<>();
        ar.put("XI", 12);
        ar.put("YI", "x");
        ar.put("ZI", new Date());
        Map<String, Object> res = p.call("P1.P", ar);
        System.out.println(res);
        assertTrue(res.get("XO").equals(new BigDecimal(13)) && res.get("YO").equals("xx"));
    }
}
