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

public class OverloadTest {
    
    public OverloadTest() {
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
        String user = props.getProperty("user1").toUpperCase();
        connection = (OracleConnection) DriverManager.getConnection(props.getProperty("url"),
                user, props.getProperty("pw1"));
        HashMap<String, String> a = TestUtil.loadSnippets("snippets.txt");

        Ddl.call(connection, a.get("pack_overload_spec"));
        Ddl.call(connection, a.get("pack_overload_body"));

        Ddl.createType(connection, "create type number_array as table of number;");
        Ddl.createType(connection, "create type varchar2_array as table of varchar2(32767);");
        Ddl.createType(connection, "create type date_array as table of date;");
    }
    
    @After
    public void tearDown() {
    }
    
    @Test
    public void test1() throws SQLException {
        Map<String,Object> args = new HashMap<>();
        args.put("A", 17);
        ProcedureCaller p = new ProcedureCaller(connection);
        Map<String,Object> res = p.call("pack_overload.p1",1, args);
        assertEquals("2/17", res.get("TXT"));
    }
    
}
