package spinat.javaplsql;

import java.io.IOException;
import java.math.BigDecimal;
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

public class ParamTableNameTest {

    public ParamTableNameTest() {
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

        Ddl.call(connection, a.get("p1_spec"));
        Ddl.call(connection, a.get("p1_body"));
    }

    @After
    public void tearDown() {
    }

    void testx(int i) throws SQLException {
        String na = "na" + i;
        String va = "va" + i;
        String da = "da" + i;
        String ra = "ra" + i;
        Ddl.createType(connection, "create type " + na + " as table of number;");
        Ddl.createType(connection, "create type " + va + " as table of varchar2(32767);");
        Ddl.createType(connection, "create type " + da + " as table of date;");
        Ddl.createType(connection, "create type " + ra + " as table of raw(32767);");
        ProcedureCaller p = new ProcedureCaller(connection);
        p.setNumberTableName(na);
        p.setVarchar2TableName(va);
        p.setDateTableName(da);
        p.setRawTableName(ra);
        HashMap<String, Object> ar = new HashMap<>();
        ar.put("XI", 12);
        ar.put("YI", "x");
        ar.put("ZI", new Date());
        Map<String, Object> res = new ProcedureCaller(connection).call("P1.P", ar);
        System.out.println(res);
        assertTrue(res.get("XO").equals(new BigDecimal(13)) && res.get("YO").equals("xx"));
    }

    @Test
    public void test1() throws SQLException {
        for (int i = 0; i < 2; i++) {
            testx(i);
        }
    }

}
