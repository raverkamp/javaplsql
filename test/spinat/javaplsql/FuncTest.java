package spinat.javaplsql;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import oracle.jdbc.OracleConnection;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import spinat.javaplsql.ProcedureCaller.Box;

public class FuncTest {

    public FuncTest() {
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
        Ddl.call(connection, a.get("proc1"));

        Ddl.createType(connection, "create type number_array as table of number;");
        Ddl.createType(connection, "create type varchar2_array as table of varchar2(32767);");
        Ddl.createType(connection, "create type date_array as table of date;");
        Ddl.createType(connection, "create type raw_array as table of raw(32767);");
    }

    @After
    public void tearDown() {
    }

    @Test
    public void t1() throws SQLException {
        HashMap<String, Object> ar = new HashMap<>();
        ar.put("XI", 12);
        ar.put("YI", "x");
        ar.put("ZI", new Date());
        Map<String, Object> res = new ProcedureCaller(connection).call("P1.P", ar);
        System.out.println(res);
        assertTrue(res.get("XO").equals(new BigDecimal(13)) && res.get("YO").equals("xx"));
    }

    @Test
    public void t1p() throws SQLException {
        HashMap<String, Object> ar = new HashMap<>();
        ar.put("XI", 12);
        ar.put("YI", "x");
        ar.put("ZI", new Date());
        ProcedureCaller p = new ProcedureCaller(connection);
        Box<BigDecimal> xo = new Box<>();
        Box<String> yo = new Box<>();
        Box<java.sql.Timestamp> zo = new Box<>();

        p.callPositional("P1.P", 12, "x", new Date(), xo, yo, zo);
        assertTrue(xo.value.equals(new BigDecimal(13)) && yo.value.equals("xx"));
    }

    @Test
    public void test2() throws SQLException {
        HashMap<String, Object> ar = new HashMap<>();
        Map<String, Object> a = new HashMap();
        a.put("X", 12);
        a.put("Y", "x");
        a.put("Z", new Date());
        ar.put("A", a);
        Map<String, Object> res = new ProcedureCaller(connection).call("P1.P2", ar);
        System.out.println(res);
        Map<String, Object> m = (Map<String, Object>) res.get("B");
        assertTrue(m.get("X").equals(new BigDecimal(13)) && m.get("Y").equals("xx"));
    }

    @Test
    public void test2p() throws SQLException {
        Map<String, Object> a = new HashMap();
        a.put("X", 12);
        a.put("Y", "x");
        a.put("Z", new Date());
        ProcedureCaller p = new ProcedureCaller(connection);
        Box<Map<String, Object>> b = new Box<>();
        p.callPositional("P1.P2", a, b);
        Map<String, Object> m = b.value;
        assertTrue(m.get("X").equals(new BigDecimal(13)) && m.get("Y").equals("xx"));
    }

    public void test3Base(int size) throws SQLException {
        HashMap<String, Object> ar = new HashMap<>();
        ArrayList<Map<String, Object>> l = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            Map<String, Object> a = new HashMap();
            a.put("X", new BigDecimal(i));
            a.put("Y", "x" + i);
            a.put("Z", new Date());
            l.add(a);
        }
        ar.put("A", l);
        Map<String, Object> res = new ProcedureCaller(connection).call("P1.P3", ar);
        ArrayList<Map<String, Object>> l2 = (ArrayList<Map<String, Object>>) res.get("B");
        for (int i = 0; i < l.size(); i++) {
            Map<String, Object> m = l.get(i);
            Map<String, Object> m2 = l2.get(i);
            assertTrue(m2.get("X").equals(((BigDecimal) m.get("X")).add(BigDecimal.ONE))
                    && m2.get("Y").equals("" + m.get("Y") + m.get("Y")));
        }
    }

    @Test
    public void test3() throws SQLException {
        test3Base(10);
    }

    @Test
    public void test4() throws SQLException {
        HashMap<String, Object> ar = new HashMap<>();
        ArrayList l0 = new ArrayList<>();
        for (int j = 0; j < 3; j++) {
            ArrayList<Map<String, Object>> l = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                Map<String, Object> a = new HashMap();
                a.put("X", new BigDecimal(i));
                a.put("Y", "x" + i);
                a.put("Z", new Date(2013, 5, 1));
                l.add(a);
            }
            l0.add(l);
        }
        ar.put("A", l0);
        Map<String, Object> res = new ProcedureCaller(connection).call("P1.P4", ar);
        System.out.println(res);
        ArrayList l2 = (ArrayList) res.get("A");
        for (int i = 0; i < l2.size(); i++) {
            ArrayList x = (ArrayList) l2.get(i);
            ArrayList y = (ArrayList) l0.get(i);
            for (int j = 0; j < x.size(); j++) {
                Map<String, Object> m1 = (Map<String, Object>) x.get(j);
                Map<String, Object> m2 = (Map<String, Object>) y.get(j);
                assertTrue(m1.get("X").equals(m2.get("X")) && m1.get("Y").equals(m2.get("Y"))
                        && m1.get("Z").equals(m2.get("Z")));
            }
        }
    }

    @Test
    public void test5() throws SQLException {
        HashMap<String, Object> ar = new HashMap<>();
        Map<String, Object> a = new HashMap();
        a.put("X", null);
        a.put("Y", null);
        a.put("Z", null);
        ar.put("A", a);
        Map<String, Object> res = new ProcedureCaller(connection).call("P1.P2", ar);
        System.out.println(res);
        Map<String, Object> m = (Map<String, Object>) res.get("B");
        assertTrue(m.get("X") == null && m.get("Y") == null);
    }

    @Test
    public void test5p() throws SQLException {
        Map<String, Object> a = new HashMap();
        a.put("X", null);
        a.put("Y", null);
        a.put("Z", null);
        Box<Map> b = new Box<>();
        new ProcedureCaller(connection).callPositional("P1.P2", a, b);
        Map<String, Object> m = (Map<String, Object>) b.value;
        assertTrue(m.get("X") == null && m.get("Y") == null);
    }

    @Test
    public void test6() throws SQLException {
        HashMap<String, Object> ar = new HashMap<>();
        ArrayList l0 = new ArrayList<>();
        for (int j = 0; j < 3; j++) {
            l0.add(null);
        }
        ar.put("A", l0);
        Map<String, Object> res = new ProcedureCaller(connection).call("P1.P4", ar);
        System.out.println(res);
        ArrayList l2 = (ArrayList) res.get("A");
        for (int i = 0; i < l2.size(); i++) {
            assertTrue(l2.get(i) == null);
        }
    }

    @Test
    public void test7() throws SQLException {
        Map<String, Object> a = new HashMap<>();
        a.put("X", 23);
        a.put("Y", "roland");
        a.put("Z", new java.util.Date());
        Map<String, Object> m = new ProcedureCaller(connection).call("proc1", a);
    }

    @Test
    public void test8() throws SQLException {
        Map<String, Object> a = new HashMap<>();
        Map<String, Object> m = new ProcedureCaller(connection).call("p1.p6", a);
    }

    @Test
    public void test9() throws SQLException {
        Map<String, Object> a = new HashMap<>();
        a.put("A", -123);
        a.put("B", "rote gruetze");
        java.sql.Timestamp dat = new java.sql.Timestamp(2014, 12, 3, 23, 45, 1, 0);
        a.put("C", dat);
        Map<String, Object> m = new ProcedureCaller(connection).call("p1.f7", a);
        Map<String, Object> r = (Map<String, Object>) m.get("RETURN");
        assertTrue(r.get("X").equals(BigDecimal.valueOf(-123))
                && r.get("Y").equals("rote gruetze")
                && r.get("Z").equals(dat));
    }

    @Test
    public void test10() throws SQLException {
        Map<String, Object> a = new HashMap<>();
        {
            a.put("X", true);
            Map<String, Object> m = new ProcedureCaller(connection).call("p1.p8", a);
            Boolean y = (Boolean) (m.get("Y"));
            assertTrue(!y);
        }
        {
            a.put("X", false);
            Map<String, Object> m = new ProcedureCaller(connection).call("p1.p8", a);
            Boolean y = (Boolean) (m.get("Y"));
            assertTrue(y);
        }
        {
            a.put("X", null);
            Map<String, Object> m = new ProcedureCaller(connection).call("p1.p8", a);
            Boolean y = (Boolean) (m.get("Y"));
            assertTrue(y == null);
        }
    }

    @Test
    public void test11() throws SQLException {
        Map<String, Object> a = new HashMap<>();
        {
            a.put("X1", 1);
            a.put("X2", 19);
            Map<String, Object> m = new ProcedureCaller(connection).call("p1.p9", a);
            BigDecimal y1 = (BigDecimal) m.get("Y1");
            assertTrue(y1.equals(BigDecimal.valueOf(-1)));
            BigDecimal y2 = (BigDecimal) m.get("Y2");
            assertTrue(y2.equals(BigDecimal.valueOf(29)));
        }
    }

    // procedure p5 takes a table is argument and return it in its
    // out argument
    @Test
    public void testTableNull() throws SQLException {
        Box<Object> b = new Box<>();
        ProcedureCaller p = new ProcedureCaller(connection);
        p.callPositional("p1.p5", null, b);
        assertNull(b.value);

        ArrayList<String> l = new ArrayList<>();
        p.callPositional("p1.p5", l, b);
        assertNotNull(b.value);
        assertEquals(0, ((ArrayList) b.value).size());
    }

    // test various methods to write a strored procedure
    @Test
    public void testName1() throws SQLException {
        Map<String, Object> a = new HashMap<>();
        ProcedureCaller p = new ProcedureCaller(connection);
        Map<String, Object> res;
        res = p.call("p1.no_args", a);
        res = p.call("\"P1\".no_args", a);
        res = p.call("\"P1\".\"NO_ARGS\"", a);
        res = p.call("p1.\"NO_ARGS\"", a);
        res = p.call("p1.\"NO_ARGS\"", a);
    }

    @Test
    public void testName2() throws SQLException {
        Map<String, Object> a = new HashMap<>();
        ProcedureCaller p = new ProcedureCaller(connection);
        Map<String, Object> res;
        Exception ex = null;
        try {
            res = p.call("p1.this_proc_does_not_exist", a);
        } catch (Exception exe) {
            ex = exe;
        }
        assertNotNull(ex);
        assertTrue(ex instanceof RuntimeException);
        assertTrue(ex.getMessage().contains("procedure in package does not exist or object is not valid"));
    }

    @Test
    public void TestSysRefCursorPos() throws SQLException {
        ProcedureCaller p = new ProcedureCaller(connection);
        Box<Object> b = new Box<>();
        Date dat = new Date(2001, 12, 1);
        p.callPositional("p1.pcursor1", 17, "xyz", dat, b);
        ArrayList<Map<String, Object>> l = (ArrayList<Map<String, Object>>) b.value;
        assertTrue(l.size() == 2);
        Map<String, Object> r2 = l.get(1);
        assertEquals(r2.get("A"), "xyz");
        assertEquals(((BigDecimal) r2.get("B")).intValue(), 17);
        assertEquals(r2.get("C"), dat);
    }

    @Test
    public void TestSysRefCursorNamed() throws SQLException {
        ProcedureCaller p = new ProcedureCaller(connection);
        Box<Object> b = new Box<>();
        Date dat = new Date(2001, 12, 1);
        HashMap<String, Object> args = new HashMap<>();
        args.put("N", 17);
        args.put("V", "xyz");
        args.put("D", dat);
        Map<String, Object> res = p.call("p1.pcursor1", args);
        ArrayList<Map<String, Object>> l = (ArrayList<Map<String, Object>>) res.get("C");
        assertTrue(l.size() == 2);
        Map<String, Object> r2 = l.get(1);
        assertEquals(r2.get("A"), "xyz");
        assertEquals(((BigDecimal) r2.get("B")).intValue(), 17);
        assertEquals(r2.get("C"), dat);
    }

    @Test
    public void TestRefCursorPos() throws SQLException {
        ProcedureCaller p = new ProcedureCaller(connection);
        Box<Object> b = new Box<>();
        Date dat = new Date(2001, 12, 1);
        p.callPositional("p1.pcursor2", 17, "xyz", dat, b);
        ArrayList<Map<String, Object>> l = (ArrayList<Map<String, Object>>) b.value;
        assertTrue(l.size() == 2);
        Map<String, Object> r2 = l.get(1);
        assertEquals(r2.get("V"), "xyz");
        assertEquals(((BigDecimal) r2.get("N")).intValue(), 17);
        assertEquals(r2.get("D"), dat);
    }

    @Test
    public void TestRefCursorNamed() throws SQLException {
        ProcedureCaller p = new ProcedureCaller(connection);
        Box<Object> b = new Box<>();
        Date dat = new Date(2001, 12, 1);
        HashMap<String, Object> args = new HashMap<>();
        args.put("N", 17);
        args.put("V", "xyz");
        args.put("D", dat);
        Map<String, Object> res = p.call("p1.pcursor2", args);
        ArrayList<Map<String, Object>> l = (ArrayList<Map<String, Object>>) res.get("C");
        assertTrue(l.size() == 2);
        Map<String, Object> r2 = l.get(1);
        assertEquals(r2.get("V"), "xyz");
        assertEquals(((BigDecimal) r2.get("N")).intValue(), 17);
        assertEquals(r2.get("D"), dat);
    }

    @Test
    public void TestRefCursor3() throws SQLException {
        ProcedureCaller p = new ProcedureCaller(connection);
        Box<Object> b = new Box<>();
        Exception ex = null;
        try {
            p.callPositional("p1.pcursor3", b);
        } catch (Exception exe) {
            ex = exe;
        }
        assertTrue(ex != null && ex instanceof RuntimeException);
        assertTrue(ex.getMessage().contains("%rowtype"));
    }

    @Test
    public void TestCursorInCursor() throws SQLException {
        ProcedureCaller p = new ProcedureCaller(connection);
        Box<Object> b = new Box<>();
        p.callPositional("p1.cursor_in_cursor", b);
        ArrayList<Map<String, Object>> l = (ArrayList<Map<String, Object>>) b.value;
        assertTrue(l.size() == 1);
        Object o = l.get(0).get("DUMMY2");
        ArrayList<Map<String, Object>> l2 = (ArrayList<Map<String, Object>>) o;
        assertTrue(l2.size() == 1);
        assertEquals("X", l2.get(0).get("DUMMY"));
    }
    @Test
    public void TestCursorInCursorInCursor() throws SQLException {
        ProcedureCaller p = new ProcedureCaller(connection);
        Box<Object> b = new Box<>();
        p.callPositional("p1.cursor_in_cursor_in_cursor", b);
        ArrayList<Map<String, Object>> l = (ArrayList<Map<String, Object>>) b.value;
        assertTrue(l.size() == 1);
        Object o2 = l.get(0).get("DUMMY2");
        ArrayList<Map<String, Object>> l2 = (ArrayList<Map<String, Object>>) o2;
        assertTrue(l2.size() == 1);
        assertEquals("X", l2.get(0).get("DUMMY"));
        Object o3 = l2.get(0).get("DUMMY3");
        ArrayList<Map<String, Object>> l3 = (ArrayList<Map<String, Object>>) o3;
        assertEquals("X", l3.get(0).get("DUMMY"));
    }
    
    @Test
    public void TestIndexBy() throws SQLException {
        int n = 20;
        ProcedureCaller p = new ProcedureCaller(connection);
        Map<String, Object> args = new HashMap<>();
        TreeMap<String, String> ai = new TreeMap<>();
        for (int i = 0; i < n; i++) {
            ai.put("k" + i, "v" + i);
        }
        args.put("AI", ai);
        TreeMap<Integer, String> bi = new TreeMap<>();
        for (int i = 0; i < n; i++) {
            bi.put(i, "v" + i);
        }
        args.put("BI", bi);
        Map<String, Object> res = p.call("p1.pindex_tab", args);
        TreeMap<String, String> ao = (TreeMap<String, String>) res.get("AO");
        TreeMap<Integer, String> bo = (TreeMap<Integer, String>) res.get("BO");
        assertTrue(ao.size() == n);
        for (int i = 0; i < n; i++) {
            assertTrue(ao.get("xk" + i).equals(ai.get("k" + i) + "y"));
        }

        assertTrue(bo.size() == n);
        for (int i = 0; i < n; i++) {
            assertTrue(bo.get(2 * i).equals(bi.get(i) + "y"));
        }
    }

    @Test
    public void TestIndexBy2() throws SQLException {
        int n = 10;
        ProcedureCaller p = new ProcedureCaller(connection);
        Map<String, Object> args = new HashMap<>();
        TreeMap<String, Object> tm = new TreeMap<>();
        args.put("X", tm);
        for (int i = 0; i < n; i++) {
            TreeMap<String, String> h = new TreeMap<>();
            for (int j = 0; j < n; j++) {
                h.put("" + j + "," + i, "v" + j + "," + i);
            }
            tm.put("a" + i, h);
        }
        Map<String, Object> res = p.call("p1.pindex_tab2", args);
        TreeMap<String, Object> tm2 = (TreeMap<String, Object>) res.get("X");
        assertTrue(tm2.size() == tm.size());
        for (Map.Entry<String, Object> kv : tm.entrySet()) {
            TreeMap<String, String> h1 = (TreeMap<String, String>) kv.getValue();
            TreeMap<String, String> h2 = (TreeMap<String, String>) tm2.get(kv.getKey());
            assertTrue(h1.size() == h2.size());
            for (Map.Entry<String, String> kv2 : h1.entrySet()) {
                assertTrue(kv2.getValue().equals(h2.get(kv2.getKey())));
            }
        }
    }

    @Test
    public void testRaw() throws SQLException {
        ProcedureCaller p = new ProcedureCaller(connection);
        Map<String, Object> args = new HashMap<>();
        byte[] b = new byte[]{1, 2, 3, 4, 76, 97};
        args.put("X", b);
        Map<String, Object> res = p.call("p1.praw", args);
        byte[] b2 = (byte[]) res.get("Y");
        assertTrue(b2.length == b.length);
        for (int i = 0; i < b2.length; i++) {
            assertTrue(b[i] == b2[i]);
        }
    }

    @Test
    public void testRawPos() throws SQLException {
        ProcedureCaller p = new ProcedureCaller(connection);
        Map<String, Object> args = new HashMap<>();
        byte[] b = new byte[]{1, 2, 3, 4, 76, 97};
        args.put("X", b);
        Box<byte[]> y = new Box<>();
        Object res = p.callPositional("p1.praw", b, y);
        byte[] b2 = y.value;
        assertTrue(b2.length == b.length);
        for (int i = 0; i < b2.length; i++) {
            assertTrue(b[i] == b2[i]);
        }
    }

    @Test
    public void testRaw2() throws SQLException {
        ProcedureCaller p = new ProcedureCaller(connection);
        Map<String, Object> args = new HashMap<>();
        byte[] b = new byte[0];
        args.put("X", b);
        Map<String, Object> res = p.call("p1.praw", args);
        byte[] b2 = (byte[]) res.get("Y");
        assertTrue(b2 == null);
        //
        args.put("X", null);
        Map<String, Object> res2 = p.call("p1.praw", args);
        byte[] b22 = (byte[]) res.get("Y");
        assertTrue(b22 == null);
    }

    @Test
    public void testRawBig() throws SQLException {
        ProcedureCaller p = new ProcedureCaller(connection);
        Map<String, Object> args = new HashMap<>();
        byte[] b = new byte[32767];
        for (int i = 0; i < b.length; i++) {
            b[i] = (byte) (i * 7 & 255);
        }
        args.put("X", b);
        Map<String, Object> res = p.call("p1.praw", args);
        byte[] b2 = (byte[]) res.get("Y");
        assertTrue(b2.length == b.length);
        for (int i = 0; i < b2.length; i++) {
            assertTrue(b[i] == b2[i]);
        }
    }
}
