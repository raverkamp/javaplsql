/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
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
import oracle.jdbc.OracleConnection;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;


public class PerfTest {

    public PerfTest() {
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
            assertTrue (m2.get("X").equals(((BigDecimal) m.get("X")).add(BigDecimal.ONE))
                    && m2.get("Y").equals("" + m.get("Y") + m.get("Y")));
        }
    }

    void perfTest(int k) throws SQLException {
        int n = 1;
        DbmsOutput.enableDbmsOutput(connection, 0);
        for (int i = 0; i < k; i++) {
            long l = System.currentTimeMillis();
            test3Base( n);
            l = System.currentTimeMillis() - l;
            System.out.println("" + n + ": " + l);
            for (String s : DbmsOutput.fetchDbmsOutput(connection)) {
                System.out.println("  " + s);
            }

            n = n * 2;
        }
    }

    void sizeTest(int rec_size, int args_size) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("create or replace package p2 as\n")
                .append("type r is record (\n");
        for (int i = 0; i < rec_size; i++) {
            if (i > 0) {
                sb.append(",\n");
            }
            sb.append("x" + i + " number,y" + i + " varchar2(200),z" + i + " date");
        }
        sb.append(");\n");
        sb.append("procedure p(");
        for (int i = 0; i < args_size; i++) {
            if (i > 0) {
                sb.append(",\n");
            }
            sb.append("in" + i + " r,out" + i + " out r\n");

        }
        sb.append(");\n");
        sb.append("end;\n");
        Ddl.call(connection, sb.toString());
        sb = new StringBuilder();
        sb.append("create or replace package body p2 as\n");
        sb.append("procedure p(");
        for (int i = 0; i < args_size; i++) {
            if (i > 0) {
                sb.append(",\n");
            }
            sb.append("in" + i + " r,out" + i + " out r\n");
        }
        sb.append(") is\n");
        sb.append("begin\n");
        for (int i = 0; i < args_size; i++) {
            sb.append("out" + i + ":=in" + i + ";\n");
        }
        sb.append("end;\n");
        sb.append("end;\n");
        Ddl.call(connection, sb.toString());
        HashMap<String, Object> args = new HashMap<>();
        HashMap<String, Object> m = new HashMap<>();
        for (int i = 0; i < rec_size; i++) {
            m.put("X" + i, i * 8.78);
            m.put("Y" + i, "String" + i);
            m.put("z" + i, new Date());
        }
        for (int i = 0; i < args_size; i++) {
            args.put("IN" + i, m);
        }
        Map<String, Object> res = new ProcedureCaller(connection).call("P2.P", args);
        System.out.println(res);
    }

    void varcharSizeTest(int vsize, int asize) throws SQLException {
        ArrayList<String> a = new ArrayList<>();
        for (int i = 0; i < asize; i++) {
            StringBuilder sb = new StringBuilder();
            int j = 0;
            while (sb.length() < vsize) {
                sb.append("x" + j);
                j++;
            }
            a.add(sb.substring(0, vsize));
        }
        Map<String, Object> args = new HashMap<>();
        args.put("A", a);
        Map<String, Object> res = new ProcedureCaller(connection).call("P1.P5", args);
        ArrayList<String> b = (ArrayList<String>) res.get("B");
        for (int i = 0; i < Math.max(a.size(), b.size()); i++) {
            assertEquals(a.get(i),b.get(i)) ;
        }
    }

    @Test
    public void varcharSizeTest_32767_100() throws SQLException {
        varcharSizeTest(32767, 1);
    }

    @Test
    public void sizeTest_30_100() throws SQLException {
        sizeTest(3, 1);
    }

    @Test
    public void perfTest_1000() throws SQLException {
        perfTest(1);
    }

}
