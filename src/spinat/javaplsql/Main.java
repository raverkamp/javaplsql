package spinat.javaplsql;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import oracle.jdbc.OracleConnection;

public class Main {

    public static void main(String[] args) throws IOException, SQLException {
        String s = args[0];
        Properties props = new java.util.Properties();
        props.load(new FileInputStream(s));
        String user = props.getProperty("user").toUpperCase();
        OracleConnection con = (OracleConnection) DriverManager.getConnection(props.getProperty("url"),
                user, props.getProperty("pw"));

        HashMap<String, String> a = loadSnippets(Main.class, "spinat/javaplsql/snippets.txt");
        Ddl.createType(con, "create type number_array as table of number;");
        Ddl.createType(con, "create type varchar2_array as table of varchar2(32767);");
        Ddl.createType(con, "create type date_array as table of date;");
        Ddl.call(con, a.get("p1_spec"));
        Ddl.call(con, a.get("p1_body"));
        System.out.println(Call.resolveName(con, "p1"));
        System.out.println(Call.resolveName(con, "bdms.p1"));
        System.out.println(Call.resolveName(con, "bdms.p1.p"));
        System.out.println(Call.resolveName(con, "dbms_utility.name_resolve"));
        System.out.println(Call.resolveName(con, "sys.dbms_utility.name_resolve"));
        //System.out.println(Call.resolveName(con, "bdms.dbms_utility.name_resolve"));

        test1(con, user);
        test2(con, user);
        test3(con, user, 2);
        test4(con, user);
        test5(con, user);
        test6(con, user);
        perfTest(con, user, 18);
        sizeTest(con, user, 1, 1);
        sizeTest(con, user, 10, 100);
        sizeTest(con, user, 10, 100);
        varcharSizeTest(con, user, 32767, 20);
    }

    static void test1(OracleConnection con, String user) throws SQLException {
        HashMap<String, Object> ar = new HashMap<>();
        ar.put("XI", 12);
        ar.put("YI", "x");
        ar.put("ZI", new Date());
        Map<String, Object> res = Call.CallProcedure(con, user, "P1", "P", ar);
        System.out.println(res);
        if (!(res.get("XO").equals(new BigDecimal(13)) && res.get("YO").equals("xx"))) {
            throw new RuntimeException("fail");
        }
    }

    static void test2(OracleConnection con, String user) throws SQLException {
        HashMap<String, Object> ar = new HashMap<>();
        Map<String, Object> a = new HashMap();
        a.put("X", 12);
        a.put("Y", "x");
        a.put("Z", new Date());
        ar.put("A", a);
        Map<String, Object> res = Call.CallProcedure(con, user, "P1", "P2", ar);
        System.out.println(res);
        Map<String, Object> m = (Map<String, Object>) res.get("B");
        if (!(m.get("X").equals(new BigDecimal(13)) && m.get("Y").equals("xx"))) {
            throw new RuntimeException("fail");
        }
    }

    static void test3(OracleConnection con, String user, int size) throws SQLException {
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
        Map<String, Object> res = Call.CallProcedure(con, user, "P1", "P3", ar);
        ArrayList<Map<String, Object>> l2 = (ArrayList<Map<String, Object>>) res.get("B");
        for (int i = 0; i < l.size(); i++) {
            Map<String, Object> m = l.get(i);
            Map<String, Object> m2 = l2.get(i);
            if (!(m2.get("X").equals(((BigDecimal) m.get("X")).add(BigDecimal.ONE))
                    && m2.get("Y").equals("" + m.get("Y") + m.get("Y")))) {
                throw new RuntimeException("fail");
            }
        }
    }

    static void test4(OracleConnection con, String user) throws SQLException {
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
        Map<String, Object> res = Call.CallProcedure(con, user, "P1", "P4", ar);
        System.out.println(res);
        ArrayList l2 = (ArrayList) res.get("A");
        for (int i = 0; i < l2.size(); i++) {
            ArrayList x = (ArrayList) l2.get(i);
            ArrayList y = (ArrayList) l0.get(i);
            for (int j = 0; j < x.size(); j++) {
                Map<String, Object> m1 = (Map<String, Object>) x.get(j);
                Map<String, Object> m2 = (Map<String, Object>) y.get(j);
                if (!(m1.get("X").equals(m2.get("X")) && m1.get("Y").equals(m2.get("Y"))
                        && m1.get("Z").equals(m2.get("Z")))) {
                    throw new RuntimeException("fail");
                }
            }

        }
    }

    static void test5(OracleConnection con, String user) throws SQLException {
        HashMap<String, Object> ar = new HashMap<>();
        Map<String, Object> a = new HashMap();
        a.put("X", null);
        a.put("Y", null);
        a.put("Z", null);
        ar.put("A", a);
        Map<String, Object> res = Call.CallProcedure(con, user, "P1", "P2", ar);
        System.out.println(res);
        Map<String, Object> m = (Map<String, Object>) res.get("B");
        if (!(m.get("X") == null && m.get("Y") == null)) {
            throw new RuntimeException("fail");
        }
    }

    static void test6(OracleConnection con, String user) throws SQLException {
        HashMap<String, Object> ar = new HashMap<>();
        ArrayList l0 = new ArrayList<>();
        for (int j = 0; j < 3; j++) {
            l0.add(null);
        }
        ar.put("A", l0);
        Map<String, Object> res = Call.CallProcedure(con, user, "P1", "P4", ar);
        System.out.println(res);
        ArrayList l2 = (ArrayList) res.get("A");
        for (int i = 0; i < l2.size(); i++) {
            if (l2.get(i) != null) {
                throw new RuntimeException("fail");
            }
        }
    }

    static void perfTest(OracleConnection con, String user, int k) throws SQLException {
        int n = 1;
        DbmsOutput.enableDbmsOutput(con, 0);
        for (int i = 0; i < k; i++) {
            long l = System.currentTimeMillis();
            test3(con, user, n);
            l = System.currentTimeMillis() - l;
            System.out.println("" + n + ": " + l);
            for (String s : DbmsOutput.fetchDbmsOutput(con)) {
                System.out.println("  " + s);
            }

            n = n * 2;
        }
    }

    static void sizeTest(OracleConnection con, String user, int rec_size, int args_size) throws SQLException {
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
        Ddl.call(con, sb.toString());
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
        Ddl.call(con, sb.toString());
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
        Map<String, Object> res = Call.CallProcedure(con, user, "P2", "P", args);
        System.out.println(res);
    }

    static void varcharSizeTest(OracleConnection con, String user, int vsize, int asize) throws SQLException {
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
        Map<String, Object> res = Call.CallProcedure(con, user, "P1", "P5", args);
        ArrayList<String> b = (ArrayList<String>) res.get("B");
        for (int i = 0; i < Math.max(a.size(), b.size()); i++) {
            if (!(a.get(i).equals(b.get(i)))) {
                throw new RuntimeException("fail");
            }
        }
    }

    public static HashMap<String, String> loadSnippets(Class cl, String name)
            throws UnsupportedEncodingException, IOException {
        ClassLoader l = cl.getClassLoader();
        InputStream ins = l.getResourceAsStream(name);
        Reader r = new InputStreamReader(ins, "UTF-8");
        BufferedReader br = new BufferedReader(r);
        HashMap<String, String> res = new HashMap<>();
        String line;
        while (true) {
            line = br.readLine();
            if (line == null || line.startsWith("##")) {
                break;
            }
        }
        while (true) {
            if (line == null) {
                return res;
            }
            String key = line.substring(3);
            key = key.trim();
            StringBuilder sb = new StringBuilder();
            while (true) {
                line = br.readLine();
                if (line == null || line.startsWith("##")) {
                    res.put(key, sb.toString());
                    break;
                } else {
                    sb.append(line);
                    sb.append("\n");
                }
            }
        }
    }

    public static void call(OracleConnection con, String... s) throws SQLException {
        try (Statement stm = con.createStatement()) {
            stm.execute(String.join("\n", s));
        }
    }
}
