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
        OracleConnection con = (OracleConnection) DriverManager.getConnection(props.getProperty("url"),
                props.getProperty("user"), props.getProperty("pw"));

        HashMap<String, String> a = loadSnippets(Main.class, "spinat/javaplsql/snippets.txt");
        Ddl.createType(con, "create type number_array as table of number;");
        Ddl.createType(con, "create type varchar2_array as table of varchar2(32767);");
        Ddl.createType(con, "create type date_array as table of date;");
        Ddl.call(con, a.get("p1_spec"));
        Ddl.call(con, a.get("p1_body"));
        test1(con);
        test2(con);
        test3(con);
        test4(con);
        test5(con);
    }

    static void test1(OracleConnection con) throws SQLException {
        HashMap<String, Object> ar = new HashMap<>();
        ar.put("XI", 12);
        ar.put("YI", "x");
        ar.put("ZI", new Date());
        Map<String, Object> res = Call.CallProcedure(con, "BDMS", "P1", "P", ar);
        System.out.println(res);
        if (!(res.get("XO").equals(new BigDecimal(13)) && res.get("YO").equals("xx"))) {
            throw new RuntimeException("fail");
        }
    }

    static void test2(OracleConnection con) throws SQLException {
        HashMap<String, Object> ar = new HashMap<>();
        Map<String, Object> a = new HashMap();
        a.put("X", 12);
        a.put("Y", "x");
        a.put("Z", new Date());
        ar.put("A", a);
        Map<String, Object> res = Call.CallProcedure(con, "BDMS", "P1", "P2", ar);
        System.out.println(res);
        Map<String, Object> m = (Map<String, Object>) res.get("B");
        if (!(m.get("X").equals(new BigDecimal(13)) && m.get("Y").equals("xx"))) {
            throw new RuntimeException("fail");
        }
    }

    static void test3(OracleConnection con) throws SQLException {
        HashMap<String, Object> ar = new HashMap<>();
        ArrayList<Map<String, Object>> l = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            Map<String, Object> a = new HashMap();
            a.put("X", new BigDecimal(i));
            a.put("Y", "x" + i);
            a.put("Z", new Date());
            l.add(a);
        }
        ar.put("A", l);
        Map<String, Object> res = Call.CallProcedure(con, "BDMS", "P1", "P3", ar);
        System.out.println(res);
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

    static void test4(OracleConnection con) throws SQLException {
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
        Map<String, Object> res = Call.CallProcedure(con, "BDMS", "P1", "P4", ar);
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

    static void test5(OracleConnection con) throws SQLException {
        HashMap<String, Object> ar = new HashMap<>();
        Map<String, Object> a = new HashMap();
        a.put("X", null);
        a.put("Y", null);
        a.put("Z", null);
        ar.put("A", a);
        Map<String, Object> res = Call.CallProcedure(con, "BDMS", "P1", "P2", ar);
        System.out.println(res);
        Map<String, Object> m = (Map<String, Object>) res.get("B");
        if (!(m.get("X") == null && m.get("Y") == null)) {
            throw new RuntimeException("fail");
        }
    }

    public static HashMap<String, String> loadSnippets(Class cl, String name) throws UnsupportedEncodingException, IOException {
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
