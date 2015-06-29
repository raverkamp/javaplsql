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
        //test1(con);
        test2(con);
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
        Map<String,Object> a = new HashMap();
        a.put("X", 12);
        a.put("Y", "x");
        a.put("Z", new Date());
        ar.put("A",a);
        Map<String, Object> res = Call.CallProcedure(con, "BDMS", "P1", "P2", ar);
        System.out.println(res);
        Map<String,Object> m = (Map<String,Object>)res.get("B");
        if (!(m.get("X").equals(new BigDecimal(13)) && m.get("Y").equals("xx"))) {
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
