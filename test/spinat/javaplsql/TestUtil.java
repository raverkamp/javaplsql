package spinat.javaplsql;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Properties;


public class TestUtil {

    public static Properties getProperties(String key) {
        try {
            Class cl = TestUtil.class;
            ClassLoader l = cl.getClassLoader();
            InputStream ins = l.getResourceAsStream(key);
            Properties props = new java.util.Properties();
            props.load(ins);
            return props;
        } catch (IOException ex) {
            throw new RuntimeException("fail", ex);
        }
    }

    public static HashMap<String, String> loadSnippets(String name)
            throws UnsupportedEncodingException, IOException {
        ClassLoader l = (TestUtil.class).getClassLoader();
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

}
