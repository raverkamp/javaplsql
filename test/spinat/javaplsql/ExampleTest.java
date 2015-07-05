/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package spinat.javaplsql;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
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

/**
 *
 * @author roland
 */
public class ExampleTest {
    
    public ExampleTest() {
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
    }
    
    @After
    public void tearDown() throws SQLException {
        this.connection.close();
    }

    @Test
    public void example1() throws SQLException {
        Statement stm = connection.createStatement();
        stm.execute("create or replace package example1 as \n"
                +" type rec is record (x number,y varchar2(200),z date);\n"
                +" type array is table of rec;\n"
                + "procedure p(a in array,b out array);\n"
                + "end;");
        stm.execute("create or replace package body example1 as\n"
                + " procedure p(a in array,b out array) is\n"
                + " begin\n"
                + " b:=a;\n"
                + " end;\n"
                +" end;");
        ArrayList<Map<String,Object>> al = new ArrayList<>();
        for(int i=0;i<10;i++) {
            Map<String,Object> m = new HashMap<>();
            m.put("X", i);
            m.put("Y", "text-"+i);
            m.put("Z", new Date()); // simple value?
            al.add(m);
        }
        Map<String,Object> args = new HashMap<>();
        args.put("A", al);
        Map<String,Object> result = new ProcedureCaller(connection).call("example1.p",args);
        for(Object o: (ArrayList<Map<String,Object>>) result.get("B")) {
            System.out.println(o);
        }
        
    }
}
