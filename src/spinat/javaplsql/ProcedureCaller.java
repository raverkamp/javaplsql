/*

 Copyright (c) 2015, Roland Averkamp, roland.averkamp.0@gmail.com

 Permission to use, copy, modify, and/or distribute this software for any
 purpose with or without fee is hereby granted, provided that the above
 copyright notice and this permission notice appear in all copies.

 THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

 */
package spinat.javaplsql;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import oracle.jdbc.OracleCallableStatement;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.OracleResultSet;
import oracle.jdbc.OracleTypes;

public final class ProcedureCaller {

    static Date stringToDate(String s) {
        if (s == null || s.equals("")) {
            return null;
        }
        if (s.length() <= 10) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            try {
                return sdf.parse(s);
            } catch (ParseException ex) {
                throw new RuntimeException("not in a valid date format:" + s);
            }
        } else {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            try {
                return sdf.parse(s);
            } catch (ParseException ex) {
                throw new RuntimeException("not in a valid date format:" + s);
            }
        }
    }

    static String dateToString(Date d) {
        if (d == null) {
            return null;
        }
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sdf.format(d);
    }

    public static class ConversionException extends RuntimeException {

        public ConversionException(String bla) {
            super(bla);
        }
    }

    public static class Box<X> {

        public X value;

        public Box() {
            this.value = null;
        }

        public Box(X x) {
            this.value = x;
        }
    }

    private final OracleConnection connection;
    private final boolean downCasing;
    private final boolean exportDateAsString;
    private String numberTableName = "NUMBER_ARRAY";
    private String varchar2TableName = "VARCHAR2_ARRAY";
    private String dateTableName = "DATE_ARRAY";
    private String rawTableName = "RAW_ARRAY";

    // unfortunately the JDBC retrival of Array Descriptors does not care about
    // set current_schema = 
    // therefore we resolve the name and store schema.name in these fields
    private String effectiveNumberTableName = null;
    private String effectiveVarchar2TableName = null;
    private String effectiveDateTableName = null;
    private String effectiveRawTableName = null;

    public ProcedureCaller(OracleConnection connection) {
        this.connection = connection;
        this.downCasing = false;
        this.exportDateAsString = false;
    }

    public ProcedureCaller(OracleConnection connection, boolean downCasing, boolean exportDateAsString) {
        this.connection = connection;
        this.downCasing = downCasing;
        this.exportDateAsString = exportDateAsString;
    }

    /**
     * @return the numberTableName
     */
    public String getNumberTableName() {
        return numberTableName;
    }

    /**
     * @param numberTableName the numberTableName to set
     */
    public void setNumberTableName(String numberTableName) {
        this.numberTableName = numberTableName;
        this.effectiveNumberTableName = null;
    }

    /**
     * @return the varchar2TableName
     */
    public String getVarchar2TableName() {
        return varchar2TableName;
    }

    /**
     * @param varchar2TableName the varchar2TableName to set
     */
    public void setVarchar2TableName(String varchar2TableName) {
        this.varchar2TableName = varchar2TableName;
        this.effectiveVarchar2TableName = null;
    }

    /**
     * @return the dateTableName
     */
    public String getDateTableName() {
        return dateTableName;
    }

    /**
     * @param dateTableName the dateTableName to set
     */
    public void setDateTableName(String dateTableName) {
        this.dateTableName = dateTableName;
        this.effectiveDateTableName = null;
    }

    /**
     * @return the dateTableName
     */
    public String getRawTableName() {
        return rawTableName;
    }

    /**
     * @param dateTableName the dateTableName to set
     */
    public void setRawTableName(String dateTableName) {
        this.rawTableName = dateTableName;
        this.effectiveRawTableName = null;
    }

    /*
     DBMS_UTILITY.NAME_RESOLVE (
     name          IN  VARCHAR2, 
     context       IN  NUMBER,
     schema        OUT VARCHAR2, 
     part1         OUT VARCHAR2, 
     part2         OUT VARCHAR2,
     dblink        OUT VARCHAR2, 
     part1_type    OUT NUMBER, 
     object_number OUT NUMBER);
    
     5 - synonym
     7 - procedure (top level)
     8 - function (top level)
     9 - package
     */
    private static class ResolvedName {

        public final String schema;
        public final String part1;
        public final String part2;
        public final String dblink;
        public final int part1_type;
        public final BigInteger object_number;

        @Override
        public String toString() {
            return "" + schema + ", " + part1 + ", " + part2 + ", " + dblink + ", " + part1_type + ", " + object_number;
        }

        public ResolvedName(String schema,
                String part1,
                String part2,
                String dblink,
                int part1_type,
                BigInteger object_number) {
            this.schema = schema;
            this.part1 = part1;
            this.part2 = part2;
            this.dblink = dblink;
            this.part1_type = part1_type;
            this.object_number = object_number;
        }
    }

    private static ResolvedName resolveName(OracleConnection con, String name, boolean typeContext) throws SQLException {
        try (CallableStatement cstm = con.prepareCall(
                "begin dbms_utility.name_resolve(?,?,?,?,?,?,?,?);end;")) {
            cstm.setString(1, name);
            if (typeContext) {
                cstm.setInt(2, 7);
            } else {
                cstm.setInt(2, 1); // is context PL/SQL code 
            }
            cstm.registerOutParameter(3, Types.VARCHAR);
            cstm.registerOutParameter(4, Types.VARCHAR);
            cstm.registerOutParameter(5, Types.VARCHAR);
            cstm.registerOutParameter(6, Types.VARCHAR);
            cstm.registerOutParameter(7, Types.INTEGER);
            cstm.registerOutParameter(8, Types.NUMERIC);
            cstm.execute();
            String schema = cstm.getString(3);
            String part1 = cstm.getString(4);
            String part2 = cstm.getString(5);
            String dblink = cstm.getString(6);
            int part1_type = cstm.getInt(7);
            if (cstm.wasNull()) {
                part1_type = 0;
            }
            BigDecimal object_number = cstm.getBigDecimal(8);
            return new ResolvedName(schema, part1, part2, dblink, part1_type, object_number.toBigIntegerExact());
        }
    }

    private String computeEffectiveName(String name) throws SQLException {
        if (name.contains(".")) {
            return name;
        }
        ResolvedName rn = resolveName(connection, name, true);
        return rn.schema + "." + name;
    }

    // determine the type of an index by table
    // it is not possible to do this by looking at the data dictionary
    // so we execute a small block to determine this
    // returns V for index by varchar2 and I for index by binary_integer
    // this is a dirty trick 
    private char isIndexByVarcharOrInt(String owner, String package_, String type) {
        try (CallableStatement s = this.connection.prepareCall(
                "declare a " + owner + "." + package_ + "." + type + ";\n"
                + " x varchar2(1):='y';\n"
                + "begin\n"
                + "begin\n"
                + "if a.exists('akl') then\n"
                // there has to something here otherwise oracle will optimize
                // to much, just the statement null; will not work!
                + "  x:='z';\n"
                + "end if;\n"
                + "x:='X';\n"
                + "exception when others then\n"
                + " x:= null;\n"
                + " end;\n"
                + "?:=x;\n"
                + "end;")) {
            s.registerOutParameter(1, Types.VARCHAR);
            s.execute();
            String x = s.getString(1);
            return null == x ? 'I' : 'V';
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    // represents types from PL/SQL
    private static abstract class Type {

        public abstract String plsqlName();

        // the data is transfered to the database in three tables one for numbers
        // one for strings and one for dates. The ArgArrays contains this data.
        // this method copies the data for Object o into ArgArrays a based on
        // the type
        public abstract void fillArgArrays(ArgArrays a, Object o);

        // this is the inverse process . The result data is returned from 
        // the database in three tables. Reconstruct the objects from the 
        // data in the arrays based on the type
        public abstract Object readFromResArrays(ResArrays a);

        // generate the PL/SQL code to read the data for the arguments from the 
        // three PL/SQL arrays
        // when reading out tables we need index variables, to keep the index variables
        // distinct each gets its own number, we track that with AtomicInteger
        // there is no concurrency involved we just need a box with an integer
        // the generated pl/SQL block should only depend should depend deterministically
        // on the procedure arguments. We do not want to blow up the statement cache
        public abstract void genReadOutThing(StringBuilder sb, AtomicInteger counter, String target);

        // generate the PL/SQL code to write the data from the OUT and IN/OUT 
        // and the return value to the three arrays
        // the reason for the AtomicInteger is the same as above
        public abstract void genWriteThing(StringBuilder sb, AtomicInteger counter, String source);

    }

    // the PL/SQL standrad types, identfied by their name DATE, NUMBER
    private static class NamedType extends Type {

        final String name; // number, integer ...
        final boolean exportDateAsString;

        public NamedType(String name, boolean exportDateAsString) {
            this.name = name;
            this.exportDateAsString = exportDateAsString;
        }

        @Override
        public String plsqlName() {
            if (this.name.equals("PL/SQL BOOLEAN")) {
                return "boolean";
            } else {
                return this.name;
            }
        }

        @Override
        public void fillArgArrays(ArgArrays a, Object o) {
            if (this.name.equals("NUMBER")
                    || this.name.equals("INTEGER")
                    || this.name.equals("BINARY_INTEGER")) {
                a.addNumber((Number) o);
            } else if (this.name.equals("PL/SQL BOOLEAN")) {
                if (o == null) {
                    a.addNumber(null);
                } else {
                    boolean b = (Boolean) o;
                    a.addNumber(b ? 1 : 0);
                }

            } else if (this.name.equals("DATE")) {
                java.util.Date d;
                if (o != null && o instanceof String) {
                    d = stringToDate((String) o);
                } else {
                    d = (java.util.Date) o;
                }
                a.addDate(d);
            } else {
                throw new RuntimeException("unsupported named type");
            }
        }

        @Override
        public Object readFromResArrays(ResArrays a) {
            if (this.name.equals("NUMBER")
                    || this.name.equals("INTEGER")
                    || this.name.equals("BINARY_INTEGER")) {
                return a.readBigDecimal();
            } else if (this.name.equals("DATE")) {
                Date d = a.readDate();
                if (this.exportDateAsString) {
                    return dateToString(d);
                } else {
                    return d;
                }
            } else if (this.name.equals("PL/SQL BOOLEAN")) {
                BigDecimal x = a.readBigDecimal();
                if (x == null) {
                    return null;
                } else {
                    return x.equals(BigDecimal.ONE);
                }
            } else {
                throw new RuntimeException("unsupported named type: " + this.name);
            }
        }

        @Override
        public void genWriteThing(StringBuilder sb, AtomicInteger counter, String source) {
            if (this.name.equals("NUMBER")
                    || this.name.equals("INTEGER")
                    || this.name.equals("BINARY_INTEGER")) {
                sb.append("an.extend; an(an.last):= " + source + ";\n");
            } else if (this.name.equals("DATE")) {
                sb.append("ad.extend; ad(ad.last):= " + source + ";\n");
            } else if (this.name.equals("PL/SQL BOOLEAN")) {
                sb.append("an.extend; an(an.last):= case when " + source + " then 1 when not " + source + " then 0 else null end;\n");
            } else {
                throw new RuntimeException("unsupported base type");
            }
        }

        @Override
        public void genReadOutThing(StringBuilder sb, AtomicInteger counter, String target) {
            if (this.name.equals("NUMBER")
                    || this.name.equals("INTEGER")
                    || this.name.equals("BINARY_INTEGER")) {
                sb.append(target).append(":= an(inn);inn:=inn+1;\n");
            } else if (this.name.equals("DATE")) {
                sb.append(target).append(":= ad(ind); ind := ind+1;\n");
            } else if (this.name.equals("PL/SQL BOOLEAN")) {
                sb.append(target).append(":= an(inn)=1; inn := inn+1;\n");
            } else {
                throw new RuntimeException("unsupported base type");
            }
        }
    }

    private static class Varchar2Type extends Type {

        String name; // varchar2, number, integer ...
        int size; // 0 if unbounded, i.e. as direct parameter

        @Override
        public String plsqlName() {
            if (size == 0) {
                return "varchar2(32767)";
            } else {
                return "varchar2(" + size + ")";
            }
        }

        @Override
        public void fillArgArrays(ArgArrays a, Object o) {
            String s = (String) o;
            if (s == null) {
                a.addString(s);
            } else {
                int allowed_size = this.size == 0 ? 32767 : this.size;
                if (s.length() <= allowed_size) {
                    a.addString(s);
                } else {
                    throw new ConversionException("string is to large, allowed are "
                            + allowed_size + ", given length " + s.length());
                }
            }
        }

        @Override
        public Object readFromResArrays(ResArrays a) {
            return a.readString();
        }

        @Override
        public void genWriteThing(StringBuilder sb, AtomicInteger counter, String source) {
            sb.append("av.extend; av(av.last) := " + source + ";\n");
        }

        @Override
        public void genReadOutThing(StringBuilder sb, AtomicInteger counter, String target) {
            sb.append(target).append(":= av(inv); inv := inv+1;\n");
        }
    }

    private static class RawType extends Type {

        String name; // varchar2, number, integer ...
        int size; // 0 if unbounded, i.e. as direct parameter

        @Override
        public String plsqlName() {
            if (size == 0) {
                return "raw(32767)";
            } else {
                return "raw(" + size + ")";
            }
        }

        @Override
        public void fillArgArrays(ArgArrays a, Object o) {
            byte[] b = (byte[]) o;
            if (b == null) {
                a.addRaw(b);
            } else {
                int allowed_size = this.size == 0 ? 32767 : this.size;
                if (b.length <= allowed_size) {
                    a.addRaw(b);
                } else {
                    throw new ConversionException("raw/byte[] is to large, allowed are "
                            + allowed_size + ", given length " + b.length);
                }
            }
        }

        @Override
        public Object readFromResArrays(ResArrays a) {
            return a.readRaw();
        }

        @Override
        public void genWriteThing(StringBuilder sb, AtomicInteger counter, String source) {
            sb.append("ar.extend; ar(ar.last) := " + source + ";\n");
        }

        @Override
        public void genReadOutThing(StringBuilder sb, AtomicInteger counter, String target) {
            sb.append(target).append(":= ar(inr); inr := inr+1;\n");
        }
    }

    private static class Field {

        String name;
        Type type;
    }

    private static class RecordType extends Type {

        String owner;
        String package_;
        String name;
        boolean downCasing = false;
        ArrayList<Field> fields;

        public boolean isAnonymous() {
            return this.owner == null;
        }

        @Override
        public String plsqlName() {
            return this.owner + "." + this.package_ + "." + this.name;
        }

        @Override
        public void fillArgArrays(ArgArrays a, Object o) {
            if (o instanceof Map) {
                Map m = (Map) o;
                for (Field f : this.fields) {
                    Object x;
                    String fname = this.downCasing ? f.name.toLowerCase() : f.name;
                    if (m.containsKey(fname)) {
                        x = m.get(fname);
                        f.type.fillArgArrays(a, x);
                    } else {
                        throw new ConversionException("slot not found: " + f.name);
                    }
                }
            }
        }

        @Override
        public Object readFromResArrays(ResArrays a) {
            HashMap<String, Object> m = new HashMap<>();
            for (Field f : this.fields) {
                Object o = f.type.readFromResArrays(a);
                String fname = this.downCasing ? f.name.toLowerCase() : f.name;
                m.put(fname, o);
            }
            return m;
        }

        @Override
        public void genWriteThing(StringBuilder sb, AtomicInteger counter, String source) {
            for (Field f : this.fields) {
                String a = source + "." + f.name;
                f.type.genWriteThing(sb, counter, a);
            }
        }

        @Override
        public void genReadOutThing(StringBuilder sb, AtomicInteger counter, String target) {
            for (Field f : this.fields) {
                String a = target + "." + f.name;
                f.type.genReadOutThing(sb, counter, a);
            }
        }
    }

    // type callProcedure is table of blub;
    // no indexed by
    private static class TableType extends Type {

        String owner;
        String package_;
        String name;
        Type slottype;

        @Override
        public String plsqlName() {
            return this.owner + "." + this.package_ + "." + this.name;
        }

        @Override
        public void fillArgArrays(ArgArrays a, Object o) {
            if (o == null) {
                a.addNumber((BigDecimal) null);
            } else {
                ArrayList l = (ArrayList) o;
                a.addNumber(l.size());
                for (Object x : l) {
                    this.slottype.fillArgArrays(a, x);
                }
            }
        }

        @Override
        public Object readFromResArrays(ResArrays a) {
            BigDecimal b = a.readBigDecimal();
            if (b == null) {
                return null;
            } else {
                int size = b.intValue();
                ArrayList<Object> res = new ArrayList<>();
                for (int i = 0; i < size; i++) {
                    res.add(this.slottype.readFromResArrays(a));
                }
                return res;
            }
        }

        @Override
        public void genWriteThing(StringBuilder sb, AtomicInteger counter, String source) {
            sb.append("an.extend;\n");
            sb.append(" if " + source + " is null then\n");
            sb.append("    an(an.last) := null;\n");
            sb.append("else \n");
            sb.append("  an(an.last) := nvl(" + source + ".last, 0);\n");
            String index = "i" + counter.incrementAndGet();
            sb.append("for " + index + " in 1 .. nvl(" + source + ".last,0) loop\n");
            this.slottype.genWriteThing(sb, counter, source + "(" + index + ")");
            sb.append("end loop;\n");
            sb.append("end if;\n");
        }

        @Override
        public void genReadOutThing(StringBuilder sb, AtomicInteger counter, String target) {
            sb.append("size_ := an(inn); inn:= inn+1;\n");
            sb.append("if size_ is null then\n");
            sb.append("  ").append(target).append(":=null;\n");
            sb.append("else\n");
            sb.append(" ").append(target).append(" := new ")
                    .append(this.package_).append(".").append(this.name).append("();\n");
            String index = "i" + counter.incrementAndGet();
            String newTarget = target + "(" + index + ")";
            sb.append("  for ").append(index).append(" in 1 .. size_ loop\n");
            sb.append("" + target + ".extend();\n");
            this.slottype.genReadOutThing(sb, counter, newTarget);
            sb.append("end loop;\n");
            sb.append("end if;\n");
        }
    }

    private static class IndexByStringTableType extends Type {

        String owner;
        String package_;
        String name;
        Type slottype;

        @Override
        public String plsqlName() {
            return this.owner + "." + this.package_ + "." + this.name;
        }

        @Override
        public void fillArgArrays(ArgArrays a, Object o) {
            if (o == null) {
                a.addNumber((BigDecimal) null);
            } else {
                //JSONObject is a Map!
                Map tm = (Map) o;
                a.addNumber(tm.size());
                for (Object entry : tm.entrySet()) {
                    @SuppressWarnings("unchecked")
                    Map.Entry<String, Object> kv = (Map.Entry<String, Object>) entry;
                    a.addString(kv.getKey());
                    this.slottype.fillArgArrays(a, kv.getValue());
                }
            }
        }

        @Override
        public Object readFromResArrays(ResArrays a) {
            BigDecimal b = a.readBigDecimal();
            if (b == null) {
                return null;
            } else {
                int size = b.intValue();
                TreeMap<String, Object> res = new TreeMap<>();
                for (int i = 0; i < size; i++) {
                    String k = a.readString();
                    res.put(k, this.slottype.readFromResArrays(a));
                }
                return res;
            }
        }

        @Override
        public void genWriteThing(StringBuilder sb, AtomicInteger counter, String source) {
            sb.append("  an.extend;\n");
            sb.append("  an(an.last) := " + source + ".count;\n");
            String index = "i" + counter.incrementAndGet();
            sb.append("declare " + index + " varchar2(32000) := " + source + ".first;\n");
            sb.append("begin\n");
            sb.append(" loop\n");
            sb.append("exit when " + index + " is null;\n");
            sb.append("  av.extend; av(av.last) := " + index + ";\n");
            this.slottype.genWriteThing(sb, counter, source + "(" + index + ")");
            sb.append(" " + index + " := " + source + ".next(" + index + ");\n");
            sb.append("end loop;\n");
            sb.append("end;\n");
        }

        @Override
        public void genReadOutThing(StringBuilder sb, AtomicInteger counter, String target) {
            sb.append("size_ := an(inn); inn:= inn+1;\n");
            sb.append("if size_ is null then\n");
            sb.append(" null;\n");
            sb.append("else\n");
            String index = "i" + counter.incrementAndGet();
            String key = "k" + counter.incrementAndGet();
            String newTarget = target + "(" + key + ")";
            sb.append("declare " + key + " varchar2(32000);\n");
            sb.append("begin\n");
            sb.append("  for ").append(index).append(" in 1 .. size_ loop\n");
            sb.append("  " + key + " :=av(inv); inv := inv+1;\n");
            this.slottype.genReadOutThing(sb, counter, newTarget);
            sb.append("end loop;\n");
            sb.append("end;\n");
            sb.append("end if;\n");
        }
    }

    private static class IndexByIntegerTableType extends Type {

        String owner;
        String package_;
        String name;
        Type slottype;

        @Override
        public String plsqlName() {
            return this.owner + "." + this.package_ + "." + this.name;
        }

        @Override
        public void fillArgArrays(ArgArrays a, Object o) {
            if (o == null) {
                a.addNumber((BigDecimal) null);
            } else {
                Map tm = (Map) o;
                a.addNumber(tm.size());
                for (Object entry : tm.entrySet()) {
                    Map.Entry kv = (Map.Entry) entry;
                    Object key = kv.getKey();
                    Object val = kv.getValue();
                    if (key == null) {
                        throw new NullPointerException("key in integer array is null");
                    }
                    if (key instanceof String) {
                        Integer x = Integer.parseInt((String) key);
                        a.addNumber(x);
                    } else if (key instanceof Integer) {
                        a.addNumber((Integer) key);
                    } else {
                        throw new RuntimeException("expecting an integer as key");
                    }
                    this.slottype.fillArgArrays(a, kv.getValue());
                }
            }
        }

        @Override
        public Object readFromResArrays(ResArrays a) {
            BigDecimal b = a.readBigDecimal();
            if (b == null) {
                return null;
            } else {
                int size = b.intValue();
                TreeMap<Integer, Object> res = new TreeMap<>();
                for (int i = 0; i < size; i++) {
                    Integer k = a.readBigDecimal().intValueExact();
                    res.put(k, this.slottype.readFromResArrays(a));
                }
                return res;
            }
        }

        @Override
        public void genWriteThing(StringBuilder sb, AtomicInteger counter, String source) {
            sb.append("  an.extend;\n");
            sb.append("  an(an.last) := " + source + ".count;\n");

            String index = "i" + counter.incrementAndGet();
            sb.append("declare " + index + " integer := " + source + ".first;\n");
            sb.append("begin\n");
            sb.append(" loop\n");
            sb.append("exit when " + index + " is null;\n");
            sb.append("  an.extend; an(an.last) := " + index + ";\n");
            this.slottype.genWriteThing(sb, counter, source + "(" + index + ")");
            sb.append(" " + index + " := " + source + ".next(" + index + ");\n");
            sb.append("end loop;\n");
            sb.append("end;\n");
        }

        @Override
        public void genReadOutThing(StringBuilder sb, AtomicInteger counter, String target) {
            sb.append("size_ := an(inn); inn:= inn+1;\n");
            sb.append("if size_ is null then\n");
            sb.append(" null;\n");
            sb.append("else\n");
            String index = "i" + counter.incrementAndGet();
            String key = "k" + counter.incrementAndGet();
            String newTarget = target + "(" + key + ")";
            sb.append("declare " + key + " integer;\n");
            sb.append("begin\n");
            sb.append("  for ").append(index).append(" in 1 .. size_ loop\n");
            sb.append("  " + key + " :=an(inn); inn := inn+1;\n");
            this.slottype.genReadOutThing(sb, counter, newTarget);
            sb.append("end loop;\n");
            sb.append("end;\n");
            sb.append("end if;\n");
        }
    }

    private static class SysRefCursorType extends Type {

        // tricky : unlike for tables we do not know the size in advance
        // and we do not know the columns
        // thus when retrieving write the columns (name,type)
        //   and then write the rows
        // for a row write 1 into number and then the row data
        // if there is no more data then write 0 into number
        // support for clobs, cursor in result set?
        // maybe a procedure whcih reads out the data and returns 
        // a handle and an array (or just a string?) with the columns types for readput
        final boolean exportDateAsString;
        final boolean downCasing;

        public SysRefCursorType(boolean exportDateAsString, boolean downCasing) {
            this.exportDateAsString = exportDateAsString;
            this.downCasing = downCasing;
        }

        @Override
        public String plsqlName() {
            return "sys_refcursor";
        }

        @Override
        public void fillArgArrays(ArgArrays a, Object o) {
            throw new Error("sys_refcursor may not be an \"IN\" or \"IN OUT\" parameter");
        }

        @Override
        public Object readFromResArrays(ResArrays a) {
            int colcount = a.readBigDecimal().intValue();
            ArrayList<String> colnames = new ArrayList<>();
            ArrayList<String> coltypes = new ArrayList<>();
            for (int i = 0; i < colcount; i++) {
                String colname = a.readString();
                if (this.downCasing) {
                    colname = colname.toLowerCase();
                }
                colnames.add(colname);
                coltypes.add(a.readString());
            }
            ArrayList<HashMap<String, Object>> l = new ArrayList<>();
            while (true) {
                if (a.readBigDecimal().intValue() == 0) {
                    break;
                }
                HashMap<String, Object> m = new HashMap<>();
                for (int i = 0; i < colcount; i++) {
                    String t = coltypes.get(i);
                    final Object o;
                    if (t.equals("N")) {
                        o = a.readBigDecimal();
                    } else if (t.equals("V")) {
                        o = a.readString();
                    } else if (t.equals("D")) {
                        final Date d = a.readDate();
                        if (this.exportDateAsString) {
                            o = dateToString(d);
                        } else {
                            o = d;
                        }
                    } else {
                        throw new Error("unknwon column type: " + t);
                    }
                    m.put(colnames.get(i), o);
                }
                l.add(m);
            }
            return l;
        }

        @Override
        public void genReadOutThing(StringBuilder sb, AtomicInteger counter, String target) {
            throw new Error("sys_refcursor may not be an \"IN\" or \"IN OUT\" parameter");
        }

        @Override
        public void genWriteThing(StringBuilder sb, AtomicInteger counter, String source) {
            sb.append("declare h integer;\n");
            sb.append(" t varchar2(100);\n");
            sb.append(" rec_tab   DBMS_SQL.DESC_TAB;\n");
            sb.append(" rec       DBMS_SQL.DESC_REC;\n");
            sb.append(" x number;\n");
            sb.append("num number;\n");
            sb.append("dat date;\n");
            sb.append("varc varchar2(4000);\n");
            sb.append(" col_cnt integer;\n");
            sb.append("begin\n");
            sb.append(" h := DBMS_SQL.TO_CURSOR_NUMBER (" + source + ");\n");
            sb.append(" DBMS_SQL.DESCRIBE_COLUMNS(h, col_cnt, rec_tab);\n");
            sb.append(" an.extend; an(an.last):= col_cnt;\n");
            sb.append(" for i in 1 .. rec_tab.last loop\n");
            sb.append("  rec := rec_tab(i);\n");
            sb.append("  av.extend; av(av.last):= rec.col_name;\n");
            sb.append("if rec.col_type = dbms_types.TYPECODE_DATE then\n");
            sb.append("        dbms_sql.define_column(h, i, dat);\n");
            sb.append(" t:=t||'D';\n");
            sb.append("elsif rec.col_type = dbms_types.TYPECODE_NUMBER then\n");
            sb.append("        dbms_sql.define_column(h, i, num);\n");
            sb.append(" t:=t||'N';\n");
            sb.append("else\n");
            sb.append("        dbms_sql.define_column(h, i, varc, 4000);\n");
            sb.append(" t:=t||'V';\n");
            sb.append("end if;");
            sb.append("av.extend;av(av.last):=substr(t,i,1);\n");
            sb.append(" end loop;\n");
            sb.append(" loop\n");
            sb.append("      x := DBMS_SQL.FETCH_ROWS(h);\n");
            sb.append("      exit when x = 0;\n");
            sb.append("      an.extend; an(an.last):= 1\n;");
            sb.append("      for i in 1 .. col_cnt loop\n");
            sb.append("        case substr(t,i,1) \n");
            sb.append("         when 'D' then\n");
            sb.append("          DBMS_SQL.COLUMN_VALUE(h, i, dat);\n");
            sb.append("          ad.extend; ad(ad.last) := dat;\n");
            sb.append("        when 'N' then\n");
            sb.append("          DBMS_SQL.COLUMN_VALUE(h, i, num);\n");
            sb.append("          an.extend; an(an.last) := num;\n");
            sb.append("        when 'V' then\n");
            sb.append("          DBMS_SQL.COLUMN_VALUE(h, i, varc);\n");
            sb.append("          av.extend; av(av.last) := varc;\n");
            sb.append("         else raise_application_error(-20000,'BUG: unknown internal type code: '||t);\n");
            sb.append("         end case;\n");
            sb.append("      end loop;\n");
            sb.append("    end loop;\n");
            sb.append("      an.extend; an(an.last):= 0\n;");
            sb.append("end;");

        }
    }

    private static ArrayList<Map<String, Object>> readSysRefCursor(boolean dateAsString, boolean downcasing, ResultSet rs) throws SQLException {

        ResultSetMetaData md = rs.getMetaData();
        ArrayList<String> fields = new ArrayList<>();
        for (int i = 0; i < md.getColumnCount(); i++) {
            if (downcasing) {
                fields.add(md.getColumnName(i + 1).toLowerCase());
            } else {
                fields.add(md.getColumnName(i + 1));
            }
        }
        ArrayList<Map<String, Object>> res = new ArrayList<>();
        while (rs.next()) {
            Map<String, Object> r = new HashMap<String, Object>();
            for (int i = 0; i < md.getColumnCount(); i++) {
                int ct = md.getColumnType(i + 1);
                if (ct == Types.VARCHAR) {
                    r.put(fields.get(i), rs.getString(i + 1));
                } else if (ct == Types.BIGINT || ct == Types.DECIMAL || ct == Types.NUMERIC || ct == Types.INTEGER) {
                    r.put(fields.get(i), rs.getBigDecimal(i + 1));
                } else if (ct == OracleTypes.CURSOR) {
                    try (ResultSet rs2 = ((OracleResultSet) rs).getCursor(i + 1)) {
                        r.put(fields.get(i), readSysRefCursor(dateAsString, downcasing, rs2));
                    }
                } else if (ct == Types.DATE || ct == Types.TIMESTAMP) {
                    Timestamp ts = rs.getTimestamp(i + 1);
                    java.util.Date d = new java.util.Date(ts.getTime());
                    if (dateAsString) {
                        r.put(fields.get(i), dateToString(d));
                    } else {
                        r.put(fields.get(i), d);
                    }
                } else if (ct == Types.VARBINARY) {
                    r.put(fields.get(i), rs.getBytes(i + 1));
                } else {
                    throw new RuntimeException("type not supported: " + ct + " for column " + fields.get(i) + ", typename=" + md.getColumnTypeName(i + 1));
                }
            }
            res.add(r);
        }
        return res;
    }

    private static class TypedRefCursorType extends Type {

        String owner;
        String package_;
        String name;
        RecordType rectype;

        @Override
        public String plsqlName() {
            return "sys_refcursor";
        }

        @Override
        public void fillArgArrays(ArgArrays a, Object o) {
            throw new Error("ref cursor may not be an \"IN\" or \"IN OUT\" parameter");
        }

        @Override
        public Object readFromResArrays(ResArrays a) {
            ArrayList<HashMap<String, Object>> l = new ArrayList<>();
            while (true) {
                if (a.readBigDecimal().intValue() == 0) {
                    break;
                }
                @SuppressWarnings("unchecked")
                HashMap<String, Object> m = (HashMap<String, Object>) rectype.readFromResArrays(a);
                l.add(m);
            }
            return l;
        }

        @Override
        public void genReadOutThing(StringBuilder sb, AtomicInteger counter, String target) {
            throw new Error("ref cursor may not be an \"IN\" or \"IN OUT\" parameter");
        }

        @Override
        public void genWriteThing(StringBuilder sb, AtomicInteger counter, String source) {
            sb.append("declare r " + rectype.plsqlName() + ";\n");
            sb.append("x integer;\n");
            sb.append("begin\n");
            sb.append("loop\n");
            sb.append(" fetch " + source + " into r;\n");
            sb.append("if " + source + "%notfound then\n");
            sb.append("  exit;\n");
            sb.append("end if;\n");
            sb.append("an.extend;an(an.last) := 1;\n");
            rectype.genWriteThing(sb, counter, "r");
            sb.append("end loop;\n");
            sb.append("an.extend;an(an.last) := 0;\n");
            sb.append("end;\n");
        }
    }

    // the arguments to a procedure/function
    private static class Argument {

        public String name;
        public String direction;
        public Type type;
    }

    // represents one procedure/function
    private static class Procedure {

        // not null if function
        Type returnType;
        String original_name;
        String owner;
        String package_; // could be null
        String name;
        int overload;
        ArrayList<Argument> arguments;
        // used to store the generated pl/sql block
        String plsqlstatement = null;
    }

    // this class corresponds 1:1 the columns in all_arguments, some columns are lft out
    // when working with the data in all_arguments it is transformed into an ArrayList
    // of this ArgumentsRow
    private static class ArgumentsRow {

        String owner;
        String object_name;
        String package_name;
        String argument_name;
        int position;
        int sequence;
        int data_level;
        String data_type;
        int overload;
        String in_out;
        String type_owner;
        String type_name;
        String type_subname;
        String pls_type;
        int data_length;
    }

    private static ArrayDeque<ArgumentsRow> fetchArgumentsRows(ResultSet rs) throws SQLException {
        ArrayDeque<ArgumentsRow> res = new ArrayDeque<>();
        while (rs.next()) {
            ArgumentsRow r = new ArgumentsRow();
            r.owner = rs.getString("OWNER");
            r.object_name = rs.getString("OBJECT_NAME");
            r.package_name = rs.getString("PACKAGE_NAME");
            r.argument_name = rs.getString("ARGUMENT_NAME");
            r.position = rs.getInt("POSITION");
            r.sequence = rs.getInt("SEQUENCE");
            r.data_level = rs.getInt("DATA_LEVEL");
            r.data_type = rs.getString("DATA_TYPE");
            r.overload = rs.getInt("OVERLOAD");
            if (rs.wasNull()) {
                r.overload = -1; //just not null
            }
            r.in_out = rs.getString("IN_OUT");
            r.type_owner = rs.getString("TYPE_OWNER");
            r.type_name = rs.getString("TYPE_NAME");
            r.type_subname = rs.getString("TYPE_SUBNAME");
            r.pls_type = rs.getString("PLS_TYPE");
            r.data_length = rs.getInt("DATA_LENGTH");
            if (rs.wasNull()) {
                r.data_length = 0;
            }
            res.add(r);
        }
        return res;
    }

    // get a Field from Args a and advance the internal position to the position
    // after this Field.
    // due to the recursive structure of PL/SQL types this method is recursive
    private Field eatArg(ArrayDeque<ArgumentsRow> a) {
        ArgumentsRow r = a.getFirst();
        Field f = new Field();
        f.name = r.argument_name;
        if (r.data_type.equals("NUMBER")
                || r.data_type.equals("DATE")
                || r.data_type.equals("INTEGER")
                || r.data_type.equals("PL/SQL BOOLEAN")
                || r.data_type.equals("BINARY_INTEGER")) {
            NamedType t = new NamedType(r.data_type, this.exportDateAsString);
            f.type = t;
            a.pop();
            return f;
        }
        if (r.data_type.equals("VARCHAR2")) {
            Varchar2Type vt = new Varchar2Type();
            vt.name = "VARCHAR2";
            vt.size = r.data_length;
            f.type = vt;
            a.pop();
            return f;
        }
        if (r.data_type.equals("RAW")) {
            RawType vt = new RawType();
            vt.name = "RAW";
            vt.size = r.data_length;
            f.type = vt;
            a.pop();
            return f;
        }
        if (r.data_type.equals("TABLE")) {
            TableType t = new TableType();
            t.owner = r.type_owner;
            t.package_ = r.type_name;
            t.name = r.type_subname;
            a.pop();
            Field f2 = eatArg(a);
            t.slottype = f2.type;
            f.type = t;
            return f;
        }
        if (r.data_type.equals("PL/SQL RECORD")) {
            RecordType t = new RecordType();
            t.downCasing = this.downCasing;
            t.owner = r.type_owner;
            t.package_ = r.type_name;
            t.name = r.type_subname;
            t.fields = new ArrayList<>();
            int level = r.data_level;
            a.pop();
            while (!a.isEmpty() && a.getFirst().data_level > level) {
                t.fields.add(eatArg(a));
            }
            f.type = t;
            if (t.isAnonymous()) {
                throw new RuntimeException("anonymous record types (%rowtype) are not supported");
            }
            return f;
        }
        // as of Oracle 11.2.0.2.0 ref cursors can not be part of records or be
        // part of a table
        if (r.data_type.equals("REF CURSOR")) {
            a.pop();
            if (a.isEmpty() || a.getFirst().data_level == 0) {
                SysRefCursorType t = new SysRefCursorType(this.exportDateAsString, this.downCasing);
                f.type = t;
                return f;
            }
            TypedRefCursorType t = new TypedRefCursorType();
            t.owner = r.type_owner;
            t.package_ = r.type_name;
            t.name = r.type_subname;
            Field f2 = eatArg(a);
            if (f2.type instanceof RecordType) {
                t.rectype = (RecordType) f2.type;
                if (t.rectype.isAnonymous()) {
                    throw new RuntimeException("anonymous record types (%rowtype) are not supported for typed ref cursors");
                }
                f.type = t;
                return f;
            } else {
                throw new RuntimeException("unknown record type for cursor");
            }
        }
        if (r.data_type.equals("PL/SQL TABLE")) {
            char tt = isIndexByVarcharOrInt(r.type_owner, r.type_name, r.type_subname);
            if (tt == 'V') {
                IndexByStringTableType t = new IndexByStringTableType();
                t.owner = r.type_owner;
                t.package_ = r.type_name;
                t.name = r.type_subname;
                a.pop();
                Field f2 = eatArg(a);
                t.slottype = f2.type;
                f.type = t;
                return f;
            } else if (tt == 'I') {
                IndexByIntegerTableType t = new IndexByIntegerTableType();
                t.owner = r.type_owner;
                t.package_ = r.type_name;
                t.name = r.type_subname;
                a.pop();
                Field f2 = eatArg(a);
                t.slottype = f2.type;
                f.type = t;
                return f;
            } else {
                throw new Error("BUG");
            }
        }
        throw new RuntimeException("unsupported type: " + r.data_type);
    }

    private Procedure eatProc(ArrayDeque<ArgumentsRow> a) {
        Procedure p = new Procedure();
        ArgumentsRow r = a.getFirst();
        p.package_ = r.package_name;
        p.name = r.object_name;
        p.overload = r.overload;
        p.owner = r.owner;
        p.arguments = new ArrayList<>();
        if (a.getFirst().data_type == null) {
            // this is a procedure with no arguments
            a.pop();
            return p;
        }
        if (r.position == 0) {
            // this a function the return type is the first argument, the 
            // argument name is null
            Field f = eatArg(a);
            p.returnType = f.type;
        }
        while (!a.isEmpty() && a.getFirst().overload == p.overload) {
            String io = a.getFirst().in_out;
            Field f = eatArg(a);
            Argument ar = new Argument();
            ar.direction = io;
            ar.name = f.name;
            ar.type = f.type;
            p.arguments.add(ar);
        }
        return p;
    }

    private static class ArgArrays {

        ArrayList<BigDecimal> decimal = new ArrayList<>();
        ArrayList<String> varchar2 = new ArrayList<>();
        ArrayList<java.sql.Timestamp> date = new ArrayList<>();
        ArrayList<byte[]> raw = new ArrayList<>();

        public void addNumber(Number n) {
            if (n == null) {
                this.decimal.add(null);
            } else if (n instanceof Integer) {
                this.decimal.add(BigDecimal.valueOf((Integer) n));
            } else if (n instanceof Long) {
                this.decimal.add(BigDecimal.valueOf((Long) n));
            } else if (n instanceof BigInteger) {
                this.decimal.add(new BigDecimal((BigInteger) n));
            } else if (n instanceof BigDecimal) {
                this.decimal.add((BigDecimal) n);
            } else if (n instanceof Double) {
                this.decimal.add(BigDecimal.valueOf((Double) n));
            } else {
                throw new RuntimeException("unsupported number type");
            }
        }

        public void addString(String s) {
            this.varchar2.add(s);
        }

        public void addDate(java.util.Date d) {
            if (d == null) {
                this.date.add(null);
            } else {
                this.date.add(new Timestamp(d.getTime()));
            }
        }

        public void addRaw(byte[] r) {
            this.raw.add(r);
        }
    }

    private static class ResArrays {

        ArrayList<BigDecimal> decimal = new ArrayList<>();
        ArrayList<String> varchar2 = new ArrayList<>();
        ArrayList<java.sql.Timestamp> date = new ArrayList<>();
        ArrayList<byte[]> raw = new ArrayList<>();

        int posd = 0;
        int posv = 0;
        int posdate = 0;
        int posr = 0;

        public String readString() {
            String res = varchar2.get(posv);
            posv++;
            return res;
        }

        public BigDecimal readBigDecimal() {
            BigDecimal res = decimal.get(posd);
            posd++;
            return res;
        }

        public java.util.Date readDate() {
            Timestamp ts = this.date.get(posdate);
            posdate++;
            if (ts == null) {
                return null;
            }
            return new java.util.Date(ts.getTime());
        }

        public byte[] readRaw() {
            byte[] res = raw.get(posr);
            posr++;
            return res;
        }
    }

    private String createStatementString(Procedure p) {
        StringBuilder sb = new StringBuilder();
        sb.append("declare\n");
        sb.append("an " + this.numberTableName + ";\n");
        sb.append("av " + this.varchar2TableName + " ;\n");
        sb.append("ad " + this.dateTableName + ";\n");
        sb.append("ar " + this.rawTableName + ";\n");
        sb.append("inn integer :=1;\n");
        sb.append("inv integer :=1;\n");
        sb.append("ind integer :=1;\n");
        sb.append("inr integer :=1;\n");
        sb.append("size_ integer;\n");
        if (p.returnType != null) {
            sb.append("result$ ").append(p.returnType.plsqlName()).append(";\n");
        }
        for (int i = 0; i < p.arguments.size(); i++) {
            sb.append("p" + i + "$ ").append(p.arguments.get(i).type.plsqlName());
            sb.append(";\n");
        }
        sb.append("begin\n");
        sb.append("an :=?;\n");
        sb.append("av :=?;\n");
        sb.append("ad :=?;\n");
        sb.append("ar :=?;\n");
        AtomicInteger counter = new AtomicInteger(0);
        for (int i = 0; i < p.arguments.size(); i++) {
            Argument a = p.arguments.get(i);
            if (a.direction.equals("OUT")) {
                continue;
            }
            a.type.genReadOutThing(sb, counter, "p" + i + "$");
        }
        // at this point the parameters have been filled, clear the
        // parameter arrays
        sb.append("an:= " + this.numberTableName + "();\n");
        sb.append("av:= " + this.varchar2TableName + "();\n");
        sb.append("ad:= " + this.dateTableName + "();\n");
        sb.append("ar:= " + this.rawTableName + "();\n");
        // generate teh actual procedure call
        if (p.returnType != null) {
            sb.append("result$:=");
        }
        sb.append(p.original_name + "(");
        for (int i = 0; i < p.arguments.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(" " + p.arguments.get(i).name + " => ");
            sb.append("p" + i + "$");
        }
        sb.append(");\n");
        // after the procedure call
        if (p.returnType != null) {
            p.returnType.genWriteThing(sb, counter, "result$");
        }
        for (int i = 0; i < p.arguments.size(); i++) {
            Argument a = p.arguments.get(i);
            if (a.direction.equals("IN") || (a.type instanceof SysRefCursorType || a.type instanceof TypedRefCursorType)) {
                continue;
            }
            a.type.genWriteThing(sb, counter, "p" + i + "$");
        }
        sb.append("?:= an;\n");
        sb.append("?:= av;\n");
        sb.append("?:= ad;\n");
        sb.append("?:= ar;\n");
        for (int i = 0; i < p.arguments.size(); i++) {
            Argument a = p.arguments.get(i);
            if (a.direction.equals("OUT") && (a.type instanceof SysRefCursorType || a.type instanceof TypedRefCursorType)) {
                sb.append("?:= p" + i + "$;\n");
            }
        }
        sb.append("end;\n");
        return sb.toString();
    }

    private void callProcedure(
            Procedure proc,
            ArgArrays argsArrays,
            ResArrays resultArrays, // used as out parameter, should be empty
            ArrayList<ArrayList<Map<String, Object>>> outCursors) // used as out parameter, should be empty
            throws SQLException {
        if (proc.plsqlstatement == null) {
            proc.plsqlstatement = createStatementString(proc);
        }
        if (this.effectiveNumberTableName == null) {
            this.effectiveNumberTableName = computeEffectiveName(this.numberTableName);
        }
        if (this.effectiveVarchar2TableName == null) {
            this.effectiveVarchar2TableName = computeEffectiveName(this.varchar2TableName);
        }
        if (this.effectiveDateTableName == null) {
            this.effectiveDateTableName = computeEffectiveName(this.dateTableName);
        }
        if (this.effectiveRawTableName == null) {
            this.effectiveRawTableName = computeEffectiveName(this.rawTableName);
        }
        final java.sql.Array no;
        final java.sql.Array vo;
        final java.sql.Array do_;
        final java.sql.Array ro;
        try (OracleCallableStatement cstm = (OracleCallableStatement) this.connection.prepareCall(proc.plsqlstatement)) {

            cstm.setArray(1, this.connection.createOracleArray(this.effectiveNumberTableName, argsArrays.decimal.toArray(new BigDecimal[0])));
            cstm.setArray(2, this.connection.createOracleArray(this.effectiveVarchar2TableName, argsArrays.varchar2.toArray(new String[0])));
            cstm.setArray(3, this.connection.createOracleArray(this.effectiveDateTableName, argsArrays.date.toArray(new Timestamp[0])));
            cstm.setArray(4, this.connection.createOracleArray(this.effectiveRawTableName, argsArrays.raw.toArray(new byte[0][])));

            cstm.registerOutParameter(5, OracleTypes.ARRAY, this.effectiveNumberTableName);
            cstm.registerOutParameter(6, OracleTypes.ARRAY, this.effectiveVarchar2TableName);
            cstm.registerOutParameter(7, OracleTypes.ARRAY, this.effectiveDateTableName);
            cstm.registerOutParameter(8, OracleTypes.ARRAY, this.effectiveRawTableName);
            int j = 8;
            for (Argument a : proc.arguments) {
                if (a.direction.equals("OUT") && (a.type instanceof SysRefCursorType || a.type instanceof TypedRefCursorType)) {
                    j++;
                    cstm.registerOutParameter(j, OracleTypes.CURSOR);
                }
            }
            cstm.execute();
            no = cstm.getArray(5);
            vo = cstm.getArray(6);
            do_ = cstm.getArray(7);
            ro = cstm.getArray(8);
            int j1 = 8;
            for (Argument a : proc.arguments) {
                if (a.direction.equals("OUT") && (a.type instanceof SysRefCursorType || a.type instanceof TypedRefCursorType)) {
                    j1++;
                    try (ResultSet rs = cstm.getCursor(j1)) {
                        outCursors.add(readSysRefCursor(this.exportDateAsString, this.downCasing, rs));
                    }
                }
            }
        }

        for (Object o : (Object[]) no.getArray()) {
            resultArrays.decimal.add((BigDecimal) o);
        }

        for (Object o : (Object[]) vo.getArray()) {
            resultArrays.varchar2.add((String) o);
        }
        for (Object o : (Object[]) do_.getArray()) {
            resultArrays.date.add((Timestamp) o);
        }
        for (Object o : (Object[]) ro.getArray()) {
            resultArrays.raw.add((byte[]) o);
        }
    }

    private Map<String, Object> call(
            Procedure proc, Map<String, Object> args) throws SQLException {
        ArgArrays argArrays = new ArgArrays();
        for (Argument arg : proc.arguments) {
            if (arg.direction.equals("OUT")) {
                continue;
            }
            String aname = this.downCasing ? arg.name.toLowerCase() : arg.name;
            if (args.containsKey(aname)) {
                Object o = args.get(aname);
                arg.type.fillArgArrays(argArrays, o);
            } else {
                throw new ConversionException("could not find argument " + arg.name);
            }
        }
        ResArrays ra = new ResArrays();
        final ArrayList<ArrayList<Map<String, Object>>> outCursors = new ArrayList<>();
        callProcedure(proc, argArrays, ra, outCursors);

        // convert result array to result
        HashMap<String, Object> res = new HashMap<>();
        if (proc.returnType != null) {
            Object o = proc.returnType.readFromResArrays(ra);
            res.put("RETURN", o);
        }
        for (Argument arg : proc.arguments) {
            if (arg.direction.equals("IN")) {
                continue;
            }
            final Object o;
            if ((arg.type instanceof SysRefCursorType || arg.type instanceof TypedRefCursorType)) {
                o = outCursors.get(0);
                outCursors.remove(0);
            } else {
                o = arg.type.readFromResArrays(ra);
            }
            String aname = this.downCasing ? arg.name.toLowerCase() : arg.name;
            res.put(aname, o);
        }
        return res;
    }

    // call the procedure, for each parameter there must be an entry in Object
    private Object callPositional(Procedure proc, Object[] args) throws SQLException {
        if (proc.arguments.size() > args.length) {
            throw new RuntimeException("not enough arguments supplied");
        }
        if (proc.arguments.size() < args.length) {
            throw new RuntimeException("too many arguments supplied");
        }
        ArgArrays aa = new ArgArrays();
        {
            int i = 0;
            for (Argument arg : proc.arguments) {
                if (!arg.direction.equals("OUT")) {
                    Object o = args[i];
                    if (o instanceof Box) {
                        o = ((Box) o).value;
                    }
                    arg.type.fillArgArrays(aa, o);
                }
                i++;
            }
        }
        ResArrays ra = new ResArrays();
        final ArrayList<ArrayList<Map<String, Object>>> outCursors = new ArrayList<>();
        callProcedure(proc, aa, ra, outCursors);

        // convert res array and to result and out parameters
        Object result;
        if (proc.returnType != null) {
            result = proc.returnType.readFromResArrays(ra);
        } else {
            result = null;
        }
        {
            int i = 0;
            for (Argument arg : proc.arguments) {
                if (!arg.direction.equals("IN")) {

                    if (args[i] != null && args[i] instanceof Box) {
                        final Object o;
                        if ((arg.type instanceof SysRefCursorType || arg.type instanceof TypedRefCursorType)) {
                            o = outCursors.get(0);
                            outCursors.remove(0);
                        } else {
                            o = arg.type.readFromResArrays(ra);
                        }
                        @SuppressWarnings("unchecked")
                        Box<Object> b = (Box<Object>) args[i];
                        b.value = o;
                    } else {
                        throw new RuntimeException("need a box for parameter " + arg.name);
                    }
                }
                i++;
            }
        }
        return result;
    }

    private static String sql1 = "select OWNER,OBJECT_NAME,PACKAGE_NAME,ARGUMENT_NAME,\n"
            + "POSITION,SEQUENCE,DATA_LEVEL,DATA_TYPE,\n"
            + " OVERLOAD, IN_OUT, TYPE_OWNER, TYPE_NAME, TYPE_SUBNAME, PLS_TYPE,data_length\n"
            + " from all_arguments \n"
            + " where owner = ? and package_name = ? and object_name = ?\n"
            + " order by owner,package_name,object_name,overload,sequence";

    private static String sql2 = "select OWNER,OBJECT_NAME,PACKAGE_NAME,ARGUMENT_NAME,"
            + "POSITION,SEQUENCE,DATA_LEVEL,DATA_TYPE,"
            + " OVERLOAD, IN_OUT, TYPE_OWNER, TYPE_NAME, TYPE_SUBNAME, PLS_TYPE,data_length\n"
            + " from all_arguments \n"
            + " where object_id = ? \n"
            + " order by owner,package_name,object_name,overload,sequence";

    private ArrayList<Procedure> getProcsFromDB(String name) throws SQLException {

        ResolvedName rn = resolveName(this.connection, name, false);
        if (rn.dblink != null) {
            throw new RuntimeException("no call over dblink");
        }
        ArrayDeque<ArgumentsRow> argument_rows;
        PreparedStatement pstm;

        if (rn.part1_type == 7 || rn.part1_type == 8) {
            // this a global procedure or function
            pstm = this.connection.prepareCall(sql2);
            pstm.setBigDecimal(1, new BigDecimal(rn.object_number));
        } else if (rn.part1_type == 9) {
            if (rn.part2 == null) {
                throw new RuntimeException("only package given: " + name);
            }
            // this is procedure or function in a package
            pstm = this.connection.prepareCall(sql1);
            pstm.setString(1, rn.schema);
            pstm.setString(2, rn.part1);
            pstm.setString(3, rn.part2);
        } else {
            throw new RuntimeException("this is not a gobal procedure/function, "
                    + "nor a procedure/function in a package: " + name);
        }
        try (ResultSet rs = pstm.executeQuery()) {
            argument_rows = fetchArgumentsRows(rs);
            rs.close();
        }
        pstm.close();
        if (argument_rows.isEmpty()) {
            throw new RuntimeException("procedure in package does not exist or object is not valid: " + name);
        }
        ArrayList<Procedure> procs = new ArrayList<>();
        while (!argument_rows.isEmpty()) {
            Procedure p = eatProc(argument_rows);
            p.original_name = name;
            procs.add(p);
        }
        return procs;
    }

    private Map<String, ArrayList<Procedure>> procsMap = new HashMap<>();

    private ArrayList<Procedure> getProcs(String name) throws SQLException {
        ArrayList<Procedure> procs = procsMap.get(name);
        if (procs == null) {
            procs = getProcsFromDB(name);
            procsMap.put(name, procs);
        }
        return procs;
    }

    public Map<String, Object> call(
            String name, int overload, Map<String, Object> args) throws SQLException {
        ArrayList<Procedure> procs = getProcs(name);

        if (overload > procs.size()) {
            throw new RuntimeException("the overload does not exist for procedure/function " + name);
        }
        if (overload <= 0) {
            throw new RuntimeException("overload must greater or equal 1");
        }
        return call(procs.get(overload - 1), args);
    }

    public Map<String, Object> call(
            String name, Map<String, Object> args) throws SQLException {
        ArrayList<Procedure> procs = getProcs(name);
        if (procs.size() > 1) {
            throw new RuntimeException("procedure/function is overloaded, supply a overload: " + name);
        } else {
            return this.call(procs.get(0), args);
        }
    }

    public Object callPositional(String name, Object... args) throws SQLException {
        ArrayList<Procedure> procs = getProcs(name);
        if (procs.size() > 1) {
            throw new RuntimeException("procedure/function is overloaded, supply a overload: " + name);
        } else {
            return this.callPositional(procs.get(0), args);
        }
    }

    public Object callPositionalO(String name, int overload, Object... args) throws SQLException {
        ArrayList<Procedure> procs = getProcs(name);
        if (overload > procs.size()) {
            throw new RuntimeException("the overload does not exist for procedure/function " + name);
        }
        if (overload <= 0) {
            throw new RuntimeException("overload must greater or equal 1");
        }
        return this.callPositional(procs.get(overload - 1), args);
    }

}
