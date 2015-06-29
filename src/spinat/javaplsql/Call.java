package spinat.javaplsql;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import oracle.jdbc.OracleCallableStatement;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.OracleTypes;
import oracle.sql.ARRAY;
import oracle.sql.ArrayDescriptor;

public class Call {

    public static abstract class Type {

        public abstract String plsqlName();
    }

    public static class Field {

        String name;
        Type type;
    }

    public static class RecordType extends Type {

        String owner;
        String package_;
        String name;
        ArrayList<Field> fields;

        @Override
        public String plsqlName() {
            return this.owner + "." + this.package_ + "." + this.name;
        }
    }

    public static class NamedType extends Type {

        String name; // varchar2, number, integer ...

        @Override
        public String plsqlName() {
            if (this.name.equals("VARCHAR2")) {
                return "varchar2(32767)";
            } else {
                return this.name;
            }
        }
    }

    public static class TableType extends Type {

        String owner;
        String package_;
        String name;
        Type slottype;

        @Override
        public String plsqlName() {
            return this.owner + "." + this.package_ + "." + this.name;
        }
    }

    public static class Argument {

        public String name;
        public String direction;
        public Type type;
    }

    public static class SysRefCursor {
    }

    public static class Procedure {

        String owner;
        String package_;
        String name;
        int overload;
        ArrayList<Argument> arguments;
    }

    // this class corresponds 1:1 the columns in all_arguments
    public static class ArgumentsRow {

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
    }

    public static ArrayList<ArgumentsRow> fetchArgumentsRows(ResultSet rs) throws SQLException {
        ArrayList<ArgumentsRow> res = new ArrayList<>();
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
            r.in_out = rs.getString("IN_OUT");
            r.type_owner = rs.getString("TYPE_OWNER");
            r.type_name = rs.getString("TYPE_NAME");
            r.type_subname = rs.getString("TYPE_SUBNAME");
            r.pls_type = rs.getString("PLS_TYPE");
            res.add(r);
        }
        return res;
    }

    public static class Args {

        int pos;
        final ArrayList<ArgumentsRow> args;

        public Args(ArrayList<ArgumentsRow> args) {
            this.args = args;
            pos = 0;
        }

        boolean atEnd() {
            return pos >= args.size();
        }

        public void next() {
            pos++;
        }

        public ArgumentsRow getRow() {
            return args.get(pos);
        }
    }

    public static Field eatArg(Args a) {
        ArgumentsRow r = a.getRow();
        Field f = new Field();
        f.name = r.argument_name;
        if (r.data_type.equals("NUMBER") || r.data_type.equals("VARCHAR2")
                || r.data_type.equals("DATE") || r.data_type.equals("INTEGER")) {
            NamedType t = new NamedType();
            t.name = r.data_type;
            f.type = t;
            a.next();
            return f;
        }
        if (r.data_type.equals("TABLE")) {
            TableType t = new TableType();
            t.owner = r.type_owner;
            t.package_ = r.type_name;
            t.name = r.type_subname;
            a.next();
            Field f2 = eatArg(a);
            f.type = f2.type;
            return f;
        }
        if (r.data_type.equals("PL/SQL RECORD")) {
            RecordType t = new RecordType();
            t.owner = r.type_owner;
            t.package_ = r.type_name;
            t.name = r.type_subname;
            t.fields = new ArrayList<>();
            int level = r.data_level;
            a.next();
            while (!a.atEnd() && a.getRow().data_level > level) {
                t.fields.add(eatArg(a));
            }
            f.type = t;
            return f;

        }
        throw new RuntimeException("unsupported type: " + r.data_type);
    }

    public static Procedure eatProc(Args a) {
        Procedure p = new Procedure();
        p.package_ = a.getRow().package_name;
        p.name = a.getRow().object_name;
        p.overload = a.getRow().overload;
        p.owner = a.getRow().owner;
        p.arguments = new ArrayList<>();
        while (!a.atEnd()) {
            String io = a.getRow().in_out;
            Field f = eatArg(a);
            Argument ar = new Argument();
            ar.direction = io;
            ar.name = f.name;
            ar.type = f.type;
            p.arguments.add(ar);
        }
        return p;
    }

    static class ArgArrays {

        ArrayList<BigDecimal> decimal = new ArrayList<>();
        ArrayList<String> varchar2 = new ArrayList<>();
        ArrayList<java.sql.Timestamp> date = new ArrayList<>();

        public void add(Number n) {
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
            } else {
                throw new RuntimeException("unsupported number type");
            }
        }

        public void add(String s) {
            this.varchar2.add(s);
        }

        public void add(Date d) {
            if (d == null) {
                this.date.add(null);
            } else {
                this.date.add(new Timestamp(d.getTime()));
            }
        }
    }

    static void fillNamedType(ArgArrays a, NamedType t, Object o) {
        if (t.name.equals("VARCHAR2")) {
            a.add((String) o);
        } else if (t.name.equals("NUMBER") || t.name.equals("INTEGER")) {
            a.add((Number) o);
        } else if (t.name.equals("DATE")) {
            a.add((Date) o);
        } else {
            throw new RuntimeException("unsupported named type");
        }
    }

    static void fillTable(ArgArrays a, TableType t, Object o) {
        if (o == null) {
            a.add((BigDecimal) null);
        } else {
            ArrayList l = (ArrayList) o;
            a.add(l.size());
            for (Object x : l) {
                fillThing(a, t.slottype, x);
            }
        }
    }

    static void fillRecord(ArgArrays a, RecordType t, Object o) {
        if (o instanceof Map) {
            Map m = (Map) o;
            for (Field f : t.fields) {
                Object x;
                if (m.containsKey(f.name)) {
                    x = m.get(f.name);
                } else {
                    String n2 = t.name.toLowerCase();
                    if (m.containsKey(n2)) {
                        x = m.get(n2);
                    } else {
                        throw new RuntimeException("slot not found: " + t.name);
                    }
                }
                fillThing(a, f.type, x);
            }
        }
    }

    static void fillThing(ArgArrays a, Type t, Object o) {
        if (t instanceof NamedType) {
            fillNamedType(a, (NamedType) t, o);
        } else if (t instanceof TableType) {
            fillTable(a, (TableType) t, o);
        } else if (t instanceof RecordType) {
            fillRecord(a, (RecordType) t, o);
        } else {
            throw new RuntimeException("not suppotrted type");
        }
    }

    static class ResArrays {

        ArrayList<BigDecimal> decimal = new ArrayList<>();
        ArrayList<String> varchar2 = new ArrayList<>();
        ArrayList<java.sql.Timestamp> date = new ArrayList<>();

        int posd = 0;
        int posv = 0;
        int posdate = 0;

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

        public Date readDate() {
            Timestamp ts = this.date.get(posdate);
            posdate++;
            if (ts==null) {
                return null;
            }
            return new Date(ts.getTime());
        }
    }

    static Object readNamedType(ResArrays a, NamedType t) {
        if (t.name.equals("VARCHAR2")) {
            return a.readString();
        } else if (t.name.equals("NUMBER") || t.name.equals("INTEGER")) {
            return a.readBigDecimal();
        } else if (t.name.equals("DATE")) {
            return a.readDate();
        } else {
            throw new RuntimeException("unsupported named type: " + t.name);
        }
    }

    static Map<String, Object> readRecord(ResArrays a, RecordType t) {
        HashMap<String, Object> m = new HashMap<>();
        for (Field f : t.fields) {
            Object o = readThing(a, f.type);
            m.put(f.name, o);
        }
        return m;
    }

    static ArrayList readTable(ResArrays a, TableType t) {
        BigDecimal b = a.readBigDecimal();
        if (b == null) {
            return null;
        } else {
            int size = b.intValue();
            ArrayList res = new ArrayList();
            for (int i = 0; i < size; i++) {
                res.add(readThing(a, t.slottype));
            }
            return res;
        }
    }

    static Object readThing(ResArrays a, Type t) {
        if (t instanceof NamedType) {
            return readNamedType(a, (NamedType) t);
        } else if (t instanceof RecordType) {
            return readRecord(a, (RecordType) t);
        } else if (t instanceof TableType) {
            return readTable(a, (TableType) t);
        } else {
            throw new RuntimeException("unsupported type " + t);
        }
    }

    static void readOutThing(StringBuilder sb, Type t, String target) {
        if (t instanceof NamedType) {
            readOutNamedType(sb, (NamedType) t, target);
        } else if (t instanceof RecordType) {
            readOutRecord(sb, (RecordType) t, target);
        } else if (t instanceof TableType) {
            readOutTable(sb, (TableType) t, target);
        }
    }

    // used for the index variables
    static int counter = 0;

    static void readOutTable(StringBuilder sb, TableType t, String target) {
        sb.append("size := an(inn); inn:= inn+1;\n");
        sb.append("if size is null then\n");
        sb.append("  ").append(target).append(":=null;\n");
        sb.append("else\n");
        sb.append(" ").append(target).append(" := new ")
                .append(t.package_).append(".").append(t.name).append("()");
        String index = "i" + counter;
        counter++;
        String newTarget = target + "(" + index + ")";
        sb.append("  for ").append(counter).append(" in 1 .. size loop\n");
        readOutThing(sb, t.slottype, newTarget);
        sb.append("end loop;\n");
        sb.append("end if;\n");
    }

    static void readOutNamedType(StringBuilder sb, NamedType t, String target) {
        if (t.name.equals("VARCHAR2")) {
            sb.append(target).append(":= av(inv); inv := inv+1;\n");
        } else if (t.name.equals("NUMBER") || t.name.equals("INTEGER")) {
            sb.append(target).append(":= an(inn);inn:=inn+1;\n");
        } else if (t.name.equals("DATE")) {
            sb.append(target).append(":= ad(ind); ind := ind+1;\n");
        } else {
            throw new RuntimeException("unsupported base type");
        }
    }

    static void readOutRecord(StringBuilder sb, RecordType t, String target) {
        for (Field f : t.fields) {
            String a = target + "." + f.name;
            readOutThing(sb, f.type, a);
        }
    }

    public static void genWriteThing(StringBuilder sb, Type t, String source) {
        if (t instanceof NamedType) {
            genWriteNamedType(sb, (NamedType) t, source);
        } else if (t instanceof RecordType) {
            genWriteRecord(sb, (RecordType) t, source);
        } else if (t instanceof TableType) {
            genWriteTable(sb, (TableType) t, source);
        } else {
            throw new RuntimeException("unsupported type");
        }
    }

    static void genWriteRecord(StringBuilder sb, RecordType t, String source) {
        for (Field f : t.fields) {
            String a = source + "." + f.name;
            genWriteThing(sb, f.type, a);
        }
    }

    static void genWriteTable(StringBuilder sb, TableType t, String source) {
        sb.append("an.extend;\n");
        sb.append(" if " + source + " is null then\n");
        sb.append("    an(an.last) := null;\n");
        sb.append("else \n");
        sb.append("  an(an.last) := nvl(" + source + ",0);\n");
        String index = "i" + counter;
        counter++;
        sb.append("for " + index + " in 1 .. nvl(" + source + ",0) loop\n");
        genWriteThing(sb, t.slottype, source + "(" + index + ")");
        sb.append("end loop;\n");
    }

    static void genWriteNamedType(StringBuilder sb, NamedType t, String source) {
        if (t.name.equals("VARCHAR2")) {
            sb.append("av.extend; av(av.last) := " + source + ";\n");
        } else if (t.name.equals("NUMBER") || t.name.equals("INTEGER")) {
            sb.append("an.extend; an(an.last):= " + source + ";\n");
        } else if (t.name.equals("DATE")) {
            sb.append("ad.extend; ad(ad.last):= " + source + ";\n");
        } else {
            throw new RuntimeException("unsupported base type");
        }
    }

    static String createStatementString(Procedure p) {
        StringBuilder sb = new StringBuilder();
        sb.append("declare\n");
        sb.append("an number_array;\n");
        sb.append("av varchar2_array;\n");
        sb.append("ad date_array;\n");
        sb.append("inn integer :=1;\n");
        sb.append("inv integer :=1;\n");
        sb.append("ind integer :=1;\n");
        for (int i = 0; i < p.arguments.size(); i++) {
            sb.append("p" + i+"$").append(" ").append(p.arguments.get(i).type.plsqlName());
            sb.append(";\n");
        }
        sb.append("begin\n");
        sb.append("an :=?;\n");
        sb.append("av :=?;\n");
        sb.append("ad :=?;\n");
        for (int i = 0; i < p.arguments.size(); i++) {
            Argument a = p.arguments.get(i);
            if (a.direction.equals("OUT")) {
                continue;
            }
            readOutThing(sb, a.type, "p" + i + "$");
        }
        sb.append(p.package_ + "." + p.name + "(");
        for (int i = 0; i < p.arguments.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append("p" + i + "$");
        }
        sb.append(");\n");
        sb.append("an:= number_array();av:=varchar2_array();ad:=date_array();\n");
        for (int i = 0; i < p.arguments.size(); i++) {
            Argument a = p.arguments.get(i);
            if (a.direction.equals("IN")) {
                continue;
            }
            genWriteThing(sb, a.type, "p" + i + "$");
        }
        sb.append("?:= an;\n");
        sb.append("?:= av;\n");
        sb.append("?:= ad;\n");
        sb.append("end;\n");
        return sb.toString();
    }

    public static Map<String, Object> CallProcedure(OracleConnection oc,
            Procedure p, Map<String, Object> args) throws SQLException {
        String s = createStatementString(p);
        System.out.println(s);
        OracleCallableStatement cstm = (OracleCallableStatement) oc.prepareCall(s);
        ArgArrays aa = new ArgArrays();
        for (Argument arg : p.arguments) {
            if (arg.direction.equals("OUT")) {
                continue;
            }
            Object o = args.get(arg.name);
            fillThing(aa, arg.type, o);
        }
        {
            oracle.sql.ARRAY na = (oracle.sql.ARRAY) oc.createARRAY("NUMBER_ARRAY", aa.decimal.toArray(new BigDecimal[0]));
            oracle.sql.ARRAY va = (oracle.sql.ARRAY) oc.createARRAY("VARCHAR2_ARRAY", aa.varchar2.toArray(new String[0]));
            oracle.sql.ARRAY da = (oracle.sql.ARRAY) oc.createARRAY("DATE_ARRAY", aa.date.toArray(new Timestamp[0]));

            cstm.setArray(1, na);
            cstm.setArray(2, va);
            cstm.setArray(3, da);
        }
        cstm.registerOutParameter(4, OracleTypes.ARRAY, "NUMBER_ARRAY");
        cstm.registerOutParameter(5, OracleTypes.ARRAY, "VARCHAR2_ARRAY");
        cstm.registerOutParameter(6, OracleTypes.ARRAY, "DATE_ARRAY");

        cstm.execute();
        ARRAY no = cstm.getARRAY(4);
        ARRAY vo = cstm.getARRAY(5);
        ARRAY do_ = cstm.getARRAY(6);
        ResArrays ra = new ResArrays();

        for (Object o : (Object[]) no.getArray()) {
            ra.decimal.add((BigDecimal) o);
        }

        for (Object o : (Object[]) vo.getArray()) {
            ra.varchar2.add((String) o);
        }
        for (Object o : (Object[]) do_.getArray()) {
            ra.date.add((Timestamp) o);
        }
        cstm.close();
        HashMap<String, Object> res = new HashMap<>();
        for (Argument arg : p.arguments) {
            if (arg.direction.equals("IN")) {
                continue;
            }
            Object o = readThing(ra, arg.type);
            res.put(arg.name, o);
        }
        return res;
    }

    public static Map<String, Object> CallProcedure(OracleConnection oc,
            String owner, String pack, String name, Map<String, Object> args) throws SQLException {
        ArrayList<ArgumentsRow> r = new ArrayList<>();
        PreparedStatement pstm = oc.prepareStatement(
                "select OWNER,OBJECT_NAME,PACKAGE_NAME,ARGUMENT_NAME,"
                + "POSITION,SEQUENCE,DATA_LEVEL,DATA_TYPE,"
                + " OVERLOAD, IN_OUT, TYPE_OWNER, TYPE_NAME, TYPE_SUBNAME, PLS_TYPE\n"
                + " from all_arguments \n"
                + " where owner = ? and package_name = ? and object_name = ?\n"
                + " order by owner,package_name,object_name,sequence");
        pstm.setString(1, owner);
        pstm.setString(2, pack);
        pstm.setString(3, name);
        ResultSet rs = pstm.executeQuery();
        r = fetchArgumentsRows(rs);
        rs.close();
        pstm.close();
        Args args2 = new Args(r);
        Procedure p = eatProc(args2);
        return CallProcedure(oc, p, args);
    }

}
