package aula10pt1;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.StringTokenizer;

class Tabela {

    public ArrayList<String> colunas;
    public ArrayList<String> valores;
    public ArrayList<String> pk;
    public ArrayList<String> pkvalor;
    public ArrayList<String> fk;
    public ArrayList<String> fkvalor;

    public Tabela() {
        colunas = new ArrayList<String>();
        valores = new ArrayList<String>();
        pk = new ArrayList<String>();
        pkvalor = new ArrayList<String>();
        fk = new ArrayList<String>();
        fkvalor = new ArrayList<String>();
    }
}

public class GeraMongo {

    Connection connection;
    Statement stmt;
    ResultSet rs;
    ArrayList<String> nomesTabelas;
    ArrayList<String> nomesColunas;
    ArrayList<String> Pks;
    ArrayList<String> Fks;
    ArrayList<String> Unique;
    ArrayList<String> R_table;
    ArrayList<Tabela> arrayTabela;
    ArrayList<Tabela> arrayTabelaEmbedded;
    Scanner scanner;
    String tabela;
    String embedTabela;
    int flag = -1; //0 -> simples, 1 -> embedded, 2 -> reference, 3 -> nn

    public void gerar(Connection connection) {
        this.connection = connection;
        scanner = new Scanner(System.in);
        scanner.hasNext();
        String tabela = scanner.nextLine();
        if (tabela.indexOf("--embedded") != -1) {
            flag = 1;
            StringTokenizer stok = new StringTokenizer(tabela, " ");
            this.tabela = stok.nextToken().trim();
            stok.nextToken().trim();
            this.embedTabela = stok.nextToken().trim();
        } else if (tabela.indexOf("--reference") != -1) {
            flag = 2;
            StringTokenizer stok = new StringTokenizer(tabela, " ");
            this.tabela = stok.nextToken().trim();
            stok.nextToken().trim();
            this.embedTabela = stok.nextToken().trim();
        } else if (tabela.indexOf("--nn") != -1) {
            flag = 3;
            StringTokenizer stok = new StringTokenizer(tabela, " ");
            this.tabela = stok.nextToken().trim();
            stok.nextToken().trim();
            this.embedTabela = stok.nextToken().trim();
        } else {
            flag = 0;
            this.tabela = tabela;
        }
        tabela = this.tabela;
        if (flag == 0) {
            pegarNomesDeTabelas(tabela);
            selectNomeColuna(tabela);
            getPk(tabela);
            getFks(tabela);
            getUniques(tabela);
            selectStar(tabela, 1);
            montaIndex(tabela);
            //montaString(tabela);
        }
        if (flag == 1) {
            pegarNomesDeTabelas(tabela);
            selectNomeColuna(tabela);
            getPk(tabela);
            getFks(tabela);
            selectStar(tabela, 1);

            pegarNomesDeTabelas(embedTabela);
            selectNomeColuna(embedTabela);
            getPk(embedTabela);
            getFks(embedTabela);
            selectStar(embedTabela, 2);

            montaStringEmbedded(tabela);
        }
        if (flag == 2) {
            pegarNomesDeTabelas(tabela);
            selectNomeColuna(tabela);
            getPk(tabela);
            getFks(tabela);
            selectStar(tabela, 1);
            montaStringRef(tabela);
        }
        if (flag == 3) {
            pegarNomesDeTabelas(tabela);
            selectNomeColuna(tabela);
            getPk(tabela);
            getFks(tabela);
            selectStar(tabela, 1);

            pegarNomesDeTabelas(embedTabela);
            selectNomeColuna(embedTabela);
            getPk(embedTabela);
            getFks(embedTabela);
            selectStar(embedTabela, 2);

            montaStringNN(tabela);
        }

    }

    public void getPk(String tabela) {
        String s = "SELECT column_name FROM all_cons_columns WHERE constraint_name = ("
                + "  SELECT constraint_name FROM all_constraints"
                + "  WHERE UPPER(table_name) = UPPER('" + tabela + "') AND CONSTRAINT_TYPE = 'P'"
                + "AND ROWNUM <=1)";
        try {
            stmt = connection.createStatement();
            rs = stmt.executeQuery(s);
            Pks = new ArrayList<String>();
            while (rs.next()) {
                Pks.add(rs.getString("COLUMN_NAME"));
            }
            stmt.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public void getUniques(String tabela) {
        String s = "SELECT column_name FROM all_cons_columns WHERE constraint_name = ("
                + "  SELECT constraint_name FROM all_constraints"
                + "  WHERE UPPER(table_name) = UPPER('" + tabela + "') AND CONSTRAINT_TYPE = 'U')";
        try {
            stmt = connection.createStatement();
            rs = stmt.executeQuery(s);
            Unique = new ArrayList<String>();
            while (rs.next()) {
                Unique.add(rs.getString("COLUMN_NAME"));
            }
            stmt.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public void montaIndex(String tabela) {
        String mongoString = "db." + tabela + ".createIndex({";
        boolean isFk = false;
        String rTable = "";
        for (int i = 0; i < Unique.size(); i++) {
            String tmp = Unique.get(i);
            if (Fks.contains(tmp)) {
                if (!isFk) {
                    isFk = true;
                    rTable = R_table.get(i);
                    if (i == 0) {
                        mongoString += rTable + "._id: 1";
                    } else {
                        mongoString += ", " + rTable + "._id: 1";
                    }
                }
            } else {
                if (i == 0) {
                    mongoString += tmp + ": 1";
                } else {
                    mongoString += ", " + tmp + ": 1";
                }
            }
        }
        mongoString += "} , {unique : true})";
        System.out.println(mongoString);
    }

    public void getFks(String tabela) {
        String s = "SELECT a.table_name, a.column_name, a.constraint_name, c.owner,"
                + "       c.r_owner, c_pk.table_name r_table_name, c_pk.constraint_name r_pk"
                + "  FROM all_cons_columns a"
                + "  JOIN all_constraints c ON a.owner = c.owner"
                + "                        AND a.constraint_name = c.constraint_name"
                + "  JOIN all_constraints c_pk ON c.r_owner = c_pk.owner"
                + "                           AND c.r_constraint_name = c_pk.constraint_name"
                + " WHERE c.constraint_type = 'R'"
                + " AND a.table_name = '" + tabela + "'";
        //+ " AND R_TABLE_NAME = '" + embedTabela + "'";
        try {
            stmt = connection.createStatement();
            rs = stmt.executeQuery(s);
            Fks = new ArrayList<String>();
            R_table = new ArrayList<String>();
            while (rs.next()) {
                Fks.add(rs.getString("COLUMN_NAME"));
                R_table.add(rs.getString("R_TABLE_NAME")); //nao faz sentido mais
            }
            stmt.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }

    }

    public void pegarNomesDeTabelas(String tabela) {
        String s = "";
        try {
            s = "SELECT table_name FROM user_tables";
            stmt = connection.createStatement();
            rs = stmt.executeQuery(s);
            nomesTabelas = new ArrayList<String>();
            while (rs.next()) {
                nomesTabelas.add(rs.getString("table_name"));
            }
            stmt.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public void selectNomeColuna(String tabela) {
        String s = "SELECT COLUMN_NAME from USER_TAB_COLUMNS where table_name='" + tabela + "'";
        try {
            stmt = connection.createStatement();
            rs = stmt.executeQuery(s);
            nomesColunas = new ArrayList<String>();
            while (rs.next()) {
                nomesColunas.add(rs.getString("COLUMN_NAME"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void selectStar(String tabela, int flag) {
        String s = "SELECT * FROM " + tabela;
        String pkvalue = "";
        ArrayList<Tabela> arrayTabela = null;
        if (flag == 1) {
            this.arrayTabela = new ArrayList<Tabela>();
            arrayTabela = this.arrayTabela;
        } else {
            arrayTabelaEmbedded = new ArrayList<Tabela>();
            arrayTabela = arrayTabelaEmbedded;
        }
        Tabela temptabela = null;
        String coluna = "";
        String valor = "";
        try {
            stmt = connection.createStatement();
            rs = stmt.executeQuery(s);
            while (rs.next()) {
                temptabela = new Tabela();
                ResultSetMetaData rsmd = rs.getMetaData();
                int nColunas = rsmd.getColumnCount();
                for (int i = 1; i <= nColunas; i++) {
                    coluna = nomesColunas.get(i - 1);
                    valor = rs.getString(i);

                    if (Pks.contains(coluna)) {
                        if (!isNumeric(valor)) {
                            temptabela.pkvalor.add("\"" + valor + "\"");
                        } else {
                            temptabela.pkvalor.add(valor);
                        }
                        temptabela.pk.add(coluna);
                    } else {
                        if (!isNumeric(valor)) {
                            temptabela.valores.add("\"" + valor + "\"");
                        } else {
                            temptabela.valores.add(valor);
                        }
                        temptabela.colunas.add(coluna);
                    }
                    if (Fks.contains(coluna)) {
                        temptabela.fk.add(coluna);
                        temptabela.fkvalor.add(valor);
                    }
                }
                arrayTabela.add(temptabela);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void montaString(String tabela) {
        String mongoString = "db.createCollection(\"" + tabela + "\")\n";
        Tabela tmp = null;
        for (int i = 0; i < arrayTabela.size(); i++) {
            tmp = arrayTabela.get(i);
            mongoString += "db." + tabela + ".insert({\n";
            for (int j = 0; j < tmp.pkvalor.size(); j++) {
                if (j == 0) {
                    mongoString += "_id:" + tmp.pkvalor.get(j);
                } else {
                    if (mongoString.charAt(mongoString.length() - 1) == '"') {
                        mongoString = mongoString.substring(0, mongoString.length() - 1);
                    }
                    if (tmp.pkvalor.get(j).charAt(0) == '"') {
                        mongoString += "_" + tmp.pkvalor.get(j).substring(1, tmp.pkvalor.get(j).length() - 1);
                    } else {
                        mongoString += "_" + tmp.pkvalor.get(j);
                    }
                }
            }
            for (int j = 0; j < tmp.colunas.size(); j++) {
                mongoString += ",\n" + tmp.colunas.get(j) + ":" + tmp.valores.get(j);
            }
            mongoString += "})\n";
        }
        System.out.println(mongoString);
    }

    public void montaStringNN(String tabela) {
        montaString(tabela);
        montaString(embedTabela);

    }

    public void montaStringEmbedded(String tabela) {
        String mongoString = "db.createCollection(\"" + tabela + "\")\n";
        Tabela tmp = null;
        int indexof = -1;
        for (int i = 0; i < arrayTabela.size(); i++) {
            tmp = arrayTabela.get(i);
            mongoString += "db." + tabela + ".insert({\n";
            for (int j = 0; j < tmp.pkvalor.size(); j++) {
                if (j == 0) {
                    mongoString += "_id:" + tmp.pkvalor.get(j);
                } else {
                    if (mongoString.charAt(mongoString.length() - 1) == '"') {
                        mongoString = mongoString.substring(0, mongoString.length() - 1);
                    }
                    if (tmp.pkvalor.get(j).charAt(0) == '"') {
                        mongoString += "_" + tmp.pkvalor.get(j).substring(1, tmp.pkvalor.get(j).length() - 1);
                    } else {
                        mongoString += "_" + tmp.pkvalor.get(j);
                    }
                    mongoString += "\"";
                }
            }
            for (int j = 0; j < tmp.colunas.size(); j++) {
                mongoString += ",\n" + tmp.colunas.get(j) + ":" + tmp.valores.get(j);
            }

            mongoString += ",\n" + embedTabela + ":{";
            for (int j = 0; j < arrayTabelaEmbedded.size(); j++) {
                if (arrayTabelaEmbedded.get(j).pkvalor.get(0).substring(1, arrayTabelaEmbedded.get(j).pkvalor.get(0).length() - 1)
                        .equals(tmp.fkvalor.get(0))) {
                    for (int k = 0; k < arrayTabelaEmbedded.get(j).pk.size(); k++) {
                        mongoString += arrayTabelaEmbedded.get(j).pk.get(k) + ":" + arrayTabelaEmbedded.get(j).pkvalor.get(k);
                    }
                    for (int k = 0; k < arrayTabelaEmbedded.get(j).colunas.size(); k++) {
                        mongoString += ",\n" + arrayTabelaEmbedded.get(j).colunas.get(k) + ":" + arrayTabelaEmbedded.get(j).valores.get(k);
                    }
                    break;
                }

            }
            mongoString += "}})\n";
        }
        System.out.println(mongoString);
    }

    public void montaStringRef(String tabela) {
        String mongoString = "db.createCollection(\"" + tabela + "\")\n";

        Tabela tmp = null;
        for (int i = 0; i < arrayTabela.size(); i++) {
            tmp = arrayTabela.get(i);
            mongoString += "var doc = db." + embedTabela + ".findOne({_id:\"" + tmp.fkvalor.get(0) + "\"})\n";
            mongoString += "db." + tabela + ".insert({\n";
            for (int j = 0; j < tmp.pkvalor.size(); j++) {
                if (j == 0) {
                    mongoString += "_id:" + tmp.pkvalor.get(j);
                } else {
                    if (mongoString.charAt(mongoString.length() - 1) == '"') {
                        mongoString = mongoString.substring(0, mongoString.length() - 1);
                    }
                    if (tmp.pkvalor.get(j).charAt(0) == '"') {
                        mongoString += "_" + tmp.pkvalor.get(j).substring(1, tmp.pkvalor.get(j).length() - 1);
                    } else {
                        mongoString += "_" + tmp.pkvalor.get(j);
                    }
                }
            }
            for (int j = 0; j < tmp.colunas.size(); j++) {
                mongoString += ",\n" + tmp.colunas.get(j) + ":" + tmp.valores.get(j);
            }
            mongoString += ",\n" + embedTabela + ": doc._id";
            mongoString += "})\n";
        }
        System.out.println(mongoString);
    }

    public void selectStarEmbedded(String tabela, String valor) {
        String s = "SELECT column_name FROM all_cons_columns WHERE constraint_name = ("
                + "  SELECT constraint_name FROM all_constraints"
                + "  WHERE UPPER(table_name) = UPPER('" + R_table.get(0) + "') AND CONSTRAINT_TYPE = 'P'"
                + " AND ROWNUM <=1"
                + ")";
        String pk = "";
        try {
            stmt = connection.createStatement();
            rs = stmt.executeQuery(s);
            while (rs.next()) {
                pk = rs.getString("COLUMN_NAME");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        s = "SELECT * FROM " + embedTabela + " WHERE " + pk + "= '" + valor + "'";
        try {
            stmt = connection.createStatement();
            rs = stmt.executeQuery(s);
            String mongoString = embedTabela + " :{";
            while (rs.next()) {
                ResultSetMetaData rsmd = rs.getMetaData();
                int nColunas = rsmd.getColumnCount();
                for (int i = 1; i <= nColunas; i++) {
                    if (!isNumeric(rs.getString(i))) {
                        if (Pks.size() == 1) {
                            if (nomesColunas.get(i - 1).equals(Pks.get(0))) {
                                mongoString += "_id: \"" + rs.getString(i) + "\"";
                            } else {
                                mongoString += nomesColunas.get(i - 1) + ": \"" + rs.getString(i) + "\"";
                            }
                        } else {
                            if (nomesColunas.get(i - 1).equals(Pks.get(0))) {
                                mongoString += "_id: \"" + rs.getString(Pks.get(0)) + "_" + rs.getString(Pks.get(1)) + "\"";
                            } else {
                                mongoString += nomesColunas.get(i - 1) + ": \"" + rs.getString(i) + "\"";
                            }
                        }
                    } else {
                        if (Pks.size() == 1) {
                            if (nomesColunas.get(i - 1).equals(Pks.get(0))) {
                                mongoString += "_id: " + rs.getString(i);
                            } else {
                                mongoString += nomesColunas.get(i - 1) + ": " + rs.getString(i);
                            }
                        } else {
                            if (nomesColunas.get(i - 1).equals(Pks.get(0))) {
                                mongoString += "_id: " + rs.getString(Pks.get(0)) + "_" + rs.getString(Pks.get(1));
                            } else {
                                mongoString += nomesColunas.get(i - 1) + ": " + rs.getString(i);
                            }
                        }
                    }
                    if (i != nColunas) {
                        mongoString += ",\n";
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public String selectFk(String valor) {
        return "";
    }

    public static void main(String args[]) {
        GeraMongo o = new GeraMongo();
        o.gerar(Bridge.connectDB());
    }

    public static boolean isNumeric(String str) {
        try {
            double d = Double.parseDouble(str);
        } catch (NumberFormatException | NullPointerException nfe) {
            return false;
        }
        return true;
    }

}
