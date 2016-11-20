package aula10pt1;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.StringTokenizer;

public class GeraMongo2 {

    Connection connection;
    Statement stmt;
    ResultSet rs;
    ArrayList<String> nomesTabelas;
    ArrayList<String> nomesColunas;
    ArrayList<String> Pks;
    ArrayList<String> Fks;
    ArrayList<String> R_table;
    Scanner scanner;
    String tabela;
    String embedTabela;

    public void gerar(Connection connection) {
        this.connection = connection;
        scanner = new Scanner(System.in);
        scanner.hasNext();
        String tabela = scanner.nextLine();
        if (tabela.indexOf("--embedded") == -1) {
            this.tabela = tabela;
        } else {
            StringTokenizer stok = new StringTokenizer(tabela, " ");
            this.tabela = stok.nextToken().trim();
            stok.nextToken().trim();
            this.embedTabela = stok.nextToken().trim();

        }
        tabela = this.tabela;
        
            pegarNomesDeTabelas(tabela);
            selectNomeColuna(tabela);
            getPk(tabela);
            getFks(tabela);
            selectStar(tabela);
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
        try {
            stmt = connection.createStatement();
            rs = stmt.executeQuery(s);
            Fks = new ArrayList<String>();
            R_table = new ArrayList<String>();
            while (rs.next()) {
                Fks.add(rs.getString("COLUMN_NAME"));
                R_table.add(rs.getString("R_TABLE_NAME"));
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

    public void selectStar(String tabela) {
        String s = "SELECT * FROM " + tabela;
        String pkvalue = "";
        try {
            String mongoString = "db.createCollection(\"" + tabela + "\")\n";
            stmt = connection.createStatement();
            rs = stmt.executeQuery(s);
            System.out.println(mongoString);

            while (rs.next()) {
                ResultSetMetaData rsmd = rs.getMetaData();
                int nColunas = rsmd.getColumnCount();
                mongoString = "db." + tabela + ".insert({\n";
                for (int i = 1; i <= nColunas; i++) {
                    if (!isNumeric(rs.getString(i))) {
                        if (Pks.size() == 1) {
                            if (nomesColunas.get(i - 1).equals(Pks.get(0))) {
                                mongoString += "_id: \"" + rs.getString(i) + "\"";
                                pkvalue = rs.getString(i);
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
                                pkvalue = rs.getString(i);
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
                mongoString += "})\n";
                System.out.println(mongoString);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
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
        
        s = "SELECT * FROM " + embedTabela + " WHERE " + pk + "= '" + valor +"'";
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
    public void selectStarEmbedded2(String tabela, String valor) {
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
        
        s = "SELECT * FROM " + embedTabela + " WHERE " + pk + "= '" + valor +"'";
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
        } catch (NumberFormatException nfe) {
            return false;
        }
        return true;
    }

}
