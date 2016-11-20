package aula10pt1;

import java.sql.*;
import java.util.ArrayList;

public class Bridge {
    // JDBC driver name and database URL
    static final String JDBC_DRIVER = "oracle.jdbc.driver.OracleDriver";
    static final String DB_URL = "jdbc:oracle:thin:@grad.icmc.usp.br:15215:orcl";

    //  Database credentials
    static final String USER = "a8937034";
    static final String PASS = "a8937034";

    public static Connection connectDB() {
        Connection conn = null;
        try {
            // Register JDBC driver
            Class.forName(JDBC_DRIVER);
            System.out.println("Driver registrado");
            // Open connection
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            System.out.println("Conexão estabelecida");
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
            System.out.println("ERRO AO ABRIR CONEXÃO");
        }
        return conn;
    }
}

