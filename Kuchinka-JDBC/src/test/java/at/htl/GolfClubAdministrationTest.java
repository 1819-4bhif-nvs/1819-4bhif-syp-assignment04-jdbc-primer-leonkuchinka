package at.htl;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class GolfClubAdministrationTest {
    public static final String DRIVER_STRING = "org.apache.derby.jdbc.ClientDriver";
    public static final String CONNECTION_STRING = "jdbc:derby://localhost:1527/db";
    public static final String USER = "app";
    public static final String PASSWORD = "app";
    private static Connection conn;

    @BeforeClass
    public static void initJdbc() {
        try {
            Class.forName(DRIVER_STRING);
            conn = DriverManager.getConnection(CONNECTION_STRING, USER, PASSWORD);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            System.err.println("Verbindung zu Datenbank nicht möglich: " + e.getMessage());
            System.exit(1);
        }
        try {
            Statement stmt = conn.createStatement();
            String sql = "CREATE TABLE golfcourse (" +
                    "id INT CONSTRAINT golfcourse_pk PRIMARY KEY," +
                    "name VARCHAR(255) NOT NULL," +
                    "holes INT NOT NULL" +
                    ")";
            stmt.execute(sql);
            sql = "INSERT INTO golfcourse (id, name, holes) VALUES (1, 'Championship Course', 18)";
            stmt.execute(sql);
            sql = "INSERT INTO golfcourse (id, name, holes) VALUES (2, '9-Hole Course', 9)";
            stmt.execute(sql);
            System.out.println("Tabelle GolfCourse erstellt");


            sql = "CREATE TABLE golfer (" +
                    "id INT CONSTRAINT golfer_pk PRIMARY KEY," +
                    "name VARCHAR(255) NOT NULL," +
                    "hcp DOUBLE NOT NULL" +
                    ")";
            stmt.execute(sql);
            sql = "INSERT INTO golfer (id, name, hcp) VALUES (1, 'Leon Kuchinka', 1.7)";
            stmt.execute(sql);
            sql = "INSERT INTO golfer (id, name, hcp) VALUES (2, 'Max Mustermann', 2.3)";
            stmt.execute(sql);
            System.out.println("Tabelle Golfer erstellt");


            sql = "CREATE TABLE TeeTime (" +
                    "id INT CONSTRAINT teetime_pk PRIMARY KEY GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1)," +
                    "time TIMESTAMP NOT NULL," +
                    "golfcourse_id INT NOT NULL CONSTRAINT fk_golfcourse_id references golfcourse(id)," +
                    "golfer_id INT NOT NULL CONSTRAINT fk_golfer_id references golfer(id)"+
                    ")";
            stmt.execute(sql);
            PreparedStatement pstmt = conn.prepareStatement("INSERT INTO TeeTime (time, golfcourse_id, golfer_id) VALUES (?, ?, ?)");
            pstmt.setTimestamp(1, java.sql.Timestamp.valueOf("2018-01-01 10:00:00"));
            pstmt.setInt(2, 1);
            pstmt.setInt(3, 1);
            pstmt.execute();

            pstmt.setTimestamp(1, java.sql.Timestamp.valueOf("2018-01-01 10:10:00"));
            pstmt.setInt(2, 2);
            pstmt.setInt(3, 2);
            pstmt.execute();

            System.out.println("Tabelle Teetime erstellt");

        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
    }
    @AfterClass
    public static void teardownJdbc() {
        try {
            conn.createStatement().execute("DROP TABLE teetime");
            System.out.println("Tabelle Teetime gelöscht");
        } catch (SQLException e) {
            System.out.println("Tablle Teetime konnte nicht gelöscht werden:\n" + e.getMessage());
        }
        try {
            conn.createStatement().execute("DROP TABLE golfcourse");
            System.out.println("Tabelle GolfCourse gelöscht");
        } catch (SQLException e) {
            System.out.println("Tablle GolfCourse konnte nicht gelöscht werden:\n" + e.getMessage());
        }
        try {
            conn.createStatement().execute("DROP TABLE golfer");
            System.out.println("Tabelle Golfer gelöscht");
        } catch (SQLException e) {
            System.out.println("Tablle Golfer konnte nicht gelöscht werden:\n" + e.getMessage());
        }


        try {
            if(conn != null && !conn.isClosed()){
                conn.close();
                System.out.println("Good bye");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGolfCourse() {
        PreparedStatement pstmt = null;
        try {
            pstmt = conn.prepareStatement("SELECT id, name, holes FROM golfcourse");
            ResultSet rs = pstmt.executeQuery();

            rs.next();
            assertThat(rs.getString("name"), is("Championship Course"));
            assertThat(rs.getInt("holes"), is(18));
            rs.next();
            assertThat(rs.getString("name"), is("9-Hole Course"));
            assertThat(rs.getInt("holes"), is(9));

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testGolfer() {
        PreparedStatement pstmt = null;
        try {
            pstmt = conn.prepareStatement("SELECT id, name, hcp FROM golfer");
            ResultSet rs = pstmt.executeQuery();

            rs.next();
            assertThat(rs.getString("name"), is("Leon Kuchinka"));
            assertThat(rs.getDouble("hcp"), is(1.7));
            rs.next();
            assertThat(rs.getString("name"), is("Max Mustermann"));
            assertThat(rs.getDouble("hcp"), is(2.3));

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testTeeTime() {
        PreparedStatement pstmt = null;
        try {
            pstmt = conn.prepareStatement("SELECT time, golfcourse_id, golfer_id FROM teetime");
            ResultSet rs = pstmt.executeQuery();

            rs.next();
            assertThat(rs.getTimestamp("time"), is(java.sql.Timestamp.valueOf("2018-01-01 10:00:00")));
            assertThat(rs.getInt("golfcourse_id"), is(1));
            assertThat(rs.getInt("golfer_id"), is(1));
            rs.next();
            assertThat(rs.getTimestamp("time"), is(java.sql.Timestamp.valueOf("2018-01-01 10:10:00")));
            assertThat(rs.getInt("golfcourse_id"), is(2));
            assertThat(rs.getInt("golfer_id"), is(2));

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testMetaDataGolfCourse() {
        try {
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            String catalog = null;
            String schemaPattern = null;
            String tableNamePattern = "GOLFCOURSE";
            String columnNamePattern = null;

            ResultSet result = databaseMetaData.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);

            result.next();
            String columnName = result.getString(4);
            int columnType = result.getInt(5);
            assertThat(columnName, is("ID"));
            assertThat(columnType, is(Types.INTEGER));

            result.next();
            columnName = result.getString(4);
            columnType = result.getInt(5);
            assertThat(columnName, is("NAME"));
            assertThat(columnType, is(Types.VARCHAR));

            result.next();
            columnName = result.getString(4);
            columnType = result.getInt(5);
            assertThat(columnName, is("HOLES"));
            assertThat(columnType, is(Types.INTEGER));


            String schema = null;
            String tableName = "GOLFCOURSE";

            result = databaseMetaData.getPrimaryKeys(
                    catalog, schema, tableName);

            result.next();
            columnName = result.getString(4);
            assertThat(columnName, is("ID"));

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    @Test
    public void testMetaDataGolfer() {
        try {
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            String catalog = null;
            String schemaPattern = null;
            String tableNamePattern = "GOLFER";
            String columnNamePattern = null;

            ResultSet result = databaseMetaData.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);

            result.next();
            String columnName = result.getString(4);
            int columnType = result.getInt(5);
            assertThat(columnName, is("ID"));
            assertThat(columnType, is(Types.INTEGER));

            result.next();
            columnName = result.getString(4);
            columnType = result.getInt(5);
            assertThat(columnName, is("NAME"));
            assertThat(columnType, is(Types.VARCHAR));

            result.next();
            columnName = result.getString(4);
            columnType = result.getInt(5);
            assertThat(columnName, is("HCP"));
            assertThat(columnType, is(Types.DOUBLE));


            String schema = null;
            String tableName = "GOLFER";

            result = databaseMetaData.getPrimaryKeys(
                    catalog, schema, tableName);

            result.next();
            columnName = result.getString(4);
            assertThat(columnName, is("ID"));

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    @Test
    public void testMetaDataTeeTime() {
        try {
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            String catalog = null;
            String schemaPattern = null;
            String tableNamePattern = "TEETIME";
            String columnNamePattern = null;

            ResultSet result = databaseMetaData.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);

            result.next();
            String columnName = result.getString(4);
            int columnType = result.getInt(5);
            assertThat(columnName, is("ID"));
            assertThat(columnType, is(Types.INTEGER));

            result.next();
            columnName = result.getString(4);
            columnType = result.getInt(5);
            assertThat(columnName, is("TIME"));
            assertThat(columnType, is(Types.TIMESTAMP));

            result.next();
            columnName = result.getString(4);
            columnType = result.getInt(5);
            assertThat(columnName, is("GOLFCOURSE_ID"));
            assertThat(columnType, is(Types.INTEGER));

            result.next();
            columnName = result.getString(4);
            columnType = result.getInt(5);
            assertThat(columnName, is("GOLFER_ID"));
            assertThat(columnType, is(Types.INTEGER));

            String schema = null;
            String tableName = "TEETIME";

            result = databaseMetaData.getPrimaryKeys(
                    catalog, schema, tableName);

            result.next();
            columnName = result.getString(4);
            assertThat(columnName, is("ID"));

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
