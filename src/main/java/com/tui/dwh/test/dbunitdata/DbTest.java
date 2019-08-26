package com.tui.dwh.test.dbunitdata;

import com.tui.dwh.util.ScriptRunner;
import junit.framework.TestCase;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.CompositeDataSet;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.dbunit.ext.postgresql.PostgresqlDataTypeFactory;
import org.dbunit.operation.DatabaseOperation;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.*;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class DbTest extends TestCase {

    private static final Logger LOG = LoggerFactory.getLogger(DbTest.class);

    public static String TEST_DIRECTORY = "unittest";
    public static String DBT_EXECUTABLE = "dbt";
    public static String DBT_CONFIG = "dbt_project.yml";
    public static String DBT_PROFILE = "profiles.yml";
    public static String DBT_MODEL_CORE = "+core";
    public static String DBT_MODEL_MART = "mart";
    public static String DBT_PROJECT_DIR = "d:/project/campaign";
    public static DataSource DATA_SOURCE = null;
    public static List<File> SRC_DS_LIST = new ArrayList<>();
    public static List<File> SRC_SCRIPT_LIST = new ArrayList<>();

    @Parameterized.Parameters
    public static Collection<Object[]> makeTests() {
        File parentFile = new File(TEST_DIRECTORY);
        if(!parentFile.isDirectory()) {
            throw new RuntimeException("Parent directory " + TEST_DIRECTORY + " does not resolve to a directory.");
        }
        Collection<Object[]> resultList = new ArrayList<>();
        for(File subDirectory : parentFile.listFiles(File::isDirectory)) {
            String suiteName = subDirectory.getName();
            List<File> dataSetList = new ArrayList<File>();
            List<File> initList = new ArrayList<File>();
            for (File aFile : subDirectory.listFiles(File::isDirectory)) {
                if (aFile.getName().equalsIgnoreCase("dataset")) {
                    for (File datasetFile : aFile.listFiles(new FilenameFilter() {
                        @Override
                        public boolean accept(File file, String s) {
                            return s.endsWith("xml");
                        }
                    })) {
                        dataSetList.add(datasetFile);
                    }
                    for (File datasetFile : aFile.listFiles(new FilenameFilter() {
                        @Override
                        public boolean accept(File file, String s) {
                            return s.endsWith("dml");
                        }
                    })) {
                        initList.add(datasetFile);
                    }
                }
            }
            DbTest.SRC_DS_LIST = dataSetList;
            DbTest.SRC_SCRIPT_LIST = initList;

            for (File aFile : subDirectory.listFiles(File::isDirectory)) {
                if (!aFile.getName().equalsIgnoreCase("dataset")
                        //&& "M7364_R1".equals(aFile.getName())
                ) {
                    List<Object> parameters = new ArrayList<>();
                    String testName = suiteName + ":" + aFile.getName();
                    parameters.add(testName);
                    parameters.add(new File(aFile.getAbsolutePath() + File.separatorChar + "expected.dml"));
                    List<File> coreFiles = new ArrayList<>();
                    for (File initFile : aFile.listFiles(File::isFile)) {
                        if (!initFile.getName().equalsIgnoreCase("expected.dml")) {
                            coreFiles.add(initFile);
                        }
                    }
                    parameters.add(coreFiles);
                    LOG.warn(Arrays.toString(parameters.toArray()));
                    resultList.add(parameters.toArray());
                }
            }
        }
        return resultList;
    }

    private File expectedSet = null;
    private List<File> initCoreScriptList = new ArrayList<>();

    public DbTest(String name, File expectedSet, List<File> initCoreScriptList) {
        super(name);
        this.expectedSet = expectedSet;
        this.initCoreScriptList = initCoreScriptList;
    }

    public boolean addDataSet(File file) {
        return SRC_DS_LIST.add(file);
    }

    public boolean addInitSourceScript(File file) {
        return SRC_SCRIPT_LIST.add(file);
    }

    public boolean addInitCoreScript(File file) {
        return initCoreScriptList.add(file);
    }

    public void setExpectedSet(File expectedSet) {
        this.expectedSet = expectedSet;
    }

    @BeforeClass
    public static void prepare() throws Exception {
/*
        LOG.warn("Prepare DbTest - R-Schema");
        IDatabaseConnection connection = null;
        try {
            connection = new DatabaseConnection( DATA_SOURCE.getConnection());

            connection.getConfig().setProperty(DatabaseConfig.FEATURE_QUALIFIED_TABLE_NAMES, Boolean.TRUE);
            connection.getConfig().setProperty(DatabaseConfig.FEATURE_CASE_SENSITIVE_TABLE_NAMES, Boolean.FALSE);
            connection.getConfig().setProperty(DatabaseConfig.PROPERTY_DATATYPE_FACTORY, new PostgresqlDataTypeFactory());
            connection.getConfig().setProperty(DatabaseConfig.PROPERTY_METADATA_HANDLER, new PostgesqlMetadataHandler());
            connection.getConfig().setProperty(DatabaseConfig.PROPERTY_BATCH_SIZE, 10000);
            connection.getConfig().setProperty(DatabaseConfig.PROPERTY_FETCH_SIZE, 10000);

            List<IDataSet> dataSets = new ArrayList<>();
            for(File dataSetFile : DbTest.SRC_DS_LIST) {
                dataSets.add(prepareDataSet(dataSetFile));
            }
            CompositeDataSet compositeDataSet = new CompositeDataSet(dataSets.toArray(new IDataSet[0]));
            DatabaseOperation.TRUNCATE_TABLE.execute(connection, compositeDataSet);
            DatabaseOperation.INSERT.execute(connection, compositeDataSet);
            connection.close();
        } finally {
            if(connection!=null)
                connection.close();
        }

        try (Connection conn = DATA_SOURCE.getConnection()) {
            conn.setAutoCommit(false);
            ScriptRunner runner = new ScriptRunner(conn, false, true);
            //initializing R
            for(File initScriptFile : SRC_SCRIPT_LIST) {
                runner.runScript(new BufferedReader( new FileReader(initScriptFile)));
            }
            conn.commit();
        }

        //running dbt -m core to build core tables
        String[] commandCore = {DBT_EXECUTABLE, "run", "-m", DBT_MODEL_CORE};
        childProcess(commandCore, DBT_PROJECT_DIR);
*/

    }

    @Test
    public void execute() throws Exception {
        LOG.warn("Execute test " + super.getName());

        try (Connection conn = DATA_SOURCE.getConnection()) {
            ScriptRunner runner = new ScriptRunner(conn, false, true);
            //initializing Core
            for(File scriptFile : this.initCoreScriptList) {
                runner.runScript(new BufferedReader( new FileReader(scriptFile)));
            }
            conn.commit();
        }

        //running dbt -m mart to build final output
        String[] commandMart = {DBT_EXECUTABLE, "run", "-m", DBT_MODEL_MART};
        childProcess(commandMart, DBT_PROJECT_DIR);

        try (Connection con = DATA_SOURCE.getConnection()) {
            ResultSet expectedRS = con.createStatement().executeQuery(readFile(this.expectedSet));
            assertExpectedEmpty(expectedRS);
            expectedRS.close();
            LOG.warn("SUCCESS test " + super.getName());
        }
    }

    private void assertExpectedEmpty(ResultSet rs) throws Exception {
        if(rs.next()) {
            // Prepare metadata object and get the number of columns.
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnsNumber = rsmd.getColumnCount();

            // Print column names (a header).
            for (int i = 1; i <= columnsNumber; i++) {
                if (i > 1) System.out.print(" | ");
                System.out.print(rsmd.getColumnName(i));
            }
            System.out.println("");

            do {
                for (int i = 1; i <= columnsNumber; i++) {
                    if (i > 1) System.out.print(" | ");
                    System.out.print(rs.getString(i));
                }
                System.out.println("");
            } while (rs.next());
            assertTrue("Expected ResultSet is not empty.", Boolean.FALSE);
        } else {
            System.out.println("Expected ResultSet is empty.");
        }
    }

    private String readFile(File file) throws Exception {
        return new String(Files.readAllBytes(file.toPath()));
    }

    private static void childProcess(String[] command, String workdir) throws Exception {
        Process process = new ProcessBuilder()
                .inheritIO()
                .command(command)
                .directory(new File(workdir))
                .start();
        int returnCode = process.waitFor();
        if(process.exitValue()!=0) {
            throw new RuntimeException("Command " + Arrays.toString(command) + " resulted in exit code " + process.exitValue());
        }
    }

    private static IDataSet prepareDataSet(File dataSetFile) throws SQLException, DatabaseUnitException, IOException {
        FlatXmlDataSetBuilder builder = new FlatXmlDataSetBuilder();
        builder.setColumnSensing(true);
        return builder.build(dataSetFile);
    }

}
