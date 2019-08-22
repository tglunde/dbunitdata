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
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

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

    public static String testDirectory = "unittest";
    public static String dbtExecutable = "dbt";
    public static String dbtConfig = "dbt_project.yml";
    public static String dbtProfile = "profiles.yml";
    public static String dbtModelCore = "+core";
    public static String dbtModelMart = "mart";
    public static String dbtProjectDir = "d:/project/campaign";
    public static DataSource ds = null;

    @Parameterized.Parameters
    public static Collection<Object[]> makeTests() {
        File parentFile = new File(testDirectory);
        if(!parentFile.isDirectory()) {
            throw new RuntimeException("Parent directory " + testDirectory + " does not resolve to a directory.");
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
            for (File aFile : subDirectory.listFiles(File::isDirectory)) {
                if (!aFile.getName().equalsIgnoreCase("dataset")
//                        && "M7521_R15_2".equals(aFile.getName())
                ) {
                    List<Object> parameters = new ArrayList<>();
                    String testName = suiteName + ":" + aFile.getName();
                    parameters.add(DbTest.ds);
                    parameters.add(testName);
                    parameters.add(dataSetList);
                    parameters.add(new File(aFile.getAbsolutePath() + File.separatorChar + "expected.dml"));
                    parameters.add(initList);
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

    private DataSource dataSource;
    private List<File> dataSetList = new ArrayList<>();
    private File expectedSet = null;
    private List<File> initSourceScriptList = new ArrayList<>();
    private List<File> initCoreScriptList = new ArrayList<>();

    public DbTest(DataSource dataSource, String name, List<File> dataSetList, File expectedSet, List<File> initSourceScriptList, List<File> initCoreScriptList) {
        super(name);
        this.dataSource = dataSource;
        this.dataSetList = dataSetList;
        this.expectedSet = expectedSet;
        this.initSourceScriptList = initSourceScriptList;
        this.initCoreScriptList = initCoreScriptList;
    }

    public boolean addDataSet(File file) {
        return dataSetList.add(file);
    }

    public boolean addInitSourceScript(File file) {
        return initSourceScriptList.add(file);
    }

    public boolean addInitCoreScript(File file) {
        return initCoreScriptList.add(file);
    }

    public void setExpectedSet(File expectedSet) {
        this.expectedSet = expectedSet;
    }

    @Before
    public void prepare() throws Exception {
        LOG.warn("Prepare DbTest " + getName());
        IDatabaseConnection connection = null;
        try {
            connection = new DatabaseConnection( ds.getConnection());

            connection.getConfig().setProperty(DatabaseConfig.FEATURE_QUALIFIED_TABLE_NAMES, Boolean.TRUE);
            connection.getConfig().setProperty(DatabaseConfig.FEATURE_CASE_SENSITIVE_TABLE_NAMES, Boolean.FALSE);
            connection.getConfig().setProperty(DatabaseConfig.PROPERTY_DATATYPE_FACTORY, new PostgresqlDataTypeFactory());
            connection.getConfig().setProperty(DatabaseConfig.PROPERTY_METADATA_HANDLER, new PostgesqlMetadataHandler());
            connection.getConfig().setProperty(DatabaseConfig.PROPERTY_BATCH_SIZE, 10000);
            connection.getConfig().setProperty(DatabaseConfig.PROPERTY_FETCH_SIZE, 10000);

            List<IDataSet> dataSets = new ArrayList<>();
            for(File dataSetFile : this.dataSetList) {
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

        try (Connection conn = dataSource.getConnection()) {
            ScriptRunner runner = new ScriptRunner(conn, false, true);
            //initializing R
            for(File initScriptFile : this.initSourceScriptList) {
                runner.runScript(new BufferedReader( new FileReader(initScriptFile)));
            }
        }
    }

    @Test
    public void execute() throws Exception {
        LOG.warn("Execute test " + super.getName());
        //running dbt -m core to build core tables

        String[] commandCore = { dbtExecutable, "run", "-m", dbtModelCore };
        childProcess(commandCore, dbtProjectDir);

        try (Connection conn = dataSource.getConnection()) {
            ScriptRunner runner = new ScriptRunner(conn, false, true);
            //initializing Core
            for(File scriptFile : this.initCoreScriptList) {
                runner.runScript(new BufferedReader( new FileReader(scriptFile)));
            }
        }

        //running dbt -m mart to build final output
        String[] commandMart = { dbtExecutable, "run", "-m", dbtModelMart };
        childProcess(commandMart, dbtProjectDir);

        try (Connection con = dataSource.getConnection()) {
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

    private void childProcess(String[] command, String workdir) throws Exception {
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

    @After
    public void tearDown() {

    }

    private IDataSet prepareDataSet(File dataSetFile) throws SQLException, DatabaseUnitException, IOException {
        FlatXmlDataSetBuilder builder = new FlatXmlDataSetBuilder();
        builder.setColumnSensing(true);
        return builder.build(dataSetFile);
    }

}
