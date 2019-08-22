package com.tui.dwh.test.dbunitdata;

import junit.framework.Test;
import junit.framework.TestResult;
import junit.framework.TestSuite;
import org.flywaydb.core.Flyway;
import org.junit.internal.TextListener;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Lookup;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

@SpringBootApplication
public class DbUnitApp implements CommandLineRunner {

	private static final Logger log = LoggerFactory.getLogger(DbUnitApp.class);

	private static final String dataSubDirectory = "dataset";

	@Value("${app.test.dir}")
	private String testDirectory = "unittest";
	@Value("${app.dbt.exec}")
	private String dbtExecutable = "dbt";
	@Value("${app.dbt.cfg}")
	private String dbtConfig = "dbt_project.yml";
	@Value("${app.dbt.model.core}")
	private String dbtModelCore = "+core";
	@Value("${app.dbt.model.mart}")
	private String dbtModelMart = "mart";
	@Value("${app.project.dir}")
	private String dbtProjectDir = "d:/project/campaign";

	public static void main(String[] args) {
		SpringApplication.run(DbUnitApp.class, args);
	}

	@Autowired
	JdbcTemplate jdbcTemplate;

	@Autowired
	ApplicationContext ctx;

	@Override
	public void run(String... args) throws Exception {
		DbTest.dbtExecutable = this.dbtExecutable;
		DbTest.dbtConfig = this.dbtConfig;
		DbTest.dbtModelMart = this.dbtModelMart;
		DbTest.dbtModelCore = this.dbtModelCore;
		DbTest.dbtProjectDir = this.dbtProjectDir;
		DbTest.testDirectory = this.testDirectory;
		DbTest.ds = jdbcTemplate.getDataSource();
		JUnitCore jUnitCore = new JUnitCore();
		jUnitCore.addListener(new TextListener(System.out));
		Result result = jUnitCore.run(DbTest.class);
		System.out.println("Finished. Result: Failures: " +
				result.getFailureCount() + ". Ignored: " +
				result.getIgnoreCount() + ". Tests run: " +
				result.getRunCount() + ". Time: " +
				result.getRunTime() + "ms.");

	}

}
