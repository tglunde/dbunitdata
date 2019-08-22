package com.tui.dwh.test.dbunitdata;

import org.dbunit.database.DefaultMetadataHandler;
import org.dbunit.database.IMetadataHandler;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

public class PostgesqlMetadataHandler extends DefaultMetadataHandler {
    @Override
    public ResultSet getTables(DatabaseMetaData metaData, String schemaName, String[] tableType) throws SQLException {
        if(schemaName != null) {
            schemaName = schemaName.toLowerCase();
        }
        return super.getTables(metaData, schemaName, tableType);
    }

    @Override
    public ResultSet getColumns(DatabaseMetaData databaseMetaData, String schemaName, String tableName) throws SQLException {
        if(schemaName != null) {
            schemaName = schemaName.toLowerCase();
        }
        if(tableName != null) {
            tableName = tableName.toLowerCase();
        }
        return super.getColumns(databaseMetaData, schemaName, tableName);
    }

    @Override
    public boolean tableExists(DatabaseMetaData metaData, String schemaName, String tableName) throws SQLException {
        if(schemaName != null) {
            schemaName = schemaName.toLowerCase();
        }
        if(tableName != null) {
            tableName = tableName.toLowerCase();
        }
        return super.tableExists(metaData, schemaName, tableName);
    }

    @Override
    public ResultSet getPrimaryKeys(DatabaseMetaData metaData, String schemaName, String tableName) throws SQLException {
        if(schemaName != null) {
            schemaName = schemaName.toLowerCase();
        }
        if(tableName != null) {
            tableName = tableName.toLowerCase();
        }
        return super.getPrimaryKeys(metaData, schemaName, tableName);
    }
}
