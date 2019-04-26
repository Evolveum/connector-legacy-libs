/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Copyright (c) 2014 ForgeRock AS. All Rights Reserved
 *
 * The contents of this file are subject to the terms
 * of the Common Development and Distribution License
 * (the License). You may not use this file except in
 * compliance with the License.
 *
 * You can obtain a copy of the License at
 * http://forgerock.org/license/CDDLv1.0.html
 * See the License for the specific language governing
 * permission and limitations under the License.
 *
 * When distributing Covered Code, include this CDDL
 * Header Notice in each file and include the License file
 * at http://forgerock.org/license/CDDLv1.0.html
 * If applicable, add the following below the CDDL Header,
 * with the fields enclosed by brackets [] replaced by
 * your own identifying information:
 * " Portions Copyrighted [year] [name of copyright owner]"
 *
 */

package org.forgerock.openicf.connectors.scriptedsql

import org.forgerock.openicf.misc.scriptedcommon.ScriptedConfiguration
import org.identityconnectors.common.StringUtil
import org.identityconnectors.common.logging.Log
import org.identityconnectors.common.security.GuardedString
import org.identityconnectors.framework.spi.ConfigurationClass
import org.identityconnectors.framework.spi.ConfigurationProperty

/**
 * Extends the {@link org.forgerock.openicf.misc.scriptedcommon.ScriptedConfiguration} class to provide all the necessary
 * parameters to initialize the ScriptedSQL Connector.
 *
 * The Driver class. The jdbcDriver is located by connector framework to
 * connect to database. Required configuration property, and should be
 * validated.
 *
 * <pre>
 *     <ul>
 *         <li>Oracle: oracle.jdbc.driver.OracleDriver</li>
 *         <li>MySQL: com.mysql.jdbc.Driver</li>
 *         <li>DB2: COM.ibm.db2.jdbc.net.DB2Driver</li>
 *         <li>MSSQL: com.microsoft.sqlserver.jdbc.SQLServerDriver</li>
 *         <li>Sybase: com.sybase.jdbc2.jdbc.SybDriver</li>
 *         <li>Derby: org.apache.derby.jdbc.ClientDriver</li>
 *         <li>Derby embedded: org.apache.derby.jdbc.EmbeddedDriver</li>
 *         <li></li>
 *     </ul>
 * </pre>
 * <p>
 *
 * The new connection validation query. The query can be empty. Then the
 * auto commit true/false command is applied by default. This can be
 * insufficient on some database drivers because of caching Then the
 * validation query is required.
 *
 * Database validationQuery notes:
 * </p>
 *
 * <pre>
 * <ul>
 *     <li>hsqldb - select 1 from INFORMATION_SCHEMA.SYSTEM_USERS</li>
 *     <li>Oracle - select 1 from dual</li>
 *     <li>DB2 - select 1 from sysibm.sysdummy1</li>
 *     <li>mysql - select 1</li>
 *     <li>microsoft SQL Server - select 1 (tested on SQL-Server 9.0, 10.5 [2008])</li>
 *     <li>postgresql - select 1</li>
 *     <li>ingres - select 1</li>
 *     <li>derby - values 1</li>
 *     <li>H2 - select 1</li>
 *     <li>Firebird - select 1 from rdb$database</li>
 * </ul>
 * </pre>
 *
 * @author Gael Allioux <gael.allioux@forgerock.com>
 * @author Martin Lizner <martin.lizner@ami.cz>
 *
 */
@ConfigurationClass(skipUnsupported = true)
public class ScriptedSQLConfiguration extends ScriptedConfiguration {

    /**
     * Setup logging for the {@link ScriptedSQLConfiguration}.
     */
    static final Log log = Log.getLog(ScriptedSQLConfiguration.class);
    public static final String EMPTY_STR = "";

    // =======================================================================
    // Configuration Interface
    // =======================================================================

    /**
     * Database Login User name. This user name is used to connect to database.
     * The provided user name and password should have rights to
     * insert/update/delete the rows in the configured identity holder table.
     * Required configuration property, and should be validated
     */
    private String user = EMPTY_STR;

    /**
     * @return user value
     */
    @ConfigurationProperty(order = 50, displayMessageKey = "USER_DISPLAY", helpMessageKey = "USER_HELP")
    public String getUser() {
        return this.user;
    }

    /**
     * @param value
     */
    public void setUser(String value) {
        this.user = value;
    }
    /**
     * Database access Password. This password is used to connect to database.
     * The provided user name and password should have rights to
     * insert/update/delete the rows in the configured identity holder table.
     * Required configuration property, and should be validated
     */
    private GuardedString password;

    /**
     * @return password value
     */
    @ConfigurationProperty(order = 51, confidential = true, displayMessageKey = "PASSWORD_DISPLAY", helpMessageKey = "PASSWORD_HELP")
    public GuardedString getPassword() {
        return this.password;
    }

    /**
     * @param value
     */
    public void setPassword(GuardedString value) {
        this.password = value;
    }

    /**
     * The datasource name is used to connect to database.
     */
    private String datasource = EMPTY_STR;

    /**
     * Return the datasource
     *
     * @return datasource value
     */
    @ConfigurationProperty(order = 20, displayMessageKey = "DATASOURCE_DISPLAY", helpMessageKey = "DATASOURCE_HELP")
    public String getDatasource() {
        return datasource;
    }

    /**
     * @param value
     */
    public void setDatasource(String value) {
        this.datasource = value;
    }
    /**
     * The jndiFactory name is used to connect to database.
     */
    private String[] jndiProperties;

    /**
     * Return the jndiFactory
     *
     * @return jndiFactory value
     */
    @ConfigurationProperty(order = 21, displayMessageKey = "JNDI_PROPERTIES_DISPLAY", helpMessageKey = "JNDI_PROPERTIES_HELP")
    public String[] getJndiProperties() {
        return jndiProperties;
    }

    /**
     * @param value
     */
    public void setJndiProperties(String[] value) {
        this.jndiProperties = value;
    }

    /**
     * The Driver class. The jdbcDriver is located by connector framework to
     * connect to database. Required configuration property, and should be
     * validated.
     * Oracle: oracle.jdbc.driver.OracleDriver
     * MySQL: com.mysql.jdbc.Driver db2: COM.ibm.db2.jdbc.net.DB2Driver
     * MSSQL: com.microsoft.sqlserver.jdbc.SQLServerDriver
     * Sybase: com.sybase.jdbc2.jdbc.SybDriver
     * Derby: org.apache.derby.jdbc.ClientDriver
     * Derby embedded: org.apache.derby.jdbc.EmbeddedDriver
     *
     */
    private String jdbcDriver = "com.mysql.jdbc.Driver";

    /**
     * @return jdbcDriver value
     */
    @ConfigurationProperty(order = 52, displayMessageKey = "JDBC_DRIVER_DISPLAY", helpMessageKey = "JDBC_DRIVER_HELP")
    public String getJdbcDriver() {
        return this.jdbcDriver;
    }

    /**
     * @param value
     */
    public void setJdbcDriver(String value) {
        this.jdbcDriver = value;
    }
    /**
     * Database connection URL. The url is used to connect to database. Required
     * configuration property, and should be validated.
     * Oracle: jdbc:oracle:thin:@%h:%p:%d MySQL jdbc:mysql://%h:%p/%d
     * Sybase: jdbc:sybase:Tds:%h:%p/%d DB2 jdbc:db2://%h:%p/%d
     * SQL: jdbc:sqlserver://%h:%p;DatabaseName=%d
     * Derby: jdbc:derby://%h:%p/%d
     * Derby: embedded jdbc:derby:%d
     */
    private String jdbcUrlTemplate = "jdbc:mysql://%h:%p/%d";

    /**
     * Return the jdbcUrlTemplate
     *
     * @return url value
     */
    @ConfigurationProperty(order = 53, displayMessageKey = "URL_TEMPLATE_DISPLAY", helpMessageKey = "URL_TEMPLATE_HELP")
    public String getJdbcUrlTemplate() {
        return jdbcUrlTemplate;
    }

    /**
     * @param value
     */
    public void setJdbcUrlTemplate(String value) {
        this.jdbcUrlTemplate = value;
    }
    /**
     * With autoCommit set to true, each SQL statement is treated as a
     * transaction. It is automatically committed right after it is executed.
     */
    private boolean autoCommit = true;

    /**
     * Accessor for the autoCommit property
     *
     * @return the autoCommit
     */
    public boolean isAutoCommit() {
        return autoCommit;
    }

    /**
     * Setter for the autoCommit property.
     *
     * @param autoCommit
     */
    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

/**
 * Attempt to validate the arguments added to the Configuration.
 *
 * TODO: This method does not seem to be called ever
 *
 */
    @Override
    public void validate() {
        super.validate();
        log.info("Validate ScriptedSQLConfiguration");
        // check the url is configured
        if (StringUtil.isNotBlank(getJdbcUrlTemplate())) {
            log.info("Validate driver configuration.");

            // determine if you can get a connection to the database..
            if (getUser() == null) {
                throw new IllegalArgumentException(getMessage("MSG_USER_BLANK"));
            }

            // make sure the jdbcDriver is in the class path..
            if (StringUtil.isBlank(getJdbcDriver())) {
                throw new IllegalArgumentException(getMessage("MSG_JDBC_DRIVER_BLANK"));
            }
            try {
                Class.forName(getJdbcDriver());
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException(getMessage("MSG_JDBC_DRIVER_NOT_FOUND"));
            }
            log.ok("driver configuration is ok");
        } else {
            throw new IllegalArgumentException(getMessage("MSG_JDBC_TEMPLATE_BLANK"));
        }
        log.ok("Configuration is valid");
    }

    /**
     * Format a URL given a template. Recognized template characters are: %
     * literal % h host p port d database
     *
     * @return the database url
     */
    public String formatUrlTemplate() {
        final StringBuffer b = new StringBuffer();
        final String url = getJdbcUrlTemplate();
        final int len = url.length();
        for (int i = 0; i < len; i++) {
            char ch = url.charAt(i);
            if (ch != '%') {
                b.append(ch);
            } else if (i + 1 < len) {
                i++;
                ch = url.charAt(i);
                if (ch == '%') {
                    b.append(ch);
                } else if (ch == 'h') {
                    b.append(getHost());
                } else if (ch == 'p') {
                    b.append(getPort());
                } else if (ch == 'd') {
                    b.append(getDatabase());
                }
            }
        }
        String formattedURL = b.toString();
        log.ok("UrlTemplate is formated to {0}", formattedURL);
        return formattedURL;
    }

    /*@Override
    public void release() {
        synchronized (this) {
            super.release();
            if (null != dataSource) {
                dataSource.close();
                dataSource = null;
            }
        }
    }*/

    /**
     * Format the connector message
     *
     * @param key
     *            key of the message
     * @return return the formated message
     */
    public String getMessage(String key) {
        final String fmt = getConnectorMessages().format(key, key);
        log.ok("Get for a key {0} connector message {1}", key, fmt);
        return fmt;
    }
}
