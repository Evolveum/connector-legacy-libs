/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * <p>
 * Copyright (c) 2014 ForgeRock AS. All Rights Reserved
 * <p>
 * The contents of this file are subject to the terms
 * of the Common Development and Distribution License
 * (the License). You may not use this file except in
 * compliance with the License.
 * <p>
 * You can obtain a copy of the License at
 * http://forgerock.org/license/CDDLv1.0.html
 * See the License for the specific language governing
 * permission and limitations under the License.
 * <p>
 * When distributing Covered Code, include this CDDL
 * Header Notice in each file and include the License file
 * at http://forgerock.org/license/CDDLv1.0.html
 * If applicable, add the following below the CDDL Header,
 * with the fields enclosed by brackets [] replaced by
 * your own identifying information:
 * " Portions Copyrighted [year] [name of copyright owner]"
 */
package org.forgerock.openicf.connectors.scriptedsql;

import groovy.lang.Binding;
import groovy.lang.Closure;
import org.forgerock.openicf.misc.scriptedcommon.ScriptedConnectorBase;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.spi.Configuration;
import org.identityconnectors.framework.spi.ConnectorClass;
import org.identityconnectors.framework.spi.PoolableConnector;

/**
 * Main implementation of the ScriptedSQL Connector.
 *
 * @author Gael Allioux <gael.allioux@forgerock.com>
 * @author Martin Lizner <martin.lizner@ami.cz>
 */
@ConnectorClass(
        displayNameKey = "groovy.sql.connector.display",
        configurationClass = ScriptedSQLConfiguration.class,
        messageCatalogPaths = {
                "org/forgerock/openicf/connectors/groovy/Messages",
                "org/forgerock/openicf/connectors/scriptedsql/Messages"
        })
public class ScriptedSQLConnector extends ScriptedConnectorBase<ScriptedSQLConfiguration> implements PoolableConnector {

    private ScriptedSQLConnection connection;
    static Log log = Log.getLog(ScriptedSQLConnector.class);

    protected Object evaluateScript(String scriptName, Binding arguments, Closure<Object> scriptEvaluator) {
        arguments.setVariable(CONNECTION, connection.getConnectionHandler());
        return scriptEvaluator.call(scriptName, arguments);
    }

    @Override
    public void init(final Configuration configuration) {
        log.ok("Creating new connector object for URL: {0}", ((ScriptedSQLConfiguration) configuration).getJdbcUrlTemplate());
        this.connection = new ScriptedSQLConnection((ScriptedSQLConfiguration) configuration);
        super.init(configuration);
    }

    @Override
    public void dispose() {
        log.ok("Disposing connector object");

        if (connection != null) {
            connection.dispose();
            connection = null;
        }

        super.dispose();
    }

    @Override
    public void checkAlive() {
        log.ok("Testing connector object");

        connection.test();
    }
}
