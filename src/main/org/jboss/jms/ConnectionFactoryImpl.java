/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import java.io.Serializable;

/**
 * Provides a factory for creating {@link Connection} objects.  The
 * domain specific implementations--{@link javax.jms.QueueConnectionFactory} and
 * {@link javax.jms.TopicConnectionFactory} simply wrap and delegating to this
 * common facility.
 *
 * @author <a href="mailto:nathan@jboss.org">Nathan Phelps</a>
 * @version $Revision$ $Date$
 */
public class ConnectionFactoryImpl implements ConnectionFactory, Serializable
{
    public ConnectionFactoryImpl()
    {
    }

    public Connection createConnection() throws JMSException
    {
        // TOD: See JMS 1.1 specification section 4.3.1 concerning default credentials
        return this.createConnection(null, null);
    }

    public Connection createConnection(String username, String password)
            throws JMSException
    {
        return new ConnectionImpl(username, password);
    }
}