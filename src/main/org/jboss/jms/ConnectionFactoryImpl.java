/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms;

import org.jboss.remoting.InvokerLocator;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import java.io.Serializable;
import java.net.MalformedURLException;

/**
 * Provides a factory for creating {@link Connection} objects.  The
 * domain specific implementations--{@link QueueConnectionFactory} and
 * {@link TopicConnectionFactory} simply wrap and delegating to this
 * common facility.
 *
 * @author <a href="mailto:nathan@jboss.org">Nathan Phelps</a>
 * @version $Revision$ $Date$
 */
public class ConnectionFactoryImpl implements ConnectionFactory, Serializable
{
    private InvokerLocator invokerLocator;

    // To faciliate the creation of connections without JNDI.
    public ConnectionFactoryImpl(String uri) throws JMSException
    {
        try
        {
            this.invokerLocator = new InvokerLocator(uri);
        }
        catch (MalformedURLException exception)
        {
            throw new JMSException(exception.getMessage());
        }
    }

    public ConnectionFactoryImpl(InvokerLocator invokerLocator)
    {
        this.invokerLocator = invokerLocator;
    }

    public Connection createConnection() throws JMSException
    {
        // TODO: See JMS 1.1 specification section 4.3.1 concerning default credentials
        return new ConnectionImpl(this.invokerLocator, null, null);
    }

    public Connection createConnection(String username, String password)
            throws JMSException
    {
        return new ConnectionImpl(this.invokerLocator, username, password);
    }

    public String toString()
    {
        return this.getClass().getName()
                + "["
                + this.invokerLocator.getLocatorURI()
                + "]";
    }
}