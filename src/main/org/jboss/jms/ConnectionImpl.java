/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms;

import org.jboss.remoting.InvokerLocator;
import org.jboss.util.id.GUID;

import javax.jms.Connection;
import javax.jms.ConnectionConsumer;
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.Topic;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author <a href="mailto:nathan@jboss.org">Nathan Phelps</a>
 * @version $Revision$ $Date$
 */
public class ConnectionImpl implements Connection
{
    private String clientId = null;
    private ConnectionMetaData connectionMetaData =
            new ConnectionMetaDataImpl();
    private ExceptionListener exceptionListener = null;
    private boolean closed = false;
    private String password = null;
    private String username = null;
    private List sessions = new ArrayList();

    ConnectionImpl(
            InvokerLocator invokerLocator,
            String username,
            String password)
            throws JMSException
    {
        this.username = username;
        this.password = password;
    }

    public void close() throws JMSException
    {
        this.closed = true;
    }

    public ConnectionConsumer createConnectionConsumer(
            Destination destination,
            String messageSelector,
            ServerSessionPool sessionPool,
            int maxMessages)
            throws JMSException
    {
        this.throwExceptionIfClosed();
        this.generateClientIDIfNull();
        return null;
    }

    public ConnectionConsumer createDurableConnectionConsumer(
            Topic topic,
            String subscriptionName,
            String messageSelector,
            ServerSessionPool sessionPool,
            int maxMessages)
            throws JMSException
    {
        this.throwExceptionIfClosed();
        this.generateClientIDIfNull();
        return null;
    }

    public Session createSession(boolean transacted, int acknowledgeMode)
            throws JMSException
    {
        this.throwExceptionIfClosed();
        this.generateClientIDIfNull();
        return null;
    }

    public String getClientID() throws JMSException
    {
        this.throwExceptionIfClosed();
        this.generateClientIDIfNull();
        return this.clientId;
    }

    public ExceptionListener getExceptionListener() throws JMSException
    {
        this.throwExceptionIfClosed();
        this.generateClientIDIfNull();
        return this.exceptionListener;
    }

    public ConnectionMetaData getMetaData() throws JMSException
    {
        this.throwExceptionIfClosed();
        this.generateClientIDIfNull();
        return this.connectionMetaData;
    }

    public synchronized void setClientID(String clientID) throws JMSException
    {
        this.throwExceptionIfClosed();
        if (this.clientId != null)
        {
            throw new IllegalStateException("The client Id has already been set by the provider.  To supply your own value, you must set the client ID immediatly after creating the connection.  See section 4.3.2 of the JMS specification for more information.");
        }
        this.clientId = clientID;
    }

    public void setExceptionListener(ExceptionListener exceptionListener)
            throws JMSException
    {
        this.throwExceptionIfClosed();
        this.generateClientIDIfNull();
        this.exceptionListener = exceptionListener;
    }

    public void start() throws JMSException
    {
        this.throwExceptionIfClosed();
        this.generateClientIDIfNull();
    }

    public void stop() throws JMSException
    {
        this.throwExceptionIfClosed();
        this.generateClientIDIfNull();
    }

    private void throwExceptionIfClosed()
    {
        if (this.closed)
        {
            throw new IllegalStateException("The connection is closed.");
        }
    }

    private synchronized void generateClientIDIfNull() throws JMSException
    {
        if (this.clientId == null)
        {
            this.setClientID(new GUID().toString().toUpperCase());
        }
    }

}