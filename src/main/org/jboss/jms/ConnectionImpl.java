/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Collection;
import java.io.Serializable;
import java.net.URL;

import javax.jms.Connection;
import javax.jms.ConnectionConsumer;
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.Topic;

import org.javagroups.Address;
import org.javagroups.Channel;
import org.javagroups.ChannelException;
import org.javagroups.ChannelListener;
import org.javagroups.JChannelFactory;
import org.javagroups.Message;
import org.javagroups.MessageListener;
import org.javagroups.blocks.PullPushAdapter;

import org.jboss.util.id.GUID;

/**
 *
 * @author <a href="mailto:nathan@jboss.org">Nathan Phelps</a>
 * @version $Revision$ $Date$
 */
public class ConnectionImpl implements Connection, ChannelListener, MessageListener
{
    private String clientId = null;
    private ConnectionMetaData connectionMetaData = new ConnectionMetaDataImpl();
    private ExceptionListener exceptionListener = null;
    private boolean closed = false;
    private String password = null;
    private String username = null;
    private List sessions = new ArrayList();

    private Channel channel = null;
    private PullPushAdapter connection = null;
    private boolean started = false;

    ConnectionImpl(String username, String password) throws JMSException
    {
        this.username = username;
        this.password = password;

        try
        {
            URL url = Thread.currentThread().getContextClassLoader().getResource("org/jboss/jms/p2p/javagroups-config.xml");
            this.channel = new JChannelFactory().createChannel(url);
            this.channel.setChannelListener(this);
            this.channel.connect("org.jboss.jms.p2p");
            this.connection = new PullPushAdapter(this.channel, this);
            this.connection.start();
        }
        catch (ChannelException exception)
        {
            throw new JMSException(exception.getMessage());
        }
    }

    public void close() throws JMSException
    {
        Iterator iterator = this.sessions.iterator();
        while (iterator.hasNext())
        {
            ((SessionImpl) iterator.next()).close();
            iterator.remove();
        }
        this.closed = true;
        this.connection.stop();
        this.channel.disconnect();
        this.channel.close();
    }

    public ConnectionConsumer createConnectionConsumer(Destination destination, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException
    {
        this.throwExceptionIfClosed();
        this.generateClientIDIfNull();
        return null;
    }

    public ConnectionConsumer createDurableConnectionConsumer(Topic topic, String subscriptionName, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException
    {
        this.throwExceptionIfClosed();
        this.generateClientIDIfNull();
        return null;
    }

    public Session createSession(boolean transacted, int acknowledgeMode) throws JMSException
    {
        this.throwExceptionIfClosed();
        this.generateClientIDIfNull();
        SessionImpl session = new SessionImpl(this, transacted, acknowledgeMode);
        this.sessions.add(session);
        return session;
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

    public void setExceptionListener(ExceptionListener exceptionListener) throws JMSException
    {
        this.throwExceptionIfClosed();
        this.generateClientIDIfNull();
        this.exceptionListener = exceptionListener;
    }

    public void start() throws JMSException
    {
        this.throwExceptionIfClosed();
        this.generateClientIDIfNull();
        this.started = true;
    }

    public void stop() throws JMSException
    {
        this.throwExceptionIfClosed();
        this.generateClientIDIfNull();
        this.started = false;
    }

    public void finalize() throws Throwable
    {
        if (!this.closed)
        {
            this.close();
        }
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

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // Methods that the session calls                                                            //
    ///////////////////////////////////////////////////////////////////////////////////////////////
    void send(MessageImpl message) throws JMSException
    {
        try
        {
            message.setOriginClientID(this.clientId);
            this.connection.send(new Message(null, null, (Serializable) message));
        }
        catch (Exception exception)
        {
            throw new JMSException(exception.getMessage());
        }
    }

    void send(Collection messages) throws JMSException
    {
        try
        {
            Iterator iterator = messages.iterator();
            while (iterator.hasNext())
            {
                ((MessageImpl)iterator.next()).setOriginClientID(this.clientId);
            }
            this.connection.send(new Message(null, null, (Serializable) messages));
        }
        catch (Exception exception)
        {
            throw new JMSException(exception.getMessage());
        }
    }

    TemporaryDestinationImpl createTemporaryDestination()
    {
        return new TemporaryDestinationImpl(this);
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // Implementation of org.javagroups.MessageListener                                          //
    ///////////////////////////////////////////////////////////////////////////////////////////////
    public void receive(Message message)
    {
        if (this.started)
        {
            Object object = message.getObject();
            if (object instanceof List)
            {
                List theList = (List) object;
                Iterator iterator = theList.iterator();
                while (iterator.hasNext())
                {
                    Object listObject = iterator.next();
                    if (listObject instanceof MessageImpl)
                    {
                        MessageImpl currentMessage = (MessageImpl)listObject;
                        if (currentMessage.getOrigianClientID().equals(this.clientId))
                        {
                            currentMessage.setIsLocal(true);
                        }
                        Iterator sessionIterator = this.sessions.iterator();
                        while (sessionIterator.hasNext())
                        {
                            ((SessionImpl) sessionIterator.next()).deliver(currentMessage);
                        }
                    }
                }
            }
            else if (object instanceof MessageImpl)
            {
                MessageImpl theMessage = (MessageImpl) object;
                if (theMessage.getOrigianClientID().equals(this.clientId))
                {
                    theMessage.setIsLocal(true);
                }
                Iterator iterator = this.sessions.iterator();
                while (iterator.hasNext())
                {
                    ((SessionImpl) iterator.next()).deliver(theMessage);
                }
            }
        }
    }

    public byte[] getState()
    {
        return new byte[0];
    }

    public void setState(byte[] bytes)
    {
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // Implementation of org.javagroups.ChannelListener                                          //
    ///////////////////////////////////////////////////////////////////////////////////////////////
    public void channelConnected(Channel channel)
    {
    }

    public void channelDisconnected(Channel channel)
    {
        this.channelClosed(channel);
    }

    public void channelClosed(Channel channel)
    {
        if (this.closed != false && this.exceptionListener != null)
        {
            this.exceptionListener.onException(new JMSException("We were unexpectedly disconnected"));
        }
    }

    public void channelShunned()
    {
        if (this.exceptionListener != null)
        {
            this.exceptionListener.onException(new JMSException("We were shunned."));
        }
    }

    public void channelReconnected(Address address)
    {
    }
}