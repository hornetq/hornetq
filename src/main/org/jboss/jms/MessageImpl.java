/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms;

import java.io.Serializable;
import java.util.Enumeration;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageNotWriteableException;
import javax.jms.ObjectMessage;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;

import org.jboss.jms.client.p2p.P2PSessionDelegate;
import org.jboss.jms.util.JMSMap;

/**
 *
 * @author <a href="mailto:nathan@jboss.org">Nathan Phelps</a>
 * @version $Revision$ $Date$
 */
public class MessageImpl implements Message, Cloneable, Serializable
{
    static final String BYTES_MESSAGE_NAME = "BytesMessage";
    static final String MAP_MESSAGE_NAME = "MapMessage";
    static final String MESSAGE_NAME = "Message";
    static final String OBJECT_MESSAGE_NAME = "ObjectMessage";
    static final String STREAM_MESSGE_NAME = "StreamMessage";
    static final String TEXT_MESSAGE_NAME = "TextMessage";

    protected Object body = null;
    private String correlationID = null;
    private int deliveryMode = Message.DEFAULT_DELIVERY_MODE;

    private Destination destination = null;
    private long expiration = 0;
    private String messageID = null;
    private int priority = Message.DEFAULT_PRIORITY;
    private JMSMap properties = JMSMap.createInstance(Message.class);
    private boolean readOnly = false;
    private boolean redelivered = false;
    private Destination replyTo = null;
    private long timestamp = 0;
    protected String type = null;

    private P2PSessionDelegate session = null;
    private long deliveryId = -1L;
    private boolean local = false;
    private String originClientId = null;

    public static Message create(Class type) throws JMSException
    {
        if (type.equals(BytesMessage.class))
        {
            return new BytesMessageImpl();
        }
        if (type.equals(MapMessage.class))
        {
            return new MapMessageImpl();
        }
        if (type.equals(Message.class))
        {
            return new MessageImpl();
        }
        if (type.equals(ObjectMessage.class))
        {
            return new ObjectMessageImpl();
        }
        else if (type.equals(StreamMessage.class))
        {
            return new StreamMessageImpl();
        }
        else if (type.equals(BytesMessage.class))
        {
            return new BytesMessageImpl();
        }
        else if (type.equals(TextMessage.class))
        {
            return new TextMessageImpl();
        }
        else
        {
            throw new JMSException("'" + type.getName() + "' is an invalid message type.");
        }
    }

    public void acknowledge() throws JMSException
    {
        if (this.session != null)
        {
            this.session.ancknowledge(this.deliveryId);
        }
    }

    public void clearBody()
    {
        // We generally let the subclass take care of this, but...
        this.body = null;
        this.readOnly = false;
    }

    public final void clearProperties()
    {
        this.properties.clear();
    }

    public final boolean getBooleanProperty(String name) throws JMSException
    {
        return this.properties.getBoolean(name);
    }

    public final byte getByteProperty(String name) throws JMSException
    {
        return this.properties.getByte(name);
    }

    public final double getDoubleProperty(String name) throws JMSException
    {
        return this.properties.getDouble(name);
    }

    public final float getFloatProperty(String name) throws JMSException
    {
        return this.properties.getFloat(name);
    }

    public final int getIntProperty(String name) throws JMSException
    {
        return this.properties.getInt(name);
    }

    public final String getJMSCorrelationID()
    {
        return this.correlationID;
    }

    public final byte[] getJMSCorrelationIDAsBytes()
    {
        return this.correlationID.getBytes();
    }

    public final int getJMSDeliveryMode()
    {
        return this.deliveryMode;
    }

    public final Destination getJMSDestination()
    {
        return this.destination;
    }

    public final long getJMSExpiration()
    {
        return this.expiration;
    }

    public final String getJMSMessageID()
    {
        return this.messageID;
    }

    public final int getJMSPriority()
    {
        return this.priority;
    }

    public final boolean getJMSRedelivered()
    {
        return this.redelivered;
    }

    public final Destination getJMSReplyTo()
    {
        return this.replyTo;
    }

    public final long getJMSTimestamp()
    {
        return this.timestamp;
    }

    public final String getJMSType()
    {
        return this.type;
    }

    public final long getLongProperty(String name) throws JMSException
    {
        return this.properties.getLong(name);
    }

    public final Object getObjectProperty(String name) throws JMSException
    {
        return this.properties.getObject(name);
    }

    public final Enumeration getPropertyNames() throws JMSException
    {
        return this.properties.getMapNames();
    }

    public final short getShortProperty(String name) throws JMSException
    {
        return this.properties.getShort(name);
    }

    public final String getStringProperty(String name) throws JMSException
    {
        return this.properties.getString(name);
    }

    public final boolean isReadOnly()
    {
        return this.readOnly;
    }

    public final boolean hasExpired()
    {
        if (this.expiration < 1)
        {
            return false;
        }
        else
        {
            return System.currentTimeMillis() > this.expiration;
        }
    }

    public final void setSession(P2PSessionDelegate session)
    {
        this.session = session;
    }

    public final void setDeliveryId(long id)
    {
        this.deliveryId = id;
    }

    public final void setOriginClientID(String clientId)
    {
        this.originClientId = clientId;
    }

    public final String getOrigianClientID()
    {
        return this.originClientId;
    }

    public final void setIsLocal(boolean value)
    {
        this.local = value;
    }

    public boolean isLocal()
    {
        return this.local;
    }

    public final boolean propertyExists(String name)
    {
        return this.properties.itemExists(name);
    }

    public final void setBooleanProperty(String name, boolean value) throws JMSException
    {
        this.properties.setBoolean(name, value);
    }

    public final void setByteProperty(String name, byte value) throws JMSException
    {
        this.properties.setByte(name, value);
    }

    public final void setDoubleProperty(String name, double value) throws JMSException
    {
        this.properties.setDouble(name, value);
    }

    public final void setFloatProperty(String name, float value) throws JMSException
    {
        this.properties.setFloat(name, value);
    }

    public final void setIntProperty(String name, int value) throws JMSException
    {
        this.properties.setInt(name, value);
    }

    public final void setJMSCorrelationID(String correlationID)
    {
        this.correlationID = correlationID;
    }

    public final void setJMSCorrelationIDAsBytes(byte[] correlationID)
    {
        throw new UnsupportedOperationException("Not implemented.");
    }

    public final void setJMSDeliveryMode(int deliveryMode)
    {
        this.deliveryMode = deliveryMode;
    }

    public final void setJMSDestination(Destination destination)
    {
        this.destination = destination;
    }

    public final void setJMSExpiration(long expiration)
    {
        this.expiration = expiration;
    }

    public final void setJMSMessageID(String messageID)
    {
        this.messageID = messageID;
    }

    public final void setJMSPriority(int priority)
    {
        this.priority = priority;
    }

    public final void setJMSRedelivered(boolean redelivered)
    {
        this.redelivered = redelivered;
    }

    public final void setJMSReplyTo(Destination replyTo)
    {
        this.replyTo = replyTo;
    }

    public final void setJMSTimestamp(long timestamp)
    {
        this.timestamp = timestamp;
    }

    public final void setJMSType(String type)
    {
        this.type = type;
    }

    public final void setLongProperty(String name, long value) throws JMSException
    {
        this.properties.setLong(name, value);
    }

    public final void setObjectProperty(String name, Object value) throws JMSException
    {
        this.properties.setObject(name, value);
    }

    public final void setReadOnly(boolean value)
    {
        this.readOnly = value;
    }

    public final void setShortProperty(String name, short value) throws JMSException
    {
        this.properties.setShort(name, value);
    }

    public final void setStringProperty(String name, String value) throws JMSException
    {
        this.properties.setString(name, value);
    }

    public Object clone()
    {
        try{
            return super.clone();
        }
        catch (CloneNotSupportedException ignored)
        {
            return null;    // To my knowledge, we shouldn't get here...
        }
    }

    protected void throwExceptionIfReadOnly() throws MessageNotWriteableException
    {
        if (this.readOnly)
        {
            throw new MessageNotWriteableException("Unable to write body: the message body is currently read only.");
        }
    }
}