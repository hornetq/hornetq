/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms;

import org.jboss.jms.util.JMSMap;

import javax.jms.JMSException;
import javax.jms.MapMessage;
import java.util.Enumeration;

/**
 * @author <a href="mailto:nathan@jboss.org">Nathan Phelps</a>
 * @version $Revision$ $Date$
 */
public class MapMessageImpl extends MessageImpl implements MapMessage
{

    MapMessageImpl()
    {
        super.type = MessageImpl.MAP_MESSAGE_NAME;
        super.body = JMSMap.createInstance(MapMessage.class);
    }

    public void clearBody()
    {
        this.getBody().clear();
        this.setReadOnly(false);
    }

    private JMSMap getBody()
    {
        return (JMSMap) super.body;
    }

    public final boolean getBoolean(String name) throws JMSException
    {
        return this.getBody().getBoolean(name);
    }

    public final byte getByte(String name) throws JMSException
    {
        return this.getBody().getByte(name);
    }

    public final byte[] getBytes(String name) throws JMSException
    {
        return this.getBody().getBytes(name);
    }

    public final char getChar(String name) throws JMSException
    {
        return this.getBody().getChar(name);
    }

    public final double getDouble(String name) throws JMSException
    {
        return this.getBody().getDouble(name);
    }

    public final float getFloat(String name) throws JMSException
    {
        return this.getBody().getFloat(name);
    }

    public final int getInt(String name) throws JMSException
    {
        return this.getBody().getInt(name);
    }

    public final long getLong(String name) throws JMSException
    {
        return this.getBody().getLong(name);
    }

    public final Enumeration getMapNames() throws JMSException
    {
        return this.getBody().getMapNames();
    }

    public final Object getObject(String name) throws JMSException
    {
        return this.getBody().getObject(name);
    }

    public final short getShort(String name) throws JMSException
    {
        return this.getBody().getShort(name);
    }

    public final String getString(String name) throws JMSException
    {
        return this.getBody().getString(name);
    }

    public final boolean itemExists(String name) throws JMSException
    {
        return this.getBody().itemExists(name);
    }

    public final void setBoolean(String name, boolean value) throws JMSException
    {
        super.throwExceptionIfReadOnly();
        this.getBody().setBoolean(name, value);
    }

    public final void setByte(String name, byte value) throws JMSException
    {
        super.throwExceptionIfReadOnly();
        this.getBody().setByte(name, value);
    }

    public final void setBytes(String name, byte[] value) throws JMSException
    {
        super.throwExceptionIfReadOnly();
        this.getBody().setBytes(name, value);
    }

    public final void setBytes(String name, byte[] value, int offset, int length)
            throws JMSException
    {
        super.throwExceptionIfReadOnly();
        this.getBody().setBytes(name, value, offset, length);
    }

    public final void setChar(String name, char value) throws JMSException
    {
        super.throwExceptionIfReadOnly();
        this.getBody().setChar(name, value);
    }

    public final void setDouble(String name, double value) throws JMSException
    {
        super.throwExceptionIfReadOnly();
        this.setDouble(name, value);
    }

    public final void setFloat(String name, float value) throws JMSException
    {
        super.throwExceptionIfReadOnly();
        this.getBody().setFloat(name, value);
    }

    public final void setInt(String name, int value) throws JMSException
    {
        super.throwExceptionIfReadOnly();
        this.getBody().setInt(name, value);
    }

    public final void setLong(String name, long value) throws JMSException
    {
        super.throwExceptionIfReadOnly();
        this.getBody().setLong(name, value);
    }

    public final void setObject(String name, Object value) throws JMSException
    {
        super.throwExceptionIfReadOnly();
        this.getBody().setObject(name, name);
    }

    public final void setShort(String name, short value) throws JMSException
    {
        super.throwExceptionIfReadOnly();
        this.getBody().setShort(name, value);
    }

    public final void setString(String name, String value) throws JMSException
    {
        super.throwExceptionIfReadOnly();
        this.getBody().setString(name, value);
    }

}