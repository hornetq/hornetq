/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.util;

import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageFormatException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;

/**
 * JMS specific map backed by a {@link HashMap}.  Does not implement
 * <code>Map</code> but instead implements the body elements of the
 * {@link MapMessage}.  Serves as he basis for {@link MessageProperties}
 * and the body for the <code>MapMessage</code>.
 *
 * @author <a href="mailto:nathan@jboss.org">Nathan Phelps</a>
 * @version $Revision$ $Date$
 */
public class JMSMap implements java.io.Serializable
{

    protected HashMap contents = new HashMap();

    public static final JMSMap createInstance(Class type)
    {
        if (type.equals(MapMessage.class))
        {
            return new JMSMap();
        }
        else
        {
            return new MessageProperties();
        }
    }

    public final void clear()
    {
        this.contents.clear();
    }

    public boolean getBoolean(String name) throws JMSException
    {
        Object value = this.contents.get(name);
        return JMSTypeConversions.getBoolean(value);
    }

    public byte getByte(String name) throws JMSException
    {
        Object value = this.contents.get(name);
        return JMSTypeConversions.getByte(value);
    }

    public byte[] getBytes(String name) throws JMSException
    {
        Object value = this.contents.get(name);
        return JMSTypeConversions.getBytes(value);
    }

    public char getChar(String name) throws JMSException
    {
        Object value = this.contents.get(name);
        return JMSTypeConversions.getChar(value);
    }

    public double getDouble(String name) throws JMSException
    {
        Object value = this.contents.get(name);
        return JMSTypeConversions.getDouble(value);
    }

    public float getFloat(String name) throws JMSException
    {
        Object value = this.contents.get(name);
        return JMSTypeConversions.getFloat(value);
    }

    public int getInt(String name) throws JMSException
    {
        Object value = this.contents.get(name);
        return JMSTypeConversions.getInt(value);
    }

    public long getLong(String name) throws JMSException
    {
        Object value = this.contents.get(name);
        return JMSTypeConversions.getLong(value);
    }

    public Enumeration getMapNames() throws JMSException
    {
        return Collections.enumeration(this.contents.keySet());
    }

    public Object getObject(String name) throws JMSException
    {
        return this.contents.get(name);
    }

    public short getShort(String name) throws JMSException
    {
        Object value = this.contents.get(name);
        return JMSTypeConversions.getShort(value);
    }

    public String getString(String name) throws JMSException
    {
        Object value = this.contents.get(name);
        return JMSTypeConversions.getString(value);
    }

    public boolean itemExists(String name)
    {
        return this.contents.containsKey(name);
    }

    public void setBoolean(String name, boolean value) throws JMSException
    {
        this.contents.put(name, new Boolean(value));
    }

    public void setByte(String name, byte value) throws JMSException
    {
        this.contents.put(name, new Byte(value));
    }

    public void setBytes(String name, byte[] value) throws JMSException
    {
        byte[] bytes = new byte[value.length];
        System.arraycopy(value, 0, bytes, 0, bytes.length);
        this.contents.put(name, bytes);
    }

    public void setBytes(String name, byte[] value, int offset, int length)
            throws JMSException
    {
        byte[] bytes = new byte[length];
        System.arraycopy(value, offset, bytes, 0, length);
        this.contents.put(name, bytes);
    }

    public void setChar(String name, char value) throws JMSException
    {
        this.contents.put(name, new Character(value));
    }

    public void setDouble(String name, double value) throws JMSException
    {
        this.contents.put(name, new Double(value));
    }

    public void setFloat(String name, float value) throws JMSException
    {
        this.contents.put(name, new Float(value));
    }

    public void setInt(String name, int value) throws JMSException
    {
        this.contents.put(name, new Integer(value));
    }

    public void setLong(String name, long value) throws JMSException
    {
        this.contents.put(name, new Long(value));
    }

    public void setObject(String name, Object value) throws JMSException
    {
        if (value instanceof Boolean
                || value instanceof Byte
                || value instanceof Character
                || value instanceof Double
                || value instanceof Float
                || value instanceof Integer
                || value instanceof Long
                || value instanceof Short
                || value instanceof String)
        {
            this.contents.put(name, value);
        }
        else
        {
            throw new MessageFormatException(""); //TOD: Implement message
        }
    }

    public void setShort(String name, short value) throws JMSException
    {
        this.contents.put(name, new Short(value));
    }

    public void setString(String name, String value) throws JMSException
    {
        this.contents.put(name, value);
    }
}