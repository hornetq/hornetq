/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms;

import org.jboss.jms.util.JMSTypeConversions;

import javax.jms.JMSException;
import javax.jms.MessageNotReadableException;
import javax.jms.StreamMessage;
import java.util.ArrayList;

/**
 *
 * @author <a href="mailto:nathan@jboss.org">Nathan Phelps</a>
 * @version $Revision$ $Date$
 */
public class StreamMessageImpl extends MessageImpl implements StreamMessage
{
    private int index = 0;

    StreamMessageImpl()
    {
        this.type = MessageImpl.STREAM_MESSGE_NAME;
        this.body = new ArrayList();
    }

    public final void clearBody()
    {
        this.getBody().clear();
        this.setReadOnly(false);
    }

    public final boolean readBoolean() throws JMSException
    {
        this.throwExceptionIfNotReadable();
        boolean value =
                JMSTypeConversions.getBoolean(this.getBody().get(this.index));
        this.incrementPosition();
        return value;
    }

    public final byte readByte() throws JMSException
    {
        this.throwExceptionIfNotReadable();
        byte value = JMSTypeConversions.getByte(this.getBody().get(this.index));
        this.incrementPosition();
        return value;
    }

    public final int readBytes(byte[] value) throws JMSException
    {
        this.throwExceptionIfNotReadable();
        value = JMSTypeConversions.getBytes(this.getBody().get(this.index));
        this.incrementPosition();
        if (value == null)
        {
            return -1;
        }
        else
        {
            return 0;
        }
    }

    public final char readChar() throws JMSException
    {
        this.throwExceptionIfNotReadable();
        char value = JMSTypeConversions.getChar(this.getBody().get(this.index));
        this.incrementPosition();
        return value;
    }

    public final double readDouble() throws JMSException
    {
        this.throwExceptionIfNotReadable();
        double value =
                JMSTypeConversions.getDouble(this.getBody().get(this.index));
        this.incrementPosition();
        return value;
    }

    public final float readFloat() throws JMSException
    {
        this.throwExceptionIfNotReadable();
        float value =
                JMSTypeConversions.getFloat(this.getBody().get(this.index));
        this.incrementPosition();
        return value;
    }

    public final int readInt() throws JMSException
    {
        this.throwExceptionIfNotReadable();
        int value = JMSTypeConversions.getInt(this.getBody().get(this.index));
        this.incrementPosition();
        return value;
    }

    public final long readLong() throws JMSException
    {
        this.throwExceptionIfNotReadable();
        long value = JMSTypeConversions.getLong(this.getBody().get(this.index));
        this.incrementPosition();
        return value;
    }

    public final Object readObject() throws JMSException
    {
        this.throwExceptionIfNotReadable();
        Object value =
                JMSTypeConversions.getObject(this.getBody().get(this.index));
        this.incrementPosition();
        return value;
    }

    public final short readShort() throws JMSException
    {
        this.throwExceptionIfNotReadable();
        short value =
                JMSTypeConversions.getShort(this.getBody().get(this.index));
        this.incrementPosition();
        return value;
    }

    public final String readString() throws JMSException
    {
        this.throwExceptionIfNotReadable();
        String value =
                JMSTypeConversions.getString(this.getBody().get(this.index));
        this.incrementPosition();
        return value;
    }

    public final void reset()
    {
        this.setReadOnly(true);
        this.index = 0;
    }

    public final void writeBoolean(boolean value) throws JMSException
    {
        this.getBody().add(new Boolean(value));
        this.incrementPosition();
    }

    public final void writeByte(byte value) throws JMSException
    {
        this.getBody().add(new Byte(value));
        this.incrementPosition();
    }

    public final void writeBytes(byte[] value) throws JMSException
    {
        this.getBody().add(value);
        this.incrementPosition();
    }

    public final void writeBytes(byte[] value, int offset, int length)
            throws JMSException
    {
        byte[] bytes = new byte[length];
        System.arraycopy(value, offset, bytes, 0, length);
        this.getBody().add(bytes);
        this.incrementPosition();
    }

    public final void writeChar(char value) throws JMSException
    {
        this.getBody().add(new Character(value));
        this.incrementPosition();
    }

    public final void writeDouble(double value) throws JMSException
    {
        this.getBody().add(new Double(value));
        this.incrementPosition();
    }

    public final void writeFloat(float value) throws JMSException
    {
        this.getBody().add(new Float(value));
        this.incrementPosition();
    }

    public final void writeInt(int value) throws JMSException
    {
        this.getBody().add(new Integer(value));
        this.incrementPosition();
    }

    public final void writeLong(long value) throws JMSException
    {
        this.getBody().add(new Long(value));
        this.incrementPosition();
    }

    public final void writeObject(Object value) throws JMSException
    {
        this.getBody().add(value);
        this.incrementPosition();
    }

    public final void writeShort(short value) throws JMSException
    {
        this.getBody().add(new Short(value));
        this.incrementPosition();
    }

    public final void writeString(String value) throws JMSException
    {
        this.getBody().add(value);
        this.incrementPosition();
    }

    private ArrayList getBody()
    {
        return (ArrayList) super.body;
    }

    private void incrementPosition()
    {
        this.index = this.index + 1;
    }

    private void throwExceptionIfNotReadable()
            throws MessageNotReadableException
    {
        if (!this.isReadOnly())
        {
            throw new MessageNotReadableException("The message is in write only mode.");
        }
    }

}