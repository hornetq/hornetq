/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MessageNotReadableException;
import javax.jms.MessageNotWriteableException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author <a href="mailto:nathan@jboss.org">Nathan Phelps</a>
 * @version $Revision$ $Date$
 */
public class BytesMessageImpl extends MessageImpl implements BytesMessage
{

    private ByteArrayOutputStream outputStream = null;

    public BytesMessageImpl()
    {
        super.type = MessageImpl.BYTES_MESSAGE_NAME;
        this.outputStream = new ByteArrayOutputStream();
        super.body = new DataOutputStream(this.outputStream);
    }

    public long getBodyLength() throws JMSException
    {
        return 0;
    }

    private DataInputStream getReadableBody()
            throws MessageNotReadableException
    {
        if (!this.isReadOnly())
        {
            throw new MessageNotReadableException(
                    "Unable to read message body. The message body"
                    + "is currently marked as write only.");
        }
        return (DataInputStream) super.body;
    }

    private DataOutputStream getWritableBody()
            throws MessageNotWriteableException
    {
        super.throwExceptionIfReadOnly();
        return (DataOutputStream) super.body;
    }

    public boolean readBoolean() throws JMSException
    {
        try
        {
            return this.getReadableBody().readBoolean();
        }
        catch (IOException exception)
        {
            throw new JMSException("");
            //TOD: Decide what exception should be thrown here.
        }
    }

    public byte readByte() throws JMSException
    {
        try
        {
            return this.getReadableBody().readByte();
        }
        catch (IOException exception)
        {
            throw new JMSException("");
            //TOD: Decide what exception should be thrown here.
        }
    }

    public int readBytes(byte[] value) throws JMSException
    {
        try
        {
            return this.getReadableBody().read(value);
        }
        catch (IOException exception)
        {
            throw new JMSException("");
            //TOD: Decide what exception should be thrown here.
        }
    }

    public int readBytes(byte[] value, int length) throws JMSException
    {
        try
        {
            return this.getReadableBody().read(value, 0, length);
            //TOD: check this.
        }
        catch (IOException exception)
        {
            throw new JMSException("");
            //TOD: Decide what exception should be thrown here.
        }
    }

    public char readChar() throws JMSException
    {
        try
        {
            return this.getReadableBody().readChar();
        }
        catch (IOException exception)
        {
            throw new JMSException("");
            //TOD: Decide what exception should be thrown here.
        }
    }

    public double readDouble() throws JMSException
    {
        try
        {
            return this.getReadableBody().readDouble();
        }
        catch (IOException exception)
        {
            throw new JMSException("");
            //TOD: Decide what exception should be thrown here.
        }
    }

    public float readFloat() throws JMSException
    {
        try
        {
            return this.getReadableBody().readFloat();
        }
        catch (IOException exception)
        {
            throw new JMSException("");
            //TOD: Decide what exception should be thrown here.
        }
    }

    public int readInt() throws JMSException
    {
        try
        {
            return this.getReadableBody().readInt();
        }
        catch (IOException exception)
        {
            throw new JMSException("");
            //TOD: Decide what exception should be thrown here.
        }
    }

    public long readLong() throws JMSException
    {
        try
        {
            return this.getReadableBody().readLong();
        }
        catch (IOException exception)
        {
            throw new JMSException("");
            //TOD: Decide what exception should be thrown here.
        }
    }

    public short readShort() throws JMSException
    {
        try
        {
            return this.getReadableBody().readShort();
        }
        catch (IOException exception)
        {
            throw new JMSException("");
            //TOD: Decide what exception should be thrown here.
        }
    }

    public int readUnsignedByte() throws JMSException
    {
        try
        {
            return this.getReadableBody().readUnsignedByte();
        }
        catch (IOException exception)
        {
            throw new JMSException("");
            //TOD: Decide what exception should be thrown here.
        }
    }

    public int readUnsignedShort() throws JMSException
    {
        try
        {
            return this.getReadableBody().readUnsignedShort();
        }
        catch (IOException exception)
        {
            throw new JMSException("");
            //TOD: Decide what exception should be thrown here.
        }
    }

    public String readUTF() throws JMSException
    {
        try
        {
            return this.getReadableBody().readUTF();
        }
        catch (IOException exception)
        {
            throw new JMSException("");
            //TOD: Decide what exception should be thrown here.
        }
    }

    public void reset() throws JMSException
    {
        this.setReadOnly(true);
        super.body =
                new DataInputStream(
                        new ByteArrayInputStream(this.outputStream.toByteArray()));
        try
        {
            this.outputStream.close();
        }
        catch (IOException exception)
        {
            //TOD: Decide what exception should be thrown here.
        }
        finally
        {
            this.outputStream = null;
        }
    }

    public void writeBoolean(boolean value) throws JMSException
    {
        try
        {
            this.getWritableBody().writeBoolean(value);
        }
        catch (IOException exception)
        {
            //TOD: Decide what exception should be thrown here.
        }
    }

    public void writeByte(byte value) throws JMSException
    {
        try
        {
            this.getWritableBody().writeByte(value);
        }
        catch (IOException exception)
        {
            //TOD: Decide what exception should be thrown here.
        }
    }

    public void writeBytes(byte[] value) throws JMSException
    {
        try
        {
            this.getWritableBody().write(value);
        }
        catch (IOException exception)
        {
            //TOD: Decide what exception should be thrown here.
        }
    }

    public void writeBytes(byte[] value, int offset, int length)
            throws JMSException
    {
        try
        {
            this.getWritableBody().write(value, offset, length);
        }
        catch (IOException exception)
        {
            //TOD: Decide what exception should be thrown here.
        }
    }

    public void writeChar(char value) throws JMSException
    {
        try
        {
            this.getWritableBody().writeChar(value);
        }
        catch (IOException exception)
        {
            //TOD: Decide what exception should be thrown here.
        }
    }

    public void writeDouble(double value) throws JMSException
    {
        try
        {
            this.getWritableBody().writeDouble(value);
        }
        catch (IOException exception)
        {
            //TOD: Decide what exception should be thrown here.
        }
    }

    public void writeFloat(float value) throws JMSException
    {
        try
        {
            this.getWritableBody().writeFloat(value);
        }
        catch (IOException exception)
        {
            //TOD: Decide what exception should be thrown here.
        }
    }

    public void writeInt(int value) throws JMSException
    {
        try
        {
            this.getWritableBody().writeInt(value);
        }
        catch (IOException exception)
        {
            //TOD: Decide what exception should be thrown here.
        }
    }

    public void writeLong(long value) throws JMSException
    {
        try
        {
            this.getWritableBody().writeLong(value);
        }
        catch (IOException exception)
        {
            //TOD: Decide what exception should be thrown here.
        }
    }

    public void writeObject(Object value) throws JMSException
    {
        if (value instanceof Boolean)
        {
            this.writeBoolean(((Boolean) value).booleanValue());
        }
        else if (value instanceof Byte)
        {
            this.writeByte(((Byte) value).byteValue());
        }
        else if (value instanceof Byte[])
        {
            this.writeBytes((byte[]) value);
        }
        else if (value instanceof Character)
        {
            this.writeChar(((Character) value).charValue());
        }
        else if (value instanceof Double)
        {
            this.writeDouble(((Double) value).doubleValue());
        }
        else if (value instanceof Float)
        {
            this.writeFloat(((Float) value).floatValue());
        }
        else if (value instanceof Integer)
        {
            this.writeInt(((Integer) value).intValue());
        }
        else if (value instanceof Long)
        {
            this.writeLong(((Long) value).longValue());
        }
        else if (value instanceof Short)
        {
            this.writeShort(((Short) value).shortValue());
        }
        else if (value instanceof String)
        {
            this.writeUTF((String) value);
        }
        else
        {
            // TOD: throw excption here...
        }
    }

    public void writeShort(short value) throws JMSException
    {
        try
        {
            this.getWritableBody().writeShort(value);
        }
        catch (IOException exception)
        {
            //TOD: Decide what exception should be thrown here.
        }
    }

    public void writeUTF(String value) throws JMSException
    {
        try
        {
            this.getWritableBody().writeUTF(value);
        }
        catch (IOException exception)
        {
            //TOD: Decide what exception should be thrown here.
        }
    }

    public void clearBody()
    {
        this.setReadOnly(false);
        this.outputStream = new ByteArrayOutputStream();
        this.body = super.body = new DataOutputStream(this.outputStream);
    }

}