/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005, JBoss Inc., and individual contributors as indicated
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.jboss.messaging.core.message;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.jboss.logging.Logger;
import org.jboss.messaging.util.StreamUtils;

/**
 * A message base.
 * 
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 * 
 * $Id$
 */
public abstract class MessageSupport implements Message
{
	// Constants -----------------------------------------------------

	private static final Logger log = Logger.getLogger(MessageSupport.class);

	// Attributes ----------------------------------------------------

	private boolean trace = log.isTraceEnabled();

	protected long messageID;

	protected boolean reliable;

	/** GMT milliseconds at which this message expires. 0 means never expires * */
	protected long expiration;

	protected long timestamp;

	protected Map headers;

	protected byte priority;

	// Must be hidden from subclasses
	private transient Object payload;

	// Must be hidden from subclasses
	private byte[] payloadAsByteArray;

	private transient boolean persisted;

	// Constructors --------------------------------------------------

	/*
	 * Construct a message for deserialization or streaming
	 */
	public MessageSupport()
	{
	}

	/*
	 * Construct a message using default values
	 */
	public MessageSupport(long messageID)
	{
		this(messageID, false, 0, System.currentTimeMillis(), (byte) 4, null,
				null);
	}

	/*
	 * Construct a message using specified values
	 */
	public MessageSupport(long messageID, boolean reliable, long expiration,
			long timestamp, byte priority, Map headers, byte[] payloadAsByteArray)
	{
		this.messageID = messageID;
		this.reliable = reliable;
		this.expiration = expiration;
		this.timestamp = timestamp;
		this.priority = priority;
		if (headers == null)
		{
			this.headers = new HashMap();
		} else
		{
			this.headers = new HashMap(headers);
		}

		this.payloadAsByteArray = payloadAsByteArray;
	}

	/*
	 * Copy constructor
	 * 
	 * Does a shallow copy of the payload
	 */
	protected MessageSupport(MessageSupport that)
	{
		this.messageID = that.messageID;
		this.reliable = that.reliable;
		this.expiration = that.expiration;
		this.timestamp = that.timestamp;
		this.headers = new HashMap(that.headers);
		this.priority = that.priority;
		this.payload = that.payload;
		this.payloadAsByteArray = that.payloadAsByteArray;
	}

	// Message implementation ----------------------------------------

	public long getMessageID()
	{
		return messageID;
	}

	public boolean isReliable()
	{
		return reliable;
	}

	public long getExpiration()
	{
		return expiration;
	}

	public void setExpiration(long expiration)
	{
		this.expiration = expiration;
	}

	public long getTimestamp()
	{
		return timestamp;
	}

	public Object putHeader(String name, Object value)
	{
		return headers.put(name, value);
	}

	public Object getHeader(String name)
	{
		return headers.get(name);
	}

	public Object removeHeader(String name)
	{
		return headers.remove(name);
	}

	public boolean containsHeader(String name)
	{
		return headers.containsKey(name);
	}

	public Set getHeaderNames()
	{
		return headers.keySet();
	}

	public Map getHeaders()
	{
		return headers;
	}

	public void setHeaders(Map headers)
	{
		this.headers = headers;
	}

	public byte getPriority()
	{
		return priority;
	}

	public void setPriority(byte priority)
	{
		this.priority = priority;
	}

	public boolean isReference()
	{
		return false;
	}

	public synchronized byte[] getPayloadAsByteArray()
	{
		if (payloadAsByteArray == null && payload != null)
		{
			// convert the payload into a byte array and store internally

			// TODO - investigate how changing the buffer size effects
			// performance

			// Ideally I would like to use the pre-existing DataOutputStream and
			// not create another one - but would then have to add markers on the
			// stream
			// to signify the end of the payload
			// This would have the advantage of us not having to allocate buffers
			// here
			// We could do this by creating our own FilterOutputStream that makes
			// sure
			// the end of marker sequence doesn't occur in the payload

			final int BUFFER_SIZE = 2048;

			try
			{
				ByteArrayOutputStream bos = new ByteArrayOutputStream(BUFFER_SIZE);
				DataOutputStream daos = new DataOutputStream(bos);
				writePayload(daos, payload);
				daos.close();
				payloadAsByteArray = bos.toByteArray();
				payload = null;
			} catch (Exception e)
			{
				RuntimeException e2 = new RuntimeException(e.getMessage());
				e2.setStackTrace(e.getStackTrace());
				throw e2;
			}
		}
		return payloadAsByteArray;
	}

	/**
	 * Warning! Calling getPayload will cause the payload to be deserialized so
	 * should not be called on the server.
	 */
	public synchronized Object getPayload()
	{
		if (payload != null)
		{
			return payload;
		} else if (payloadAsByteArray != null)
		{
			// deserialize the payload from byte[]

			// TODO use the same DataInputStream as in the read() method and
			// add markers on the stream to represent end of payload
			ByteArrayInputStream bis = new ByteArrayInputStream(payloadAsByteArray);
			DataInputStream dis = new DataInputStream(bis);
			try
			{
				payload = readPayload(dis, payloadAsByteArray.length);
			} catch (Exception e)
			{
				RuntimeException e2 = new RuntimeException(e.getMessage());
				e2.setStackTrace(e.getStackTrace());
				throw e2;
			}

			payloadAsByteArray = null;
			return payload;
		} else
		{
			return null;
		}
	}

	public void setPayload(Serializable payload)
	{
		this.payload = payload;
	}

	protected void clearPayloadAsByteArray()
	{
		this.payloadAsByteArray = null;
	}

	public synchronized boolean isPersisted()
	{
		return persisted;
	}

	public synchronized void setPersisted(boolean persisted)
	{
		this.persisted = persisted;
	}

	public boolean isExpired()
	{
		if (expiration == 0)
		{
			return false;
		}
		long overtime = System.currentTimeMillis() - expiration;
		if (overtime >= 0)
		{
			// discard it
			if (trace)
			{
				log.trace(this + " expired by " + overtime + " ms");
			}

			return true;
		}
		return false;
	}

	// Public --------------------------------------------------------

	public boolean equals(Object o)
	{
		if (this == o)
		{
			return true;
		}
		if (!(o instanceof MessageSupport))
		{
			return false;
		}
		MessageSupport that = (MessageSupport) o;
		return that.messageID == this.messageID;
	}

	public int hashCode()
	{
		return (int) ((this.messageID >>> 32) ^ this.messageID);
	}

	public String toString()
	{
		return "M[" + messageID + "]";
	}

	// Streamable implementation ---------------------------------

	public void write(DataOutputStream out) throws Exception
	{
		out.writeLong(messageID);

		out.writeBoolean(reliable);

		out.writeLong(expiration);

		out.writeLong(timestamp);

		StreamUtils.writeMap(out, headers, true);

		out.writeByte(priority);

		byte[] bytes = getPayloadAsByteArray();

		if (bytes != null)
		{
			out.writeInt(bytes.length);

			out.write(bytes);
		} else
		{
			out.writeInt(0);
		}
	}

	public void read(DataInputStream in) throws Exception
	{
		messageID = in.readLong();

		reliable = in.readBoolean();

		expiration = in.readLong();

		timestamp = in.readLong();

		headers = StreamUtils.readMap(in, true);

		priority = in.readByte();

		int length = in.readInt();

		if (length == 0)
		{
			// no payload
			payloadAsByteArray = null;
		} else
		{
			payloadAsByteArray = new byte[length];

			in.readFully(payloadAsByteArray);
		}
	}

	// Package protected ---------------------------------------------

	// Protected -----------------------------------------------------

	/**
	 * Override this if you want more sophisticated payload externalization.
	 * 
	 * @throws Exception
	 *            TODO
	 */
	protected void writePayload(DataOutputStream out, Object thePayload)
			throws Exception
	{
		StreamUtils.writeObject(out, thePayload, true, true);
	}

	/**
	 * Override this if you want more sophisticated payload externalization.
	 * 
	 * @throws Exception
	 *            TODO
	 */
	protected Object readPayload(DataInputStream in, int length)
			throws Exception
	{
		return StreamUtils.readObject(in, true);
	}

	/**
	 * It makes sense to use this method only from within JBossBytesMessage
	 * (optimization). Using it from anywhere else will lead to corrupted data.
	 */
	protected final void copyPayloadAsByteArrayToPayload()
	{
		payload = payloadAsByteArray;
	}

	// Private -------------------------------------------------------

	// Inner classes -------------------------------------------------
}
