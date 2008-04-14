/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;

import org.jboss.messaging.core.client.impl.RemotingConnection;
import org.jboss.messaging.core.remoting.Packet;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class PacketImpl implements Packet
{
	// Constants -----------------------------------------------------

	public static final long NO_ID_SET = -1L;

	// Attributes ----------------------------------------------------

	private long correlationID = NO_ID_SET;

	private long targetID = NO_ID_SET;

	private long executorID = NO_ID_SET;

	private final PacketType type;

	/**
	 * <code>oneWay</code> is <code>true</code> when the packet is sent "one way"
	 * by the client which does not expect any response to it.
	 * 
	 * @see RemotingConnection#sendOneWay(AbstractPacket)
	 */
	private boolean oneWay = false;

	// Static --------------------------------------------------------

	// Constructors --------------------------------------------------

	public PacketImpl(PacketType type)
	{
		assert type != null;

		this.type = type;
	}

	// Public --------------------------------------------------------

	public PacketType getType()
	{
		return type;
	}

	public void setCorrelationID(long correlationID)
	{
		this.correlationID = correlationID;
	}

	public long getCorrelationID()
	{
		return correlationID;
	}

	public long getTargetID()
	{
		return targetID;
	}

	public void setTargetID(long targetID)
	{
		this.targetID = targetID;
	}

	public long getExecutorID()
	{
		return executorID;
	}

	public void setExecutorID(long executorID)
	{
		this.executorID = executorID;
	}

	public void setOneWay(boolean oneWay)
	{
		this.oneWay = oneWay;
	}

	public boolean isOneWay()
	{
		return oneWay;
	}

	public void normalize(Packet other)
	{
		assert other != null;

		setCorrelationID(other.getCorrelationID());
	}

	/**
	 * An AbstractPacket is a request if it has a target ID and a correlation ID
	 */
	 public boolean isRequest()
	 {
		 return targetID != NO_ID_SET && correlationID != NO_ID_SET;
	 }

	 @Override
	 public String toString()
	 {
		 return getParentString() + "]";
	 }

	 // Package protected ---------------------------------------------

	 protected String getParentString()
	 {
		 return "PACKET[type=" + type
		 + ", correlationID=" + correlationID + ", targetID=" + targetID
		 + ", executorID=" + executorID + ", oneWay=" + oneWay;
	 }

	 // Protected -----------------------------------------------------

	 // Private -------------------------------------------------------

	 // Inner classes -------------------------------------------------

}
