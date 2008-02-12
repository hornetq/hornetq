/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

import static org.jboss.messaging.core.remoting.Assert.assertValidID;

import org.jboss.jms.client.remoting.MessagingRemotingConnection;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class AbstractPacket implements Packet
{
   // Constants -----------------------------------------------------

   public static final String NO_ID_SET = "NO_ID_SET";

   public static final long NO_CORRELATION_ID = -1L;

   // Attributes ----------------------------------------------------

   private long correlationID = NO_CORRELATION_ID;

   private String targetID = NO_ID_SET;

   private String callbackID = NO_ID_SET;

   private final PacketType type;

   /**
    * <code>oneWay</code> is <code>true</code> when the packet is sent "one way"
    * by the client which does not expect any response to it.
    * 
    * @see MessagingRemotingConnection#sendOneWay(AbstractPacket)
    */
   private boolean oneWay = false;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public AbstractPacket(PacketType type)
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

   public String getTargetID()
   {
      return targetID;
   }

   public void setTargetID(String targetID)
   {
      assertValidID(targetID);

      this.targetID = targetID;
   }

   public void setCallbackID(String callbackID)
   {
      assertValidID(callbackID);

      this.callbackID = callbackID;
   }

   public String getCallbackID()
   {
      return callbackID;
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
      setTargetID(other.getCallbackID());
   }

   /**
    * An AbstractPacket is a request if it has a target ID and a correlation ID
    */
   public boolean isRequest()
   {
      return targetID != NO_ID_SET && correlationID != NO_CORRELATION_ID;
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
            + ", callbackID=" + callbackID + ", oneWay=" + oneWay;
   }

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
