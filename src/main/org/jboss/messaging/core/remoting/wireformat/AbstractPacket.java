/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

import static org.jboss.messaging.core.remoting.Assert.assertValidID;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class AbstractPacket
{
   // Constants -----------------------------------------------------

   public static final String NO_ID_SET = "NO_ID_SET";

   public static final long NO_CORRELATION_ID = -1L;

   public static final byte NO_VERSION_SET = (byte)-1;

   // Attributes ----------------------------------------------------

   private byte version = NO_VERSION_SET;

   private long correlationID = NO_CORRELATION_ID;

   private String targetID = NO_ID_SET;

   private String callbackID = NO_ID_SET;

   private final PacketType type;

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

   public void setVersion(byte version)
   {
      assert version != NO_VERSION_SET;

      this.version = version;
   }

   public byte getVersion()
   {
      return version;
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

   public void normalize(AbstractPacket other)
   {
      assert other != null;

      setVersion(other.getVersion());
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
      return "PACKET[type=" + type + ", version=" + version
            + ", correlationID=" + correlationID + ", targetID=" + targetID
            + ", callbackID=" + callbackID;
   }

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
