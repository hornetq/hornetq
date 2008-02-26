/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.messaging.core.remoting.impl.wireformat;

public interface Packet
{
   // Public --------------------------------------------------------

   void setCorrelationID(long correlationID);

   long getCorrelationID();

   PacketType getType();

   String getTargetID();

   void setTargetID(String targetID);

   void setCallbackID(String callbackID);

   String getCallbackID();

   void setOneWay(boolean oneWay);

   boolean isOneWay();
   
   void normalize(Packet other);

   /**
    * An AbstractPacket is a request if it has a target ID and a correlation ID
    */
   public boolean isRequest();


}
