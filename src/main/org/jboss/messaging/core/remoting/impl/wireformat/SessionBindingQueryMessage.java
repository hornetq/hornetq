/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;


/**
 * 
 * A SessionQueueQueryMessage
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionBindingQueryMessage extends AbstractPacket
{
   private final String address;

   public SessionBindingQueryMessage(final String address)
   {
      super(PacketType.SESS_BINDINGQUERY);

      this.address = address;            
   }

   public String getAddress()
   {
      return address;
   }
   
}
