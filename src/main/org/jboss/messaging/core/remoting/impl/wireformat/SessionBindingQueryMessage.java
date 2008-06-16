/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;

import org.jboss.messaging.util.MessagingBuffer;
import org.jboss.messaging.util.SimpleString;


/**
 * 
 * A SessionQueueQueryMessage
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionBindingQueryMessage extends PacketImpl
{
   private SimpleString address;

   public SessionBindingQueryMessage(final SimpleString address)
   {
      super(SESS_BINDINGQUERY);

      this.address = address;            
   }
   
   public SessionBindingQueryMessage()
   {
      super(SESS_BINDINGQUERY);          
   }

   public SimpleString getAddress()
   {
      return address;
   }
   
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.putSimpleString(address);
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      address = buffer.getSimpleString();
   }
   
   public boolean equals(Object other)
   {
      if (other instanceof SessionBindingQueryMessage == false)
      {
         return false;
      }
            
      SessionBindingQueryMessage r = (SessionBindingQueryMessage)other;
      
      return this.address.equals(r.address);
   }
   
}
