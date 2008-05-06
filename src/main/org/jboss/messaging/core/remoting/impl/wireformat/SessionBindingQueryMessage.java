/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;

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
   private final SimpleString address;

   public SessionBindingQueryMessage(final SimpleString address)
   {
      super(PacketType.SESS_BINDINGQUERY);

      this.address = address;            
   }

   public SimpleString getAddress()
   {
      return address;
   }
   
}
