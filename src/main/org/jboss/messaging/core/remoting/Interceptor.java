/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.messaging.core.remoting;

import org.jboss.jms.exception.MessagingJMSException;
import org.jboss.messaging.core.remoting.wireformat.Packet;

/**
 *
 * This is class is a simple way to intercepting server calls on JBoss Messaging.
 * 
 * To Add this interceptor, you have to modify jbm-configuration.xml, or call MinaService.addInterceptor manually.
 * 
 * If you deploy any Interceptor as a POJO on the Microcontainer, MinaService.addInterceptor is called automagically.
 *  
 * @author clebert.suconic@jboss.com
 */
public interface Interceptor
{
   /**
    * If you need to intercept a return value, you could create your own implementation of PacketSender and recover the return value.
    *
    * @param packet
    * @param handler
    * @param sender
    * @return false if the Packet transmission should be interrupted after this call
    */
   void intercept(Packet packet) throws MessagingJMSException;
}
