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
 * Deploying it to the POJO Container (Micro Container) is then all you need to do.
 * 
 * This gives you the option of deploying it on any package you like, as long as you define your Filter.
 * 
 * Example of configuration file:
 * 
 * <code>
      <?xml version="1.0" encoding="UTF-8"?>
      <deployment xmlns="urn:jboss:bean-deployer:2.0">
             <bean name="MyInterceptor" class="a.b.MyClassImplementingPacketFilter"/>
      </deployment>
  </code>
 * 
 * Note: This interface only captures messages from client2Server. If you need to capture server@client calls you should substitute the sender parameter by an inner class.
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
