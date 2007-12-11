/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.core.remoting.integration;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static junit.framework.Assert.fail;
import static org.jboss.messaging.core.remoting.wireformat.AbstractPacket.NO_ID_SET;
import static org.jboss.test.messaging.core.remoting.integration.TestSupport.reverse;

import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.jboss.messaging.core.remoting.PacketSender;
import org.jboss.messaging.core.remoting.wireformat.AbstractPacket;
import org.jboss.messaging.core.remoting.wireformat.TextPacket;
import org.jboss.test.messaging.core.remoting.TestPacketHandler;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class ReversePacketHandler extends TestPacketHandler
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private int sleepTime;
   private TimeUnit timeUnit;
   private PacketSender lastSender;
 
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void setSleepTime(int sleepTime, TimeUnit timeUnit)
   {
      this.sleepTime = sleepTime;
      this.timeUnit = timeUnit;
   }
   
   public PacketSender getLastSender()
   {
      return lastSender;
   }
   
   // TestPacketHandler overrides -----------------------------------
   
   @Override
   protected void doHandle(AbstractPacket packet, PacketSender sender)
   {
      Assert.assertTrue(packet instanceof TextPacket);

      lastSender = sender;

      if (sleepTime > 0)
      {
         try
         {
            Thread.sleep(MILLISECONDS.convert(sleepTime, timeUnit));
         } catch (InterruptedException e)
         {
            fail();
         }
      }
      
      TextPacket message = (TextPacket) packet;
      if (message.isRequest() || message.getCallbackID() != NO_ID_SET)
      {
         TextPacket response = new TextPacket(reverse(message.getText()));
         response.normalize(message);
         sender.send(response);
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
