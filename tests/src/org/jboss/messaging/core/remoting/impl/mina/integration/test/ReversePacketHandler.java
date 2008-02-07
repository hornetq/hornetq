/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.mina.integration.test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static junit.framework.Assert.fail;
import static org.jboss.messaging.core.remoting.impl.mina.integration.test.TestSupport.reverse;
import static org.jboss.messaging.core.remoting.wireformat.AbstractPacket.NO_ID_SET;

import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.jboss.messaging.core.remoting.PacketSender;
import org.jboss.messaging.core.remoting.test.unit.TestPacketHandler;
import org.jboss.messaging.core.remoting.wireformat.Packet;
import org.jboss.messaging.core.remoting.wireformat.TextPacket;
import org.jboss.messaging.util.Logger;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class ReversePacketHandler extends TestPacketHandler
{
   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(ReversePacketHandler.class);
   

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
   protected void doHandle(Packet packet, PacketSender sender)
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
      if (message.isRequest() || !NO_ID_SET.equals(message.getCallbackID()))
      {
         TextPacket response = new TextPacket(reverse(message.getText()));
         response.normalize(message);
         try
         {
            sender.send(response);
         }
         catch (Exception e)
         {
            log.error("Failed to handle", e);
         }
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
