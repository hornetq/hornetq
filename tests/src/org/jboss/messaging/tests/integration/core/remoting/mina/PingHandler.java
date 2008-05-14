/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.tests.integration.core.remoting.mina;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static junit.framework.Assert.fail;

import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.PacketReturner;
import org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket;
import org.jboss.messaging.core.remoting.impl.wireformat.Ping;
import org.jboss.messaging.core.remoting.impl.wireformat.Pong;
import org.jboss.messaging.tests.unit.core.remoting.TestPacketHandler;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class PingHandler extends TestPacketHandler
{
   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(PingHandler.class);
   

   // Attributes ----------------------------------------------------

   private int sleepTime;
   private TimeUnit timeUnit;
   private PacketReturner lastSender;
 
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------
   
   public PingHandler(final long id)
   {
   	super(id);
   }

   // Public --------------------------------------------------------

   public void setSleepTime(int sleepTime, TimeUnit timeUnit)
   {
      this.sleepTime = sleepTime;
      this.timeUnit = timeUnit;
   }
   
   public PacketReturner getLastSender()
   {
      return lastSender;
   }
   
   // TestPacketHandler overrides -----------------------------------
   
   protected boolean isRequest(final Packet packet)
   {
      return packet.getTargetID() != EmptyPacket.NO_ID_SET && packet.getResponseTargetID() != EmptyPacket.NO_ID_SET;
   }
   
   @Override
   protected void doHandle(Packet packet, PacketReturner sender)
   {
      Assert.assertTrue(packet instanceof Ping);

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
                  
      Ping message = (Ping) packet;
      if (isRequest(message))
      {
         Pong response = new Pong(message.getSessionID(), true);
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
