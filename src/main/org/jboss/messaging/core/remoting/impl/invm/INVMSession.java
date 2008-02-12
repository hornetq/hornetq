/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.invm;

import static java.util.UUID.randomUUID;

import java.util.concurrent.TimeUnit;

import org.jboss.messaging.core.remoting.NIOSession;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.PacketSender;
import org.jboss.messaging.core.remoting.wireformat.AbstractPacket;
import org.jboss.messaging.core.remoting.wireformat.Packet;
import org.jboss.messaging.util.Logger;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class INVMSession implements NIOSession
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private String id;
   private long correlationCounter;
   private PacketDispatcher serverDispatcher;
   private boolean connected;
   
   // Static --------------------------------------------------------
   private static final Logger log = Logger.getLogger(INVMSession.class);

   // Constructors --------------------------------------------------

   public INVMSession(PacketDispatcher serverDispatcher)
   {
      assert serverDispatcher != null;
      
      this.id = randomUUID().toString();
      this.correlationCounter = 0;
      this.serverDispatcher = serverDispatcher;
      connected = true;
   }

   // Public --------------------------------------------------------

   public boolean close()
   {
      connected = false;
      return true;
   }

   // NIOSession implementation -------------------------------------

   public String getID()
   {
      return id;
   }

   public boolean isConnected()
   {
      return connected;
   }

   public void write(final Object object) throws Exception
   {
      assert object instanceof AbstractPacket;

      serverDispatcher.dispatch((AbstractPacket) object,
            new PacketSender()
            {
               public void send(Packet response) throws Exception
               {                  
                  serverDispatcher.callFilters(response);
                  PacketDispatcher.client.dispatch(response, null);   
               }
               
               public String getSessionID()
               {
                  return getID();
               }
            });
   }

   public Object writeAndBlock(final AbstractPacket request, long timeout, TimeUnit timeUnit) throws Exception
   {
      request.setCorrelationID(correlationCounter++);
      final Packet[] responses = new Packet[1];

      serverDispatcher.dispatch(request,
            new PacketSender()
            {
               public void send(Packet response)
               {
                  try
                  {
                     serverDispatcher.callFilters(response);
                     // 1st response is used to reply to the blocking request
                     if (responses[0] == null)
                     {
                        responses[0] = response;
                     } else 
                     // other later responses are dispatched directly to the client
                     {
                        PacketDispatcher.client.dispatch(response, null);
                     }
                  }
                  catch (Exception e)
                  {
                     log.warn("An interceptor throwed an exception what caused the packet " + response + " to be ignored", e);
                     responses[0] = null;
                  }
               }

               public String getSessionID()
               {
                  return getID();
               }
            });

      if (responses[0] == null)
      {
         throw new IllegalStateException("No response received for request " + request);
      }

      return responses[0];
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
