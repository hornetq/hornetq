/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.invm;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.PacketReturner;
import org.jboss.messaging.core.remoting.RemotingSession;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class INVMSession implements RemotingSession
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final long id;
   private long correlationCounter;
   private final PacketDispatcher clientDispatcher;
   private final PacketDispatcher serverDispatcher;
   private boolean connected;
   private ExecutorService executor = Executors.newSingleThreadExecutor();
   
   // Static --------------------------------------------------------
   private static final Logger log = Logger.getLogger(INVMSession.class);

   // Constructors --------------------------------------------------

   public INVMSession(final long id, final PacketDispatcher clientDispatcher, final PacketDispatcher serverDispatcher)
   {
      assert clientDispatcher != null;
      assert serverDispatcher != null;
      
      this.id = id;
      this.correlationCounter = 0;
      this.clientDispatcher = clientDispatcher;
      this.serverDispatcher = serverDispatcher;
      connected = true;
   }

   // Public --------------------------------------------------------

   public boolean close()
   {
      executor.shutdownNow();
      connected = false;
      return true;
   }

   // NIOSession implementation -------------------------------------

   public long getID()
   {
      return id;
   }

   public boolean isConnected()
   {
      return connected;
   }

   public void write(final Packet packet) throws Exception
   {
     // assert packet instanceof Packet;

      //Must be executed on different thread
      
      executor.execute(new Runnable()
      {
         public void run()
         {
            try
            {
               serverDispatcher.dispatch((Packet) packet,
                     new PacketReturner()
                     {
                        public void send(Packet response) throws Exception
                        {                  
                           serverDispatcher.callFilters(response);
                           clientDispatcher.dispatch(response, null);   
                        }
                        
                        public long getSessionID()
                        {
                           return getID();
                        }
                        
                        public String getRemoteAddress()
                        {
                           return "invm";
                        }
                     });
            }
            catch (Exception e)
            {
               log.error("Failed to execute dispatch", e);
            }
         }
      });
            
      
   }

//   public Object writeAndBlock(final Packet request, long timeout, TimeUnit timeUnit) throws Exception
//   {
//      request.setCorrelationID(correlationCounter++);
//      final Packet[] responses = new Packet[1];
//
//      serverDispatcher.dispatch(request,
//            new PacketReturner()
//            {
//               public void send(Packet response)
//               {
//                  try
//                  {
//                     serverDispatcher.callFilters(response);
//                     // 1st response is used to reply to the blocking request
//                     if (responses[0] == null)
//                     {
//                        responses[0] = response;
//                     } else 
//                     // other later responses are dispatched directly to the client
//                     {
//                        clientDispatcher.dispatch(response, null);
//                     }
//                  }
//                  catch (Exception e)
//                  {
//                     log.warn("An interceptor throwed an exception what caused the packet " + response + " to be ignored", e);
//                     responses[0] = null;
//                  }
//               }
//
//               public long getSessionID()
//               {
//                  return getID();
//               }
//               
//               public String getRemoteAddress()
//               {
//                  return "invm";
//               }
//            });
//
//      if (responses[0] == null)
//      {
//         throw new IllegalStateException("No response received for request " + request);
//      }
//
//      return responses[0];
//   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
