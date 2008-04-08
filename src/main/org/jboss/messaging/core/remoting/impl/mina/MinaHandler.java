/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.mina;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import org.apache.mina.common.IoHandlerAdapter;
import org.apache.mina.common.IoSession;
import org.apache.mina.filter.reqres.Response;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.PacketHandlerRegistrationListener;
import org.jboss.messaging.core.remoting.PacketSender;
import org.jboss.messaging.core.remoting.RemotingException;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.Packet;
import org.jboss.messaging.core.remoting.impl.wireformat.Ping;
import org.jboss.messaging.util.OrderedExecutorFactory;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class MinaHandler extends IoHandlerAdapter implements PacketHandlerRegistrationListener
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(MinaHandler.class);

   // Attributes ----------------------------------------------------

   private final PacketDispatcher dispatcher;

   private FailureNotifier failureNotifier;

   private boolean closeSessionOnExceptionCaught;

   private OrderedExecutorFactory executorFactory;

   private Map<String, Executor> executors = new HashMap<String, Executor>();
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------
   public MinaHandler(PacketDispatcher dispatcher, ExecutorService executorService, FailureNotifier failureNotifier, boolean closeSessionOnExceptionCaught)
   {
      assert dispatcher!= null;
      assert executorService != null;
      
      this.dispatcher = dispatcher;
      this.failureNotifier = failureNotifier;
      this.closeSessionOnExceptionCaught = closeSessionOnExceptionCaught;
      
      this.executorFactory = new OrderedExecutorFactory(executorService);
      this.dispatcher.setListener(this);
   }

   // Public --------------------------------------------------------

   // PacketHandlerRegistrationListener implementation --------------

   public void handlerRegistered(String handlerID)
   {
      // do nothing on registration
   }
   
   public void handlerUnregistered(String handlerID)
   {
      executors.remove(handlerID);
   }

   // IoHandlerAdapter overrides ------------------------------------

   @Override
   public void exceptionCaught(IoSession session, Throwable cause)
         throws Exception
   {
      log.error("caught exception " + cause + " for session " + session, cause);
      
      if (failureNotifier != null)
      {
         String serverSessionID = Long.toString(session.getId());
         RemotingException re = new RemotingException(MessagingException.INTERNAL_ERROR, "unexpected exception", serverSessionID);
         re.initCause(cause);
         failureNotifier.fireFailure(re);
      }
      if (closeSessionOnExceptionCaught)
      {
         session.close();
      }
   }
   
   @Override
   public void messageReceived(final IoSession session, final Object message)
         throws Exception
   {
      if (message instanceof Response)
      {
         if (log.isTraceEnabled())
            log.trace("received response " + message);
         // response is handled by the reqres filter.
         // do nothing
         return;
      }
      
      if (message instanceof Ping)
      {
         if (log.isTraceEnabled())
            log.trace("received ping " + message);
         // response is handled by the keep-alive filter.
         // do nothing
         return;
      } 
      
      if (!(message instanceof Packet))
      {
         throw new IllegalArgumentException("Unknown message type: " + message);
      }
      
      final Packet packet = (Packet) message;
      String executorID = packet.getExecutorID();
      
      if (PacketImpl.NO_ID_SET.equals(executorID)) 
         throw new IllegalArgumentException("executor ID not set for " + packet);
      
      Executor executor = executors.get(executorID);
      if (executor == null)
      {
         executor = this.executorFactory.getOrderedExecutor();
         executors.put(executorID, executor);
      }
      
      executor.execute(new Runnable() 
      {
         public void run()
         {
            try
            {
               messageReceived0(session, packet);
            } catch (Exception e)
            {
               log.error("unexpected error", e);
            }
         }
      });
      
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void messageReceived0(final IoSession session, Packet packet)
         throws Exception
   {
      PacketSender sender = new PacketSender()
      {
         public void send(Packet p) throws Exception
         {
            dispatcher.callFilters(p);
            session.write(p);            
         }
         
         public String getSessionID()
         {
            return Long.toString(session.getId());
         }
         
         public String getRemoteAddress()
         {
            return session.getRemoteAddress().toString();
         }
      };

      if (log.isTraceEnabled())
         log.trace("received packet " + packet);

      dispatcher.dispatch(packet, sender);
   }
   
   // Inner classes -------------------------------------------------
}
