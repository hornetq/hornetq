package org.jboss.jms.server.endpoint;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.remoting.PacketHandler;
import org.jboss.messaging.core.remoting.PacketSender;
import org.jboss.messaging.core.remoting.wireformat.MessagingExceptionMessage;
import org.jboss.messaging.core.remoting.wireformat.Packet;
import org.jboss.messaging.util.MessagingException;

/**
 * 
 * A ServerPacketHandlerSupport
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public abstract class ServerPacketHandlerSupport implements PacketHandler
{
   private static final Logger log = Logger.getLogger(ServerPacketHandlerSupport.class);
   
   
   public void handle(Packet packet, PacketSender sender)
   {
      Packet response;
      
      try
      {      
         response = doHandle(packet, sender);
      }
      catch (Exception e)
      {
         MessagingException me;
         
         if (e instanceof MessagingException)
         {
            me = (MessagingException)e;
         }
         else
         {
            log.error("Caught unexpected exception", e);
            
            me = new MessagingException(MessagingException.INTERNAL_ERROR);
         }
                  
         response = new MessagingExceptionMessage(me);         
      }
      
      // reply if necessary
      if (response != null && !packet.isOneWay())
      {
         response.normalize(packet);
         
         try
         {
            sender.send(response);
         }
         catch (Exception e)
         {
            log.error("Failed to send packet", e);
         }
      }
   }
   
   protected abstract Packet doHandle(Packet packet, PacketSender sender) throws Exception;

}
