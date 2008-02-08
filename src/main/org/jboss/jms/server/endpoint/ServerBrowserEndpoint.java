/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005, JBoss Inc., and individual contributors as indicated
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.jboss.jms.server.endpoint;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.CLOSE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_BROWSER_HASNEXTMESSAGE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_BROWSER_NEXTMESSAGE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_BROWSER_RESET;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.jboss.messaging.core.Filter;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.Queue;
import org.jboss.messaging.core.impl.filter.FilterImpl;
import org.jboss.messaging.core.remoting.PacketHandler;
import org.jboss.messaging.core.remoting.PacketSender;
import org.jboss.messaging.core.remoting.wireformat.NullPacket;
import org.jboss.messaging.core.remoting.wireformat.Packet;
import org.jboss.messaging.core.remoting.wireformat.PacketType;
import org.jboss.messaging.core.remoting.wireformat.SessionBrowserHasNextMessageResponseMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionBrowserNextMessageResponseMessage;
import org.jboss.messaging.util.Logger;
import org.jboss.messaging.util.MessagingException;

/**
 * Concrete implementation of BrowserEndpoint.
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * 
 * $Id$
 */
public class ServerBrowserEndpoint
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ServerBrowserEndpoint.class);

   // Static ---------------------------------------------------------------------------------------

   private static boolean trace = log.isTraceEnabled();

   // Attributes -----------------------------------------------------------------------------------

   private final String id;
   private final ServerSessionEndpoint session;
   private final Queue destination;
   private final Filter filter;
   private Iterator iterator;

   // Constructors ---------------------------------------------------------------------------------

   ServerBrowserEndpoint(ServerSessionEndpoint session, String id,
                         Queue destination, String messageFilter) throws Exception
   {     
      this.session = session;
      this.id = id;
      this.destination = destination;

		if (messageFilter != null)
		{	
		   filter = new FilterImpl(messageFilter);
		}
		else
		{
		   filter = null;
		}
   }

   // BrowserEndpoint implementation ---------------------------------------------------------------

   public void reset() throws Exception
   {
      iterator = createIterator();
   }

   public boolean hasNextMessage() throws Exception
   {
      if (iterator == null)
      {
         iterator = createIterator();
      }

      boolean has = iterator.hasNext();

      return has;
   }
   
   public Message nextMessage() throws Exception
   {
      if (iterator == null)
      {
         iterator = createIterator();
      }

      Message r = (Message)iterator.next();

      return r;
   }

   public Message[] nextMessageBlock(int maxMessages) throws Exception
   {
      if (maxMessages < 2)
      {
         throw new IllegalArgumentException("maxMessages must be >=2 otherwise use nextMessage");
      }

      if (iterator == null)
      {
         iterator = createIterator();
      }

      ArrayList messages = new ArrayList(maxMessages);
      int i = 0;
      while (i < maxMessages)
      {
         if (iterator.hasNext())
         {
            Message m = (Message)iterator.next();
            messages.add(m);
            i++;
         }
         else break;
      }		
		return (Message[])messages.toArray(new Message[messages.size()]);	
   }
   
   public void close() throws Exception
   {
      iterator = null;
      
      session.getConnectionEndpoint().getMessagingServer().getRemotingService().getDispatcher().unregister(id);

      session.removeBrowser(id);
      
      log.trace(this + " closed");
   }
           
   // Public ---------------------------------------------------------------------------------------

   public String toString()
   {
      return "BrowserEndpoint[" + id + "]";
   }

   // Package protected ----------------------------------------------------------------------------
   
   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   private Iterator createIterator()
   {
      List<MessageReference> refs = destination.list(filter);
      
      List<Message> msgs = new ArrayList<Message>();
      
      for (MessageReference ref: refs)
      {
         msgs.add(ref.getMessage());
      }
      
      return msgs.iterator();
   }

   public PacketHandler newHandler()
   {
      return new ServerBrowserEndpointHandler();
   }

   // Inner classes --------------------------------------------------------------------------------
   
   private class ServerBrowserEndpointHandler extends ServerPacketHandlerSupport
   {

      public String getID()
      {
         return ServerBrowserEndpoint.this.id;
      }
      
      public Packet doHandle(Packet packet, PacketSender sender) throws Exception
      {
         Packet response = null;

         PacketType type = packet.getType();
         
         if (type == SESS_BROWSER_HASNEXTMESSAGE)
         {
            response = new SessionBrowserHasNextMessageResponseMessage(hasNextMessage());
         }
         else if (type == SESS_BROWSER_NEXTMESSAGE)
         {
            Message message = nextMessage();
            
            response = new SessionBrowserNextMessageResponseMessage(message);
         }
         else if (type == SESS_BROWSER_RESET)
         {
            reset();
         }
         else if (type == CLOSE)
         {
            close();
         }
         else
         {
            throw new MessagingException(MessagingException.UNSUPPORTED_PACKET,
                                         "Unsupported packet " + type);
         }

         // reply if necessary
         if (response == null && packet.isOneWay() == false)
         {
            response = new NullPacket();               
         }            
         
         return response;
      }

      @Override
      public String toString()
      {
         return "ServerBrowserEndpointHandler[id=" + id + "]";
      }
   }
}
