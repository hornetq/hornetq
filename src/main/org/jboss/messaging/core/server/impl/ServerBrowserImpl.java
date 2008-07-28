/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.core.server.impl;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.CLOSE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.NO_ID_SET;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.NULL;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_BROWSER_HASNEXTMESSAGE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_BROWSER_NEXTMESSAGE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_BROWSER_RESET;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.filter.impl.FilterImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.PacketHandler;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.wireformat.MessagingExceptionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.ReceiveMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBrowserHasNextMessageResponseMessage;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.server.ServerSession;
import org.jboss.messaging.util.SimpleString;

/**
 * Concrete implementation of BrowserEndpoint.
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision: 3778 $</tt>
 * 
 * $Id: ServerBrowserImpl.java 3778 2008-02-24 12:15:29Z timfox $
 */
public class ServerBrowserImpl
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ServerBrowserImpl.class);

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private final long id;
   private final ServerSession session;
   private final Queue destination;
   private final Filter filter;
   private Iterator<ServerMessage> iterator;
   private final RemotingConnection remotingConnection;
   
   // Constructors ---------------------------------------------------------------------------------

   public ServerBrowserImpl(final ServerSession session,
                            final Queue destination, final String messageFilter,
                            final PacketDispatcher dispatcher,
                            final RemotingConnection remotingConnection) throws MessagingException
   {     
      this.session = session;
      
      this.id = dispatcher.generateID();
      
      this.destination = destination;

		if (messageFilter != null)
		{	
		   filter = new FilterImpl(new SimpleString(messageFilter));
		}
		else
		{
		   filter = null;
		}
		
		this.remotingConnection = remotingConnection;
   }

   // BrowserEndpoint implementation ---------------------------------------------------------------

   public long getID()
   {
   	return id;
   }
   
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
   
   public ServerMessage nextMessage() throws Exception
   {
      if (iterator == null)
      {
         iterator = createIterator();
      }

      ServerMessage r = iterator.next();

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

      List<ServerMessage> messages = new ArrayList<ServerMessage>(maxMessages);
      int i = 0;
      while (i < maxMessages)
      {
         if (iterator.hasNext())
         {
            ServerMessage m = iterator.next();
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
      
      session.removeBrowser(this);
      
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

   private Iterator<ServerMessage> createIterator()
   {
      List<MessageReference> refs = destination.list(filter);
      
      List<ServerMessage> msgs = new ArrayList<ServerMessage>();
      
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
   
   private class ServerBrowserEndpointHandler implements PacketHandler
   {
      public long getID()
      {
         return ServerBrowserImpl.this.id;
      }
      
      public void handle(final long connectionID, final Packet packet)
      {
         Packet response = null;

         try
         {
            byte type = packet.getType();
            switch (type)
            {
            case SESS_BROWSER_HASNEXTMESSAGE:
               response = new SessionBrowserHasNextMessageResponseMessage(hasNextMessage());            
               break;
            case SESS_BROWSER_NEXTMESSAGE:
               ServerMessage message = nextMessage();               
               response = new ReceiveMessage(message, 0, 0);
               break;
            case SESS_BROWSER_RESET:            
               reset();
               response = new PacketImpl(NULL); 
               break;
            case CLOSE:
               close();
               response = new PacketImpl(NULL); 
               break;
            default:
               response = new MessagingExceptionMessage(new MessagingException(MessagingException.UNSUPPORTED_PACKET,
                     "Unsupported packet " + type));
            }
         }
         catch (Throwable t)
         {
            MessagingException me;
            
            log.error("Caught unexpected exception", t);         
            
            if (t instanceof MessagingException)
            {
               me = (MessagingException)t;
            }
            else
            {            
               me = new MessagingException(MessagingException.INTERNAL_ERROR);
            }
                     
            response = new MessagingExceptionMessage(me);    
         }
         
         response.normalize(packet);
         
         remotingConnection.sendOneWay(response);         
      }
   }
}
