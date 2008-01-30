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

import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_BROWSER_RESET;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_CLOSE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_BROWSER_HASNEXTMESSAGE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_BROWSER_NEXTMESSAGE;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.jms.IllegalStateException;
import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;

import org.jboss.jms.exception.MessagingJMSException;
import org.jboss.messaging.core.Filter;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.Queue;
import org.jboss.messaging.core.impl.filter.FilterImpl;
import org.jboss.messaging.core.remoting.PacketHandler;
import org.jboss.messaging.core.remoting.PacketSender;
import org.jboss.messaging.core.remoting.wireformat.AbstractPacket;
import org.jboss.messaging.core.remoting.wireformat.BrowserHasNextMessageResponse;
import org.jboss.messaging.core.remoting.wireformat.BrowserNextMessageResponse;
import org.jboss.messaging.core.remoting.wireformat.JMSExceptionMessage;
import org.jboss.messaging.core.remoting.wireformat.NullPacket;
import org.jboss.messaging.core.remoting.wireformat.Packet;
import org.jboss.messaging.core.remoting.wireformat.PacketType;
import org.jboss.messaging.util.ExceptionUtil;
import org.jboss.messaging.util.Logger;

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

   private String id;
   private boolean closed;
   private ServerSessionEndpoint session;
   private Queue destination;
   private Filter filter;
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
		   try
		   {
		      filter = new FilterImpl(messageFilter);
		   }
		   catch (Exception e)
		   {
		      throw new InvalidSelectorException("Invalid selector " + messageFilter);
		   }
		}
   }

   // BrowserEndpoint implementation ---------------------------------------------------------------

   public void reset() throws JMSException
   {
      try
      {
         if (closed)
         {
            throw new IllegalStateException("Browser is closed");
         }

         log.trace(this + " is being resetted");

         iterator = createIterator();
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " hasNextMessage");
      }
   }

   public boolean hasNextMessage() throws JMSException
   {
      try
      {
         if (closed)
         {
            throw new IllegalStateException("Browser is closed");
         }

         if (iterator == null)
         {
            iterator = createIterator();
         }

         boolean has = iterator.hasNext();
         if (trace) { log.trace(this + (has ? " has": " DOESN'T have") + " a next message"); }
         return has;
      }   
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " hasNextMessage");
      }
   }
   
   public Message nextMessage() throws JMSException
   {
      try
      {
         if (closed)
         {
            throw new IllegalStateException("Browser is closed");
         }

         if (iterator == null)
         {
            iterator = createIterator();
         }

         Message r = (Message)iterator.next();
   
         if (trace) { log.trace(this + " returning " + r); }
         
         return r;
      }   
      catch (Throwable t)
      {
         t.printStackTrace();
         throw ExceptionUtil.handleJMSInvocation(t, this + " nextMessage");
      }
   }

   public Message[] nextMessageBlock(int maxMessages) throws JMSException
   {

      if (trace) { log.trace(this + " returning next message block of " + maxMessages); }

      try
      {
         if (closed)
         {
            throw new IllegalStateException("Browser is closed");
         }
         
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
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " nextMessageBlock");
      }
   }
   
   public void close() throws JMSException
   {
      try
      {
         localClose();
         session.removeBrowser(id);
         log.trace(this + " closed");
      }   
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " close");
      }
   }
         
   public void closing() throws JMSException
   {
   }
   
   // Public ---------------------------------------------------------------------------------------

   public String toString()
   {
      return "BrowserEndpoint[" + id + "]";
   }

   // Package protected ----------------------------------------------------------------------------
   
   void localClose() throws JMSException
   {
      if (closed)
      {
         throw new IllegalStateException("Browser is already closed");
      }
      
      iterator = null;
      
      session.getConnectionEndpoint().getMessagingServer().getRemotingService().getDispatcher().unregister(id);
      
      closed = true;
   }

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
   
   private class ServerBrowserEndpointHandler implements PacketHandler {

      public String getID()
      {
         return ServerBrowserEndpoint.this.id;
      }
      
      public void handle(Packet packet, PacketSender sender)
      {
         try
         {
            Packet response = null;

            PacketType type = packet.getType();
            if (type == REQ_BROWSER_HASNEXTMESSAGE)
            {
               response = new BrowserHasNextMessageResponse(hasNextMessage());
            } else if (type == REQ_BROWSER_NEXTMESSAGE)
            {
               Message message = nextMessage();
               response = new BrowserNextMessageResponse(message);
            } else if (type == MSG_BROWSER_RESET)
            {
               reset();
            } else if (type == PacketType.MSG_CLOSING)
            {
               closing();
            } else if (type == MSG_CLOSE)
            {
               close();
            } else
            {
               response = new JMSExceptionMessage(new MessagingJMSException(
                     "Unsupported packet for browser: " + packet));
            }

            // reply if necessary
            if (response == null && packet.isOneWay() == false)
            {
               response = new NullPacket();               
            }
            
            if (response != null)
            {
               response.normalize(packet);
               sender.send(response);
            }
         } catch (JMSException e)
         {
            JMSExceptionMessage message = new JMSExceptionMessage(e);
            message.normalize(packet);
            sender.send(message);
         }
      }

      @Override
      public String toString()
      {
         return "ServerBrowserEndpointHandler[id=" + id + "]";
      }
   }
}
