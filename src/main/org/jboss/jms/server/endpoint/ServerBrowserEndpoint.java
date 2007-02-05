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

import java.util.ArrayList;
import java.util.Iterator;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Message;

import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.selector.Selector;
import org.jboss.jms.util.ExceptionUtil;
import org.jboss.jms.wireformat.Dispatcher;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.Channel;
import org.jboss.messaging.core.Filter;
import org.jboss.messaging.core.Routable;

/**
 * Concrete implementation of BrowserEndpoint.
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ServerBrowserEndpoint implements BrowserEndpoint
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ServerBrowserEndpoint.class);

   // Static ---------------------------------------------------------------------------------------

   private static boolean trace = log.isTraceEnabled();

   // Attributes -----------------------------------------------------------------------------------

   private int id;
   private boolean closed;
   private ServerSessionEndpoint session;
   private Channel destination;
   private Filter filter;
   private Iterator iterator;

   // Constructors ---------------------------------------------------------------------------------

   ServerBrowserEndpoint(ServerSessionEndpoint session, int id,
                         Channel destination, String messageSelector) throws JMSException
   {     
      this.session = session;
      this.id = id;
      this.destination = destination;

		if (messageSelector != null)
		{	
			filter = new Selector(messageSelector);		
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

         log.debug(this + " is being resetted");

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
   
   public JBossMessage nextMessage() throws JMSException
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

         Routable r = (Routable)iterator.next();
   
         if (trace) { log.trace(this + " returning " + r); }
         
         return (JBossMessage)r.getMessage();
      }   
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " nextMessage");
      }
   }

   public JBossMessage[] nextMessageBlock(int maxMessages) throws JMSException
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
               Message m = (Message)((Routable)iterator.next()).getMessage();
               messages.add(m);
               i++;
            }
            else break;
         }		
   		return (JBossMessage[])messages.toArray(new JBossMessage[messages.size()]);	
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
         log.debug(this + " closed");
      }   
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " close");
      }
   }
         
   public void closing() throws JMSException
   {
      // Do nothing
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
      
      Dispatcher.instance.unregisterTarget(id, this);
      
      closed = true;
   }

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   private Iterator createIterator()
   {
      return destination.browse(filter).iterator();
   }

   // Inner classes --------------------------------------------------------------------------------

}
