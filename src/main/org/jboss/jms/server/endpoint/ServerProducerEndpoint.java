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

import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Message;

import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.server.remoting.JMSDispatcher;
import org.jboss.logging.Logger;

/**
 * Concrete implementation of ProducerEndpoint.
 * 
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ServerProducerEndpoint implements ProducerEndpoint
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ServerProducerEndpoint.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------
   
   private boolean trace = log.isTraceEnabled();

   protected int id;

   /** I need this to set up the JMSDestination header on outgoing messages */
   protected Destination jmsDestination;
   
   protected ServerSessionEndpoint sessionEndpoint;
   
   protected boolean closed;

   // Constructors --------------------------------------------------

   ServerProducerEndpoint(int id,
                          Destination jmsDestination,
                          ServerSessionEndpoint parent)
   {
      this.id = id;
      
      this.jmsDestination = jmsDestination;
      
      sessionEndpoint = parent;
   }

   // ProducerDelegate implementation ------------------------

   public void closing() throws JMSException
   {
      //Currently this does nothing
      if (trace) { log.trace("closing (noop)"); }
   }

   public void close() throws JMSException
   {
      if (closed)
      {
         throw new IllegalStateException("Producer is already closed");
      }
      
      //Currently this does nothing
      if (trace) { log.trace("close (noop)"); }
      this.sessionEndpoint.producers.remove(new Integer(this.id));
      
      JMSDispatcher.instance.unregisterTarget(new Integer(id));
      
      closed = true;
   }
   
   public void send(Message m) throws JMSException
   {
      if (closed)
      {
         throw new IllegalStateException("Producer is closed");
      }
      
      if (trace) { log.trace("Sending message: " + m); }

      sessionEndpoint.connectionEndpoint.sendMessage(m, null);
   }

   // Public --------------------------------------------------------

   public ServerSessionEndpoint getSessionEndpoint()
   {
      return sessionEndpoint;
   }

   public ServerPeer getServerPeer()
   {
      return sessionEndpoint.getConnectionEndpoint().getServerPeer();
   }

   public String toString()
   {
      return "ProducerEndpoint[" + id + ", " + jmsDestination + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
