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
package org.jboss.jms.client.impl;

import org.jboss.jms.client.api.AcknowledgementHandler;
import org.jboss.jms.client.api.ClientProducer;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.util.Logger;
import org.jboss.messaging.util.MessagingException;

/**
 * The client-side Producer connectionFactory class.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ClientProducerImpl implements ClientProducer
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ClientProducerImpl.class);

   // Attributes -----------------------------------------------------------------------------------

   private boolean trace = log.isTraceEnabled();
   
   private ClientSessionInternal session;
   
   private volatile boolean closed;
   
   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------
      
   public ClientProducerImpl(ClientSessionInternal session)
   {
      this.session = session;
   }
   
   // ClientProducer implementation ----------------------------------------------------------------

   public void send(String address, Message message) throws MessagingException
   {
      checkClosed();
      
      session.send(address, message);
   }
   
   public void registerAcknowledgementHandler(AcknowledgementHandler handler)
   {
      // TODO      
   }

   public void unregisterAcknowledgementHandler(AcknowledgementHandler handler)
   {
      // TODO  
   }

   public synchronized void close() throws MessagingException
   {
      if (closed)
      {
         return;         
      }
      session.removeProducer(this);
      
      closed = true;
   }

   public boolean isClosed()
   {
      return closed;
   }
   
   // Public ---------------------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------
   
   // Package Private ------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   private void checkClosed() throws MessagingException
   {
      if (closed)
      {
         throw new MessagingException(MessagingException.OBJECT_CLOSED, "Producer is closed");
      }
   }
   
   // Inner Classes --------------------------------------------------------------------------------

}
