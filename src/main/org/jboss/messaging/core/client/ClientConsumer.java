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

package org.jboss.messaging.core.client;

import org.jboss.messaging.core.exception.MessagingException;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public interface ClientConsumer
{      
	ClientMessage receive() throws MessagingException;
	
   ClientMessage receive(long timeout) throws MessagingException;
   
   ClientMessage receiveImmediate() throws MessagingException;
   
   MessageHandler getMessageHandler() throws MessagingException;

   void setMessageHandler(MessageHandler handler) throws MessagingException;
   
   void close() throws MessagingException;
   
   boolean isClosed();   
   
   boolean isDirect();

   boolean awaitMessage(long timeOut) throws Exception;

   void stop() throws MessagingException;

   void start() throws MessagingException;

   void restart() throws MessagingException;
}
