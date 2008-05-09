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
package org.jboss.messaging.core.client.impl;

import org.jboss.messaging.core.client.ClientBrowser;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket;
import org.jboss.messaging.core.remoting.impl.wireformat.ReceiveMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBrowserHasNextMessageResponseMessage;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 * @version <tt>$Revision: 3602 $</tt>
 *
 * $Id: ClientBrowserImpl.java 3602 2008-01-21 17:48:32Z timfox $
 */
public class ClientBrowserImpl implements ClientBrowser
{
   // Constants ------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private final long serverTargetID;
   
	private final ClientSessionInternal session;
	
	private final RemotingConnection remotingConnection;
	
	private volatile boolean closed;
	
   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public ClientBrowserImpl(final long serverTargetID, final ClientSessionInternal session,
   		                   final RemotingConnection remotingConnection)
   {
      this.remotingConnection = remotingConnection;
      
      this.serverTargetID = serverTargetID;
      
      this.session = session;
   }

   // ClientBrowser implementation -----------------------------------------------------------------
   
   public synchronized void close() throws MessagingException
   {
      if (closed)
      {
         return;
      }
      
      try
      {
         remotingConnection.sendBlocking(serverTargetID, session.getServerTargetID(), new EmptyPacket(EmptyPacket.CLOSE));
      }
      finally
      {
         session.removeBrowser(this);
         
         closed = true;
      }
   }

   public boolean isClosed()
   {
      return closed;
   }

   public void reset() throws MessagingException
   {
      checkClosed();
      
      remotingConnection.sendBlocking(serverTargetID, session.getServerTargetID(), new EmptyPacket(EmptyPacket.SESS_BROWSER_RESET));
   }

   public boolean hasNextMessage() throws MessagingException
   {
      checkClosed();
      
      SessionBrowserHasNextMessageResponseMessage response =
         (SessionBrowserHasNextMessageResponseMessage)remotingConnection.sendBlocking(serverTargetID, session.getServerTargetID(), new EmptyPacket(EmptyPacket.SESS_BROWSER_HASNEXTMESSAGE));
      
      return response.hasNext();
   }

   public ClientMessage nextMessage() throws MessagingException
   {
      checkClosed();
      
      ReceiveMessage response =
         (ReceiveMessage)remotingConnection.sendBlocking(serverTargetID, session.getServerTargetID(), new EmptyPacket(EmptyPacket.SESS_BROWSER_NEXTMESSAGE));
      
      return response.getClientMessage();
   }

   // Public ---------------------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Package Private ------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------
   
   private void checkClosed() throws MessagingException
   {
      if (closed)
      {
         throw new MessagingException(MessagingException.OBJECT_CLOSED, "Browser is closed");
      }
   }

   // Inner Classes --------------------------------------------------------------------------------

}
