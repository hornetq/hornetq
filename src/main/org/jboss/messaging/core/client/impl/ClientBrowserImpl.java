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

import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.client.ClientBrowser;
import org.jboss.messaging.core.remoting.wireformat.CloseMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionBrowserHasNextMessageMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionBrowserHasNextMessageResponseMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionBrowserNextMessageBlockMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionBrowserNextMessageBlockResponseMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionBrowserNextMessageMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionBrowserNextMessageResponseMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionBrowserResetMessage;
import org.jboss.messaging.util.MessagingException;

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

   private String id;
   
	private ClientSessionInternal session;
	
	private RemotingConnection remotingConnection;
	
	private volatile boolean closed;
	
   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public ClientBrowserImpl(RemotingConnection remotingConnection, ClientSessionInternal session, String id)
   {
      this.remotingConnection = remotingConnection;
      
      this.id = id;
      
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
         remotingConnection.send(id, new CloseMessage());
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
      
      remotingConnection.send(id, new SessionBrowserResetMessage());
   }

   public boolean hasNextMessage() throws MessagingException
   {
      checkClosed();
      
      SessionBrowserHasNextMessageResponseMessage response =
         (SessionBrowserHasNextMessageResponseMessage)remotingConnection.send(id, new SessionBrowserHasNextMessageMessage());
      
      return response.hasNext();
   }

   public Message nextMessage() throws MessagingException
   {
      checkClosed();
      
      SessionBrowserNextMessageResponseMessage response =
         (SessionBrowserNextMessageResponseMessage)remotingConnection.send(id, new SessionBrowserNextMessageMessage());
      
      return response.getMessage();
   }

   public Message[] nextMessageBlock(int maxMessages) throws MessagingException
   {
      checkClosed();
      
      SessionBrowserNextMessageBlockResponseMessage response =
         (SessionBrowserNextMessageBlockResponseMessage)remotingConnection.send(id, new SessionBrowserNextMessageBlockMessage(maxMessages));
      return response.getMessages();
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
