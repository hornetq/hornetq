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

import javax.jms.IllegalStateException;
import javax.jms.JMSException;

import org.jboss.jms.client.api.ClientBrowser;
import org.jboss.jms.client.api.ClientSession;
import org.jboss.jms.client.remoting.MessagingRemotingConnection;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.remoting.wireformat.BrowserHasNextMessageRequest;
import org.jboss.messaging.core.remoting.wireformat.BrowserHasNextMessageResponse;
import org.jboss.messaging.core.remoting.wireformat.BrowserNextMessageBlockRequest;
import org.jboss.messaging.core.remoting.wireformat.BrowserNextMessageBlockResponse;
import org.jboss.messaging.core.remoting.wireformat.BrowserNextMessageResponse;
import org.jboss.messaging.core.remoting.wireformat.BrowserResetMessage;
import org.jboss.messaging.core.remoting.wireformat.CloseMessage;
import org.jboss.messaging.core.remoting.wireformat.ClosingMessage;

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
   
	private ClientSession session;
	
	private MessagingRemotingConnection remotingConnection;
	
	private volatile boolean closed;
	
   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public ClientBrowserImpl(MessagingRemotingConnection remotingConnection, ClientSession session, String id)
   {
      this.remotingConnection = remotingConnection;
      
      this.id = id;
      
      this.session = session;
   }

   // Closeable implementation ---------------------------------------------------------------------

   public synchronized void close() throws JMSException
   {
      if (closed)
      {
         return;
      }
      
      try
      {
         remotingConnection.sendBlocking(id, new CloseMessage());
      }
      finally
      {
         session.removeBrowser(this);
         
         closed = true;
      }
   }

   public synchronized void closing() throws JMSException
   {
      if (closed)
      {
         return;
      }
      
      remotingConnection.sendBlocking(id, new ClosingMessage());
   }

   public void reset() throws JMSException
   {
      checkClosed();
      
      remotingConnection.sendBlocking(id, new BrowserResetMessage());
   }

   public boolean hasNextMessage() throws JMSException
   {
      checkClosed();
      
      BrowserHasNextMessageResponse response =
         (BrowserHasNextMessageResponse)remotingConnection.sendBlocking(id, new BrowserHasNextMessageRequest());
      
      return response.hasNext();
   }

   public Message nextMessage() throws JMSException
   {
      checkClosed();
      
      BrowserNextMessageResponse response =
         (BrowserNextMessageResponse)remotingConnection.sendBlocking(id, new org.jboss.messaging.core.remoting.wireformat.BrowserNextMessageRequest());
      
      return response.getMessage();
   }

   public Message[] nextMessageBlock(int maxMessages) throws JMSException
   {
      checkClosed();
      
      BrowserNextMessageBlockResponse response =
         (BrowserNextMessageBlockResponse)remotingConnection.sendBlocking(id, new BrowserNextMessageBlockRequest(maxMessages));
      return response.getMessages();
   }

   // Public ---------------------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Package Private ------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------
   
   private void checkClosed() throws IllegalStateException
   {
      if (closed)
      {
         throw new IllegalStateException("Browser is closed");
      }
   }

   // Inner Classes --------------------------------------------------------------------------------

}
