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
package org.jboss.jms.client.delegate;

import java.io.DataInputStream;
import java.io.DataOutputStream;

import javax.jms.JMSException;

import org.jboss.jms.client.state.ConnectionState;
import org.jboss.jms.client.state.HierarchicalState;
import org.jboss.jms.delegate.BrowserDelegate;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.remoting.wireformat.BrowserHasNextMessageRequest;
import org.jboss.messaging.core.remoting.wireformat.BrowserHasNextMessageResponse;
import org.jboss.messaging.core.remoting.wireformat.BrowserNextMessageBlockRequest;
import org.jboss.messaging.core.remoting.wireformat.BrowserNextMessageBlockResponse;
import org.jboss.messaging.core.remoting.wireformat.BrowserNextMessageResponse;
import org.jboss.messaging.core.remoting.wireformat.BrowserResetMessage;
import org.jboss.messaging.core.remoting.wireformat.CloseMessage;
import org.jboss.messaging.core.remoting.wireformat.ClosingRequest;
import org.jboss.messaging.core.remoting.wireformat.ClosingResponse;

/**
 * The client-side Browser delegate class.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 *
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ClientBrowserDelegate extends DelegateSupport implements BrowserDelegate
{
   // Constants ------------------------------------------------------------------------------------

	private static final long serialVersionUID = 3048255931412144958L;
	
   // Attributes -----------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

	public ClientBrowserDelegate(String objectID)
   {
      super(objectID);
   }

   public ClientBrowserDelegate()
   {
   }

   // DelegateSupport overrides --------------------------------------------------------------------

   public void synchronizeWith(DelegateSupport nd) throws Exception
   {
      super.synchronizeWith(nd);

      ClientBrowserDelegate newDelegate = (ClientBrowserDelegate)nd;

      // synchronize server endpoint state

      // synchronize (recursively) the client-side state

      state.synchronizeWith(newDelegate.getState());

      client = ((ConnectionState)state.getParent().getParent()).getRemotingConnection().
         getRemotingClient();
   }

   public void setState(HierarchicalState state)
   {
      super.setState(state);

      client = ((ConnectionState)state.getParent().getParent()).getRemotingConnection().
         getRemotingClient();
   }


   // Closeable implementation ---------------------------------------------------------------------

   public void close() throws JMSException
   {
      sendBlocking(new CloseMessage());
   }

   public long closing(long sequence) throws JMSException
   {
      ClosingResponse response = (ClosingResponse) sendBlocking(new ClosingRequest(sequence));
      return response.getID();
   }

   // BrowserDelegate implementation ---------------------------------------------------------------

   public void reset() throws JMSException
   {
      sendBlocking(new BrowserResetMessage());
   }

   public boolean hasNextMessage() throws JMSException
   {
      BrowserHasNextMessageResponse response = (BrowserHasNextMessageResponse) sendBlocking(new BrowserHasNextMessageRequest());
      return response.hasNext();
   }

   public Message nextMessage() throws JMSException
   {
      BrowserNextMessageResponse response = (BrowserNextMessageResponse) sendBlocking(new org.jboss.messaging.core.remoting.wireformat.BrowserNextMessageRequest());
      return response.getMessage();
   }

   public Message[] nextMessageBlock(int maxMessages) throws JMSException
   {

      BrowserNextMessageBlockResponse response = (BrowserNextMessageBlockResponse) sendBlocking(new BrowserNextMessageBlockRequest(maxMessages));
      return response.getMessages();
   }

   // Streamable implementation ----------------------------------------------------------

   public void read(DataInputStream in) throws Exception
   {
      super.read(in);
   }

   public void write(DataOutputStream out) throws Exception
   {
      super.write(out);
   }

   // Public ---------------------------------------------------------------------------------------

   public String getStackName()
   {
      return "BrowserStack";
   }

   public String toString()
   {
      return "BrowserDelegate[" + System.identityHashCode(this) + ", ID=" + id + "]";
   }

   // Protected ------------------------------------------------------------------------------------

   // Package Private ------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner Classes --------------------------------------------------------------------------------

}
