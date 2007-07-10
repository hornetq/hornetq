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
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.wireformat.BrowserHasNextMessageRequest;
import org.jboss.jms.wireformat.BrowserNextMessageBlockRequest;
import org.jboss.jms.wireformat.BrowserNextMessageRequest;
import org.jboss.jms.wireformat.BrowserResetRequest;
import org.jboss.jms.wireformat.CloseRequest;
import org.jboss.jms.wireformat.ClosingRequest;
import org.jboss.jms.wireformat.RequestSupport;

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
      RequestSupport req = new CloseRequest(id, version);

      doInvoke(client, req);
   }

   public long closing() throws JMSException
   {
      RequestSupport req = new ClosingRequest(id, version);

      return ((Long)doInvoke(client, req)).longValue();
   }

   // BrowserDelegate implementation ---------------------------------------------------------------

   public void reset() throws JMSException
   {
      RequestSupport req = new BrowserResetRequest(id, version);
      doInvoke(client, req);
   }

   public boolean hasNextMessage() throws JMSException
   {
      RequestSupport req = new BrowserHasNextMessageRequest(id, version);

      return ((Boolean)doInvoke(client, req)).booleanValue();
   }

   public JBossMessage nextMessage() throws JMSException
   {
      RequestSupport req = new BrowserNextMessageRequest(id, version);

      return (JBossMessage)doInvoke(client, req);
   }

   public JBossMessage[] nextMessageBlock(int maxMessages) throws JMSException
   {
      RequestSupport req = new BrowserNextMessageBlockRequest(id, version, maxMessages);

      return (JBossMessage[])doInvoke(client, req);
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
