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

import javax.jms.JMSException;
import javax.jms.Message;

import org.jboss.jms.delegate.BrowserDelegate;
import org.jboss.remoting.InvokerLocator;

/**
 * The client-side Browser delegate class.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 *
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ClientBrowserDelegate extends DelegateSupport implements BrowserDelegate
{
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = 8293543769773757409L;

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ClientBrowserDelegate(String objectID, InvokerLocator locator)
   {
      super(objectID, locator);
   }


   // BrowserDelegate implementation --------------------------------

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public void close() throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   public void closing() throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   public boolean hasNextMessage() throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   public Message nextMessage() throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   public Message[] nextMessageBlock(int maxMessages) throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   // Public --------------------------------------------------------

   public String getStackName()
   {
      return "BrowserStack";
   }

   // Protected -----------------------------------------------------

   // Package Private -----------------------------------------------

   // Private -------------------------------------------------------

   // Inner Classes -------------------------------------------------

}
