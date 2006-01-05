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
package org.jboss.jms.server.endpoint.advised;

import javax.jms.JMSException;
import javax.jms.Message;

import org.jboss.jms.server.endpoint.BrowserEndpoint;

/**
 * The server-side advised instance corresponding to a Browser. It is bound to the AOP
 * Dispatcher's map.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class BrowserAdvised extends AdvisedSupport implements BrowserEndpoint
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   protected BrowserEndpoint endpoint;

   // Constructors --------------------------------------------------

   public BrowserAdvised(BrowserEndpoint endpoint)
   {
      this.endpoint = endpoint;
   }

   // Static --------------------------------------------------------

   // BrowserAdvised implementation ---------------------------------

   public void close() throws JMSException
   {
      endpoint.close();
   }

   public void closing() throws JMSException
   {
      endpoint.closing();
   }

   public boolean hasNextMessage() throws JMSException
   {
      return endpoint.hasNextMessage();
   }

   public Message nextMessage() throws JMSException
   {
      return endpoint.nextMessage();
   }

   public Message[] nextMessageBlock(int maxMessages) throws JMSException
   {
      return endpoint.nextMessageBlock(maxMessages);
   }

   // AdvisedSupport overrides --------------------------------------
   
   public Object getEndpoint()
   {
      return endpoint;
   }

   // Public --------------------------------------------------------

   public String toString()
   {
      return "BrowserAdvised->" + endpoint;
   }

   // Protected -----------------------------------------------------

   // Package Private -----------------------------------------------

   // Private -------------------------------------------------------

   // Inner Classes -------------------------------------------------
}
