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

import org.jboss.jms.server.endpoint.ProducerEndpoint;

/**
 * The server-side advised instance corresponding to a Producer. It is bound to the AOP
 * Dispatcher's map.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ProducerAdvised extends AdvisedSupport implements ProducerEndpoint
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   protected ProducerEndpoint endpoint;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ProducerAdvised(ProducerEndpoint endpoint)
   {
      this.endpoint = endpoint;
   }

   // ProducerEndpoint implementation -------------------------------

   public void close() throws JMSException
   {
      endpoint.close();
   }

   public void closing() throws JMSException
   {
      endpoint.closing();
   }

   public void sendMessage(Message message) throws JMSException
   {
      endpoint.sendMessage(message);
   }

   // AdvisedSupport overrides --------------------------------------

   public Object getEndpoint()
   {
      return endpoint;
   }

   // Public --------------------------------------------------------

   public String toString()
   {
      return "ProducerAdvised->" + endpoint;
   }

   // Protected -----------------------------------------------------

   // Package Private -----------------------------------------------

   // Private -------------------------------------------------------

   // Inner Classes -------------------------------------------------

}
