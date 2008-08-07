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

package org.jboss.messaging.jms;

import javax.jms.JMSException;
import javax.jms.Queue;

import org.jboss.messaging.util.SimpleString;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class JBossQueue extends JBossDestination implements Queue
{
   // Constants -----------------------------------------------------
   
	private static final long serialVersionUID = -1106092883162295462L;
	
	public static final String JMS_QUEUE_ADDRESS_PREFIX = "queuejms.";

   // Static --------------------------------------------------------
   
   public static SimpleString createAddressFromName(String name)
   {
      return new SimpleString(JMS_QUEUE_ADDRESS_PREFIX + name);
   }

   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------

   public JBossQueue(final String name)
   {
      super(JMS_QUEUE_ADDRESS_PREFIX + name, name);
   }

   protected JBossQueue(final String address, final String name)
   {
      super(address, name);
   }

   // Queue implementation ------------------------------------------

   public String getQueueName() throws JMSException
   {
      return name;
   }

   // Public --------------------------------------------------------
   
   public boolean isTemporary()
   {
      return false;
   }
   
   public String toString()
   {
      return "JBossQueue[" + name + "]";
   }
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
