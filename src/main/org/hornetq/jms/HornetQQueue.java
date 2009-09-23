/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */


package org.hornetq.jms;

import javax.jms.JMSException;
import javax.jms.Queue;

import org.hornetq.core.logging.Logger;
import org.hornetq.utils.SimpleString;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class HornetQQueue extends HornetQDestination implements Queue
{
   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(HornetQQueue.class);

   
	private static final long serialVersionUID = -1106092883162295462L;
	
	public static final String JMS_QUEUE_ADDRESS_PREFIX = "jms.queue.";

   // Static --------------------------------------------------------
   
   public static SimpleString createAddressFromName(String name)
   {
      return new SimpleString(JMS_QUEUE_ADDRESS_PREFIX + name);
   }

   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------

   public HornetQQueue(final String name)
   {      
      super(JMS_QUEUE_ADDRESS_PREFIX + name, name);
   }

   public HornetQQueue(final String address, final String name)
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
      return "HornetQQueue[" + name + "]";
   }
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
