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
import javax.jms.TemporaryQueue;

import org.hornetq.jms.client.HornetQSession;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 3569 $</tt>
 *
 * $Id: HornetQQueue.java 3569 2008-01-15 21:14:04Z timfox $
 */
public class HornetQTemporaryQueue extends HornetQQueue implements TemporaryQueue
{   
   // Constants -----------------------------------------------------
   
	private static final long serialVersionUID = -4624930377557954624L;

	public static final String JMS_TEMP_QUEUE_ADDRESS_PREFIX = "jms.tempqueue.";
   
   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   private final transient HornetQSession session;
   
   // Constructors --------------------------------------------------

   public HornetQTemporaryQueue(final HornetQSession session, final String name)
   {
      super(JMS_TEMP_QUEUE_ADDRESS_PREFIX + name, name);
      
      this.session = session;
   }
   
   // TemporaryQueue implementation ------------------------------------------

   public void delete() throws JMSException
   {      
      session.deleteTemporaryQueue(this);
   }

   // Public --------------------------------------------------------
   
   public boolean isTemporary()
   {
      return true;
   }
      
   public String toString()
   {
      return "HornetQTemporaryQueue[" + name + "]";
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
