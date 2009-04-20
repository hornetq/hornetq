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
import javax.jms.TemporaryQueue;

import org.jboss.messaging.jms.client.JBossSession;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 3569 $</tt>
 *
 * $Id: JBossQueue.java 3569 2008-01-15 21:14:04Z timfox $
 */
public class JBossTemporaryQueue extends JBossQueue implements TemporaryQueue
{   
   // Constants -----------------------------------------------------
   
	private static final long serialVersionUID = -4624930377557954624L;

	public static final String JMS_TEMP_QUEUE_ADDRESS_PREFIX = "jms.tempqueue.";
   
   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   private final transient JBossSession session;
   
   // Constructors --------------------------------------------------

   public JBossTemporaryQueue(final JBossSession session, final String name)
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
      return "JBossTemporaryQueue[" + name + "]";
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
