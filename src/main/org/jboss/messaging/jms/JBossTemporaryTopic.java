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
package org.jboss.messaging.jms;

import javax.jms.JMSException;
import javax.jms.TemporaryTopic;

import org.jboss.messaging.jms.client.JBossSession;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 3569 $</tt>
 *
 * $Id: JBossQueue.java 3569 2008-01-15 21:14:04Z timfox $
 */
public class JBossTemporaryTopic extends JBossTopic implements TemporaryTopic
{      
   // Constants -----------------------------------------------------
      
	private static final long serialVersionUID = 845450764835635266L;

	private static final String JMS_TEMP_TOPIC_ADDRESS_PREFIX = "topictempjms.";
   
   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   private final transient JBossSession session;
      
   // Constructors --------------------------------------------------

   public JBossTemporaryTopic(final JBossSession session, final String name)
   {
      super(JMS_TEMP_TOPIC_ADDRESS_PREFIX + name, name);
      
      this.session = session;
   }
   
   // TemporaryQueue implementation ------------------------------------------

   public void delete() throws JMSException
   {      
      session.deleteTemporaryDestination(this);
   }

   // Public --------------------------------------------------------
   
   public boolean isTemporary()
   {
      return true;
   }
      
   public String toString()
   {
      return "JBossTemporaryTopic[" + name + "]";
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
