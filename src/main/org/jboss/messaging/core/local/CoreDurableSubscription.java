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
package org.jboss.messaging.core.local;

import javax.jms.JMSException;

import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;

/**
 * 
 * Represents a durable topic subscription
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 *
 * $Id$
 */
public class CoreDurableSubscription extends CoreSubscription
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   protected String name;
   protected String clientID;

   // Constructors --------------------------------------------------

   public CoreDurableSubscription(long id, String clientID, String name, Topic topic,
                                  String selector, boolean noLocal, MessageStore ms,
                                  PersistenceManager pm)
   {
      super(id, topic, selector, noLocal, ms, pm);
      this.name = name;
      this.clientID = clientID;
   }
   
   // Channel implementation ----------------------------------------

   // Public --------------------------------------------------------
   
   public void closeConsumer() throws JMSException
   {
      // do nothing - this is durable
   }
   
   public String getName()
   {
      return name;
   }

   public String getClientID()
   {
      return clientID;
   }

   public void load() throws Exception
   {
      this.state.load();
   }

   public String toString()
   {
      return "CoreDurableSubscription[" + getChannelID() + ", " + topic + "]";
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}

