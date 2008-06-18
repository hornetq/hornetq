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

import java.io.Serializable;

import javax.jms.Destination;
import javax.naming.NamingException;
import javax.naming.Reference;

import org.jboss.messaging.jms.referenceable.SerializableObjectRefAddr;
import org.jboss.messaging.util.SimpleString;


/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public abstract class JBossDestination implements Destination, Serializable/*, Referenceable http://jira.jboss.org/jira/browse/JBMESSAGING-395*/
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
      
	protected static String escape(final String input)
   {
	   if (input == null)
	   {
	      return "";
	   }
      return input.replace("\\", "\\\\").replace(".", "\\.");
   }
	
	public static JBossDestination fromAddress(final String address)
	{
		if (address.startsWith(JBossQueue.JMS_QUEUE_ADDRESS_PREFIX))
		{
			String name = address.substring(JBossQueue.JMS_QUEUE_ADDRESS_PREFIX.length());
			
			return new JBossQueue(address, name);
		}
		else if (address.startsWith(JBossTopic.JMS_TOPIC_ADDRESS_PREFIX))
		{
			String name = address.substring(JBossTopic.JMS_TOPIC_ADDRESS_PREFIX.length());
			
			return new JBossTopic(address, name);
		}
		else if (address.startsWith(JBossTemporaryQueue.JMS_TEMP_QUEUE_ADDRESS_PREFIX))
		{
			String name = address.substring(JBossTemporaryQueue.JMS_TEMP_QUEUE_ADDRESS_PREFIX.length());
			
			return new JBossTemporaryQueue(null, name);
		}
		else if (address.startsWith(JBossTemporaryTopic.JMS_TEMP_TOPIC_ADDRESS_PREFIX))
		{
			String name = address.substring(JBossTemporaryTopic.JMS_TEMP_TOPIC_ADDRESS_PREFIX.length());
			
			return new JBossTemporaryTopic(null, name);
		}
		else
		{
			throw new IllegalArgumentException("Invalid address " + address);
		}
	}
      
   // Attributes ----------------------------------------------------

   protected final String name;
   
   private final String address;
   
   private final SimpleString simpleAddress;
         
   // Constructors --------------------------------------------------

   public JBossDestination(final String address, final String name)
   {
      this.address = address;
      
      this.name = name;
      
      this.simpleAddress = new SimpleString(address);
   }
   
   // Referenceable implementation ---------------------------------------
   
   public Reference getReference() throws NamingException
   {
      return new Reference("org.jboss.jms.destination.JBossDestination",
                           new SerializableObjectRefAddr("JBM-DEST", this),
                           "org.jboss.jms.referenceable.DestinationObjectFactory",
                           null);
   }

   // Public --------------------------------------------------------
   
   public String getAddress()
   {
      return address;
   }
   
   public SimpleString getSimpleAddress()
   {
   	return simpleAddress;
   }
   
   public String getName()
   {
      return name;
   }
   
   public abstract boolean isTemporary();

   public boolean equals(Object o)
   {
      if (this == o)
      {
         return true;
      }
      
      if (!(o instanceof JBossDestination))
      {
         return false;
      }
      
      JBossDestination that = (JBossDestination)o;
      
      return this.address.equals(that.address);      
   }

   public int hashCode()
   {
      return address.hashCode();      
   }
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
