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

import java.io.Serializable;

import javax.jms.Destination;
import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.Referenceable;

import org.hornetq.jms.referenceable.DestinationObjectFactory;
import org.hornetq.jms.referenceable.SerializableObjectRefAddr;
import org.hornetq.utils.SimpleString;


/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public abstract class HornetQDestination implements Destination, Serializable, Referenceable
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
	
	public static HornetQDestination fromAddress(final String address)
	{
		if (address.startsWith(HornetQQueue.JMS_QUEUE_ADDRESS_PREFIX))
		{
			String name = address.substring(HornetQQueue.JMS_QUEUE_ADDRESS_PREFIX.length());
			
			return new HornetQQueue(address, name);
		}
		else if (address.startsWith(HornetQTopic.JMS_TOPIC_ADDRESS_PREFIX))
		{
			String name = address.substring(HornetQTopic.JMS_TOPIC_ADDRESS_PREFIX.length());
			
			return new HornetQTopic(address, name);
		}
		else if (address.startsWith(HornetQTemporaryQueue.JMS_TEMP_QUEUE_ADDRESS_PREFIX))
		{
			String name = address.substring(HornetQTemporaryQueue.JMS_TEMP_QUEUE_ADDRESS_PREFIX.length());
			
			return new HornetQTemporaryQueue(null, name);
		}
		else if (address.startsWith(HornetQTemporaryTopic.JMS_TEMP_TOPIC_ADDRESS_PREFIX))
		{
			String name = address.substring(HornetQTemporaryTopic.JMS_TEMP_TOPIC_ADDRESS_PREFIX.length());
			
			return new HornetQTemporaryTopic(null, name);
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

   public HornetQDestination(final String address, final String name)
   {
      this.address = address;
      
      this.name = name;
      
      this.simpleAddress = new SimpleString(address);
   }
   
   // Referenceable implementation ---------------------------------------
   
   public Reference getReference() throws NamingException
   {
      return new Reference(this.getClass().getCanonicalName(),
                           new SerializableObjectRefAddr("HornetQ-DEST", this),
                           DestinationObjectFactory.class.getCanonicalName(),
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
      
      if (!(o instanceof HornetQDestination))
      {
         return false;
      }
      
      HornetQDestination that = (HornetQDestination)o;
      
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
