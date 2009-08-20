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

package org.hornetq.core.remoting.impl.wireformat.replication;

import org.hornetq.core.remoting.impl.wireformat.PacketImpl;
import org.hornetq.core.remoting.spi.HornetQBuffer;
import org.hornetq.utils.SimpleString;
import org.hornetq.utils.TypedProperties;

/**
 * 
 * A ReplicateRemoteConsumerRemovedMessage
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 4 Mar 2009 18:36:30
 *
 *
 */
public class ReplicateRemoteConsumerRemovedMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private SimpleString uniqueBindingName;
   
   private SimpleString filterString;
   
   private TypedProperties properties;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ReplicateRemoteConsumerRemovedMessage(SimpleString uniqueBindingName, SimpleString filterString, final TypedProperties properties)
   {
      super(REPLICATE_REMOVE_REMOTE_CONSUMER);

      this.uniqueBindingName = uniqueBindingName;
      
      this.filterString = filterString;
      
      this.properties = properties;
   }

   // Public --------------------------------------------------------

   public ReplicateRemoteConsumerRemovedMessage()
   {
      super(REPLICATE_REMOVE_REMOTE_CONSUMER);
   }

   public int getRequiredBufferSize()
   {
      return BASIC_PACKET_SIZE + 
             uniqueBindingName.sizeof() + // buffer.writeSimpleString(uniqueBindingName);
             SimpleString.sizeofNullableString(filterString) + // buffer.writeNullableSimpleString(filterString);
             properties.getEncodeSize(); // properties.encode(buffer);
   }

   public void encodeBody(final HornetQBuffer buffer)
   {
      buffer.writeSimpleString(uniqueBindingName);
      
      buffer.writeNullableSimpleString(filterString);
      
      properties.encode(buffer);
   }

   public void decodeBody(final HornetQBuffer buffer)
   {
      uniqueBindingName = buffer.readSimpleString();
      
      filterString = buffer.readNullableSimpleString();
      
      properties = new TypedProperties();
      
      properties.decode(buffer);
   }

   public SimpleString getUniqueBindingName()
   {
      return uniqueBindingName;
   }
   
   public SimpleString getFilterString()
   {
      return filterString;
   }
   
   public TypedProperties getProperties()
   {
      return properties;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
