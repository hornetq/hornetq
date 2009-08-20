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
import org.hornetq.utils.DataConstants;
import org.hornetq.utils.SimpleString;

/**
 * 
 * A ReplicateRemoteBindingAddedMessage
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 4 Mar 2009 18:36:30
 *
 *
 */
public class ReplicateRemoteBindingAddedMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private SimpleString clusterConnectionName;

   private SimpleString address;

   private SimpleString uniqueName;

   private SimpleString routingName;

   private int remoteQueueID;

   private SimpleString filterString;

   private SimpleString sfQueueName;

   private int distance;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ReplicateRemoteBindingAddedMessage(final SimpleString clusterConnectionName,
                                             final SimpleString address,
                                             final SimpleString uniqueName,
                                             final SimpleString routingName,
                                             final int remoteQueueID,
                                             final SimpleString filterString,
                                             final SimpleString sfQueueName,
                                             final int distance)
   {
      super(REPLICATE_ADD_REMOTE_QUEUE_BINDING);

      this.clusterConnectionName = clusterConnectionName;
      this.address = address;
      this.uniqueName = uniqueName;
      this.routingName = routingName;
      this.remoteQueueID = remoteQueueID;
      this.filterString = filterString;
      this.sfQueueName = sfQueueName;
      this.distance = distance;
   }

   // Public --------------------------------------------------------

   public ReplicateRemoteBindingAddedMessage()
   {
      super(REPLICATE_ADD_REMOTE_QUEUE_BINDING);
   }

   public int getRequiredBufferSize()
   {
      return BASIC_PACKET_SIZE +
             clusterConnectionName.sizeof() + // buffer.writeSimpleString(clusterConnectionName);
             address.sizeof() + // buffer.writeSimpleString(address);
             uniqueName.sizeof() + // buffer.writeSimpleString(uniqueName);
             routingName.sizeof() + //  buffer.writeSimpleString(routingName);
             DataConstants.SIZE_INT + // buffer.writeInt(remoteQueueID);
             SimpleString.sizeofNullableString(filterString) + // buffer.writeNullableSimpleString(filterString);
             sfQueueName.sizeof() + // buffer.writeSimpleString(sfQueueName);
             DataConstants.SIZE_INT; // buffer.writeInt(distance);
   }

   @Override
   public void encodeBody(final HornetQBuffer buffer)
   {
      buffer.writeSimpleString(clusterConnectionName);
      buffer.writeSimpleString(address);
      buffer.writeSimpleString(uniqueName);
      buffer.writeSimpleString(routingName);
      buffer.writeInt(remoteQueueID);
      buffer.writeNullableSimpleString(filterString);
      buffer.writeSimpleString(sfQueueName);
      buffer.writeInt(distance);
   }

   @Override
   public void decodeBody(final HornetQBuffer buffer)
   {
      clusterConnectionName = buffer.readSimpleString();
      address = buffer.readSimpleString();
      uniqueName = buffer.readSimpleString();
      routingName = buffer.readSimpleString();
      remoteQueueID = buffer.readInt();
      filterString = buffer.readNullableSimpleString();
      sfQueueName = buffer.readSimpleString();
      distance = buffer.readInt();
   }

   public SimpleString getClusterConnectionName()
   {
      return clusterConnectionName;
   }

   public SimpleString getAddress()
   {
      return address;
   }

   public SimpleString getUniqueName()
   {
      return uniqueName;
   }

   public SimpleString getRoutingName()
   {
      return routingName;
   }

   public int getRemoteQueueID()
   {
      return remoteQueueID;
   }

   public SimpleString getFilterString()
   {
      return filterString;
   }

   public SimpleString getSfQueueName()
   {
      return sfQueueName;
   }

   public int getDistance()
   {
      return distance;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
