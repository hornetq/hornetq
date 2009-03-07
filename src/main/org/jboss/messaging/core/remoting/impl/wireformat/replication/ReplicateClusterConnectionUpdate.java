/*
 * JBoss, Home of Professional Open Source Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors by
 * the @authors tag. See the copyright.txt in the distribution for a full listing of individual contributors. This is
 * free software; you can redistribute it and/or modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of the License, or (at your option) any later version.
 * This software is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details. You should have received a copy of the GNU Lesser General Public License along with this software; if not,
 * write to the Free Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */

package org.jboss.messaging.core.remoting.impl.wireformat.replication;

import java.util.List;

import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.utils.Pair;
import org.jboss.messaging.utils.SimpleString;

/**
 * 
 * A ReplicateClusterConnectionUpdate
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 4 Mar 2009 17:46:53
 *
 *
 */
public class ReplicateClusterConnectionUpdate extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private SimpleString clusterConnectionName;
   
   private List<Pair<TransportConfiguration, TransportConfiguration>> connectors;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ReplicateClusterConnectionUpdate(SimpleString clusterConnectionName, List<Pair<TransportConfiguration, TransportConfiguration>> connectors)
   {
      super(REPLICATE_UPDATE_CONNECTORS);

      this.clusterConnectionName = clusterConnectionName;
      
      this.connectors = connectors;
   }

   public ReplicateClusterConnectionUpdate()
   {
      super(REPLICATE_UPDATE_CONNECTORS);
   }

   // Public --------------------------------------------------------

   public SimpleString getClusterConnectionName()
   {
      return clusterConnectionName;
   }
   
   public List<Pair<TransportConfiguration, TransportConfiguration>> getConnectors()
   {
      return connectors;
   }

   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.writeSimpleString(clusterConnectionName);
      
      buffer.writeInt(connectors.size());

      for (Pair<TransportConfiguration, TransportConfiguration> connectorPair : connectors)
      {
         connectorPair.a.encode(buffer);

         if (connectorPair.b != null)
         {
            buffer.writeBoolean(true);

            connectorPair.b.encode(buffer);
         }
         else
         {
            buffer.writeBoolean(false);
         }
      }
   }

   public void decodeBody(final MessagingBuffer buffer)
   {
      clusterConnectionName = buffer.readSimpleString();
      
      int size = buffer.readInt();

      for (int i = 0; i < size; i++)
      {
         TransportConfiguration connector = new TransportConfiguration();

         connector.decode(buffer);

         boolean existsBackup = buffer.readBoolean();

         TransportConfiguration backupConnector = null;

         if (existsBackup)
         {
            backupConnector = new TransportConfiguration();

            backupConnector.decode(buffer);
         }

         Pair<TransportConfiguration, TransportConfiguration> connectorPair = new Pair<TransportConfiguration, TransportConfiguration>(connector,
                                                                                                                                       backupConnector);

         connectors.add(connectorPair);
      }
   }

   public boolean isRequiresConfirmations()
   {
      return false;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
