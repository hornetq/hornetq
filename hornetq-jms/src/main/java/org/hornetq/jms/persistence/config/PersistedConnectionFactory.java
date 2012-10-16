/*
 * Copyright 2010 Red Hat, Inc.
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

package org.hornetq.jms.persistence.config;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.core.journal.EncodingSupport;
import org.hornetq.jms.server.config.ConnectionFactoryConfiguration;
import org.hornetq.jms.server.config.impl.ConnectionFactoryConfigurationImpl;

/**
 * A PersistedConnectionFactory
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class PersistedConnectionFactory implements EncodingSupport
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private long id;

   private ConnectionFactoryConfiguration config;

   public PersistedConnectionFactory()
   {
      super();
   }

   /**
    * @param config
    */
   public PersistedConnectionFactory(final ConnectionFactoryConfiguration config)
   {
      super();
      this.config = config;
   }

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   /**
    * @return the id
    */
   public long getId()
   {
      return id;
   }

   public void setId(final long id)
   {
      this.id = id;
   }

   public String getName()
   {
      return config.getName();
   }

   /**
    * @return the config
    */
   public ConnectionFactoryConfiguration getConfig()
   {
      return config;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.journal.EncodingSupport#decode(org.hornetq.api.core.HornetQBuffer)
    */
   public void decode(final HornetQBuffer buffer)
   {
      config = new ConnectionFactoryConfigurationImpl();
      config.decode(buffer);
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.journal.EncodingSupport#encode(org.hornetq.api.core.HornetQBuffer)
    */
   public void encode(final HornetQBuffer buffer)
   {
      config.encode(buffer);
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.journal.EncodingSupport#getEncodeSize()
    */
   public int getEncodeSize()
   {
      return config.getEncodeSize();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
