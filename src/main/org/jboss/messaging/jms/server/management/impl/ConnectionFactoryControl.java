/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.jms.server.management.impl;

import java.util.List;

import javax.management.NotCompliantMBeanException;
import javax.management.StandardMBean;

import org.jboss.messaging.core.client.ClientConnectionFactory;
import org.jboss.messaging.jms.client.JBossConnectionFactory;
import org.jboss.messaging.jms.server.management.ConnectionFactoryControlMBean;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class ConnectionFactoryControl extends StandardMBean implements
      ConnectionFactoryControlMBean
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final JBossConnectionFactory cf;
   private ClientConnectionFactory coreCF;
   private final List<String> bindings;
   private String name;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ConnectionFactoryControl(final JBossConnectionFactory cf,
         final ClientConnectionFactory coreCF, final String name, final List<String> bindings)
         throws NotCompliantMBeanException
   {
      super(ConnectionFactoryControlMBean.class);
      this.cf = cf;
      this.coreCF = coreCF;
      this.name = name;
      this.bindings = bindings;
   }

   // Public --------------------------------------------------------

   // ManagedConnectionFactoryMBean implementation ------------------

   public String getURL()
   {
      return coreCF.getLocation().toString();
   }

   public List<String> getBindings()
   {
      return bindings;
   }

   public String getClientID()
   {
      return cf.getClientID();
   }

   public int getDefaultConsumerMaxRate()
   {
      return coreCF.getDefaultConsumerMaxRate();
   }

   public int getDefaultConsumerWindowSize()
   {
      return coreCF.getDefaultConsumerWindowSize();
   }

   public int getDefaultProducerMaxRate()
   {
      return coreCF.getDefaultProducerMaxRate();
   }

   public int getDefaultProducerWindowSize()
   {
      return coreCF.getDefaultProducerWindowSize();
   }

   public int getDupsOKBatchSize()
   {
      return cf.getDupsOKBatchSize();
   }

   public boolean isDefaultBlockOnAcknowledge()
   {
      return coreCF.isDefaultBlockOnAcknowledge();
   }

   public boolean isDefaultBlockOnNonPersistentSend()
   {
      return coreCF.isDefaultBlockOnNonPersistentSend();
   }

   public boolean isDefaultBlockOnPersistentSend()
   {
      return coreCF.isDefaultBlockOnPersistentSend();
   }

   public String getName()
   {
      return name;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
