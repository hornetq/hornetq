/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.tests.integration.cluster.failover;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryInternal;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.remoting.impl.ByteBufferWrapper;
import org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory;
import org.jboss.messaging.core.remoting.impl.invm.TransportConstants;
import org.jboss.messaging.core.server.impl.MessagingServiceImpl;

/**
 * A LargeMessageMuliThreadFailoverTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * 
 * Created Jan 18, 2009 4:52:09 PM
 *
 *
 */
public class LargeMessageMuliThreadFailoverTest extends MultiThreadRandomFailoverTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------
   
   protected ClientSessionFactoryInternal createSessionFactory()
   {
      ClientSessionFactoryInternal sf = super.createSessionFactory();
      
      sf.setMinLargeMessageSize(200);
      
      return sf;

   }


   @Override
   protected void start() throws Exception
   {

      deleteDirectory(new File(getTestDir()));

      Configuration backupConf = new ConfigurationImpl();

      backupConf.setJournalDirectory(getJournalDir(getTestDir() + "/backup"));
      backupConf.setLargeMessagesDirectory(getLargeMessagesDir(getTestDir() + "/backup"));
      backupConf.setBindingsDirectory(getBindingsDir(getTestDir() + "/backup"));
      backupConf.setPagingDirectory(getPageDir(getTestDir() + "/backup"));
      backupConf.setJournalFileSize(100 * 1024);

      backupConf.setSecurityEnabled(false);
      backupParams.put(TransportConstants.SERVER_ID_PROP_NAME, 1);

      backupConf.getAcceptorConfigurations()
                .add(new TransportConfiguration(InVMAcceptorFactory.class.getCanonicalName(), backupParams));
      backupConf.setBackup(true);

      backupService = MessagingServiceImpl.newMessagingService(backupConf);
      backupService.start();

      Configuration liveConf = new ConfigurationImpl();

      liveConf.setJournalDirectory(getJournalDir(getTestDir() + "/live"));
      liveConf.setLargeMessagesDirectory(getLargeMessagesDir(getTestDir() + "/live"));
      liveConf.setBindingsDirectory(getBindingsDir(getTestDir() + "/live"));
      liveConf.setPagingDirectory(getPageDir(getTestDir() + "/live"));

      liveConf.setJournalFileSize(100 * 1024);

      liveConf.setSecurityEnabled(false);
      liveConf.getAcceptorConfigurations()
              .add(new TransportConfiguration(InVMAcceptorFactory.class.getCanonicalName()));

      Map<String, TransportConfiguration> connectors = new HashMap<String, TransportConfiguration>();

      TransportConfiguration backupTC = new TransportConfiguration(INVM_CONNECTOR_FACTORY,
                                                                   backupParams,
                                                                   "backup-connector");
      connectors.put(backupTC.getName(), backupTC);
      liveConf.setConnectorConfigurations(connectors);
      liveConf.setBackupConnectorName(backupTC.getName());
      liveService = MessagingServiceImpl.newMessagingService(liveConf);

      liveService.start();

   }
   
   protected int getNumIterations()
   {
      return 10;
   }


   @Override
   protected void setBody(final ClientMessage message) throws Exception
   {
      message.setBody(new ByteBufferWrapper(ByteBuffer.allocate(500)));

   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.tests.integration.cluster.failover.MultiThreadRandomFailoverTestBase#checkSize(org.jboss.messaging.core.client.ClientMessage)
    */
   @Override
   protected boolean checkSize(ClientMessage message)
   {
      return 500 ==  message.getBodySize();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
