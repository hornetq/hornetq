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
package org.jboss.example.embedded;

import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.server.impl.MessagingServerImpl;

import static org.jboss.messaging.core.remoting.TransportType.TCP;
import org.jboss.messaging.core.management.MessagingServerManagement;
import org.jboss.messaging.core.management.impl.MessagingServerManagementImpl;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.jms.server.JMSServerManager;
import org.jboss.messaging.jms.server.impl.JMSServerManagerImpl;
import org.jnp.server.NamingBeanImpl;
import org.jnp.server.Main;

import javax.management.StandardMBean;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class EmbeddedManagementExample
{
   public static void main(String args[]) throws Exception
   {
      System.setProperty("java.naming.factory.initial","org.jnp.interfaces.NamingContextFactory");
      System.setProperty("java.naming.factory.url.pkgs","org.jboss.naming:org.jnp.interfaces");

      NamingBeanImpl namingBean = new NamingBeanImpl();
      namingBean.start();
      Main mainMBean = new Main();
      mainMBean.setPort(1099);
      mainMBean.setBindAddress("localhost");
      mainMBean.setRmiPort(1098);
      mainMBean.setRmiBindAddress("localhost");
      mainMBean.setNamingInfo(namingBean);
      mainMBean.start();
      ConfigurationImpl remotingConf = new ConfigurationImpl();
      remotingConf.setPort(5400);
      remotingConf.setHost("localhost");
      remotingConf.setTransport(TCP);
      remotingConf.setInvmDisabled(true);
      MessagingServer messagingServer = new MessagingServerImpl(remotingConf);
      messagingServer.start();
      MessagingServerManagementImpl messagingServerManagement = new MessagingServerManagementImpl();
      messagingServerManagement.setMessagingServer(messagingServer);
      messagingServerManagement.start();
      JMSServerManagerImpl jmsServerManager = new JMSServerManagerImpl();
      jmsServerManager.setMessagingServerManagement(messagingServerManagement);
      jmsServerManager.start();
      StandardMBean serverManagementMBean = new StandardMBean(messagingServerManagement, MessagingServerManagement.class);
      ObjectName serverManagementON = ObjectName.getInstance("org.jboss.messaging:name=MessagingServerManagement");
      ManagementFactory.getPlatformMBeanServer().registerMBean(serverManagementMBean, serverManagementON);
      StandardMBean JMSServerManagementMBean = new StandardMBean(jmsServerManager, JMSServerManager.class);
      ObjectName JMSServerManagerON = ObjectName.getInstance("org.jboss.messaging:name=JMSServerManager");
      ManagementFactory.getPlatformMBeanServer().registerMBean(JMSServerManagementMBean, JMSServerManagerON);
      System.out.println("Press enter to kill server");
      System.out.println("Receiving jmx connections on port 5401, try running command 'jconsole service:jmx:rmi:///jndi/rmi://localhost:5401/jmxrmi'");
       
      System.in.read();
      messagingServerManagement.stop();
      messagingServer.stop();
      mainMBean.stop();
      namingBean.stop();
   }
}
