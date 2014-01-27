/*
* JBoss, Home of Professional Open Source.
* Copyright 2010, Red Hat, Inc., and individual contributors
* as indicated by the @author tags. See the copyright.txt file in the
* distribution for a full listing of individual contributors.
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
package org.hornetq.jms.server.impl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.hornetq.core.server.ActivateCallback;
import org.hornetq.core.server.HornetQComponent;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServerLogger;
import org.jnp.server.Main;
import org.jnp.server.NamingBeanImpl;

/**
 * This server class is only used in the standalone mode, its used to control the life cycle of the Naming Server to allow
 * it to be activated and deactivated
 *
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         11/8/12
 */
public class StandaloneNamingServer implements HornetQComponent
{
   private Main jndiServer;

   private HornetQServer server;

   private NamingBeanImpl namingBean;

   private int port = 1099;

   private String bindAddress = "localhost";

   private int rmiPort = 1098;

   private String rmiBindAddress = "localhost";

   private ExecutorService executor;

   public StandaloneNamingServer(HornetQServer server)
   {
      this.server = server;
   }

   @Override
   public void start() throws Exception
   {
      server.registerActivateCallback(new ServerActivateCallback());
   }

   @Override
   public void stop() throws Exception
   {
   }

   @Override
   public boolean isStarted()
   {
      return false;
   }

   public void setPort(int port)
   {
      this.port = port;
   }

   public void setBindAddress(String bindAddress)
   {
      this.bindAddress = bindAddress;
   }

   public void setRmiPort(int rmiPort)
   {
      this.rmiPort = rmiPort;
   }

   public void setRmiBindAddress(String rmiBindAddress)
   {
      this.rmiBindAddress = rmiBindAddress;
   }

   private class ServerActivateCallback implements ActivateCallback
   {
      private boolean activated = false;

      @Override
      public synchronized void preActivate()
      {
         if (activated)
         {
            return;
         }
         try
         {
            jndiServer = new Main();
            namingBean = new NamingBeanImpl();
            jndiServer.setNamingInfo(namingBean);
            executor = Executors.newCachedThreadPool();
            jndiServer.setLookupExector(executor);
            jndiServer.setPort(port);
            jndiServer.setBindAddress(bindAddress);
            jndiServer.setRmiPort(rmiPort);
            jndiServer.setRmiBindAddress(rmiBindAddress);
            namingBean.start();
            jndiServer.start();
         }
         catch (Exception e)
         {
            HornetQServerLogger.LOGGER.unableToStartNamingServer(e);
         }

         activated = true;
      }

      @Override
      public void activated()
      {

      }

      @Override
      public synchronized void deActivate()
      {
         if (!activated)
         {
            return;
         }
         if (jndiServer != null)
         {
            try
            {
               jndiServer.stop();
            }
            catch (Exception e)
            {
               HornetQServerLogger.LOGGER.unableToStopNamingServer(e);
            }
         }
         if (namingBean != null)
         {
            namingBean.stop();
         }
         if (executor != null)
         {
            executor.shutdown();
         }
         activated = false;
      }
   }
}
