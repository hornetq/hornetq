package org.hornetq.jmstests.tools;
import java.net.UnknownHostException;

import org.hornetq.core.logging.Logger;
import org.jnp.server.Main;
import org.jnp.server.NamingBean;

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

/**
 * A WrappedJNDIServer
 * 
 * We wrap the JBoss AS JNDI server, since we want to introduce a pause of 500 milliseconds on stop()
 * 
 * This is because there is a bug in the JBoss AS class whereby the socket can remaining open some time after
 * stop() is called.
 * 
 * So if you call stop() then start() quickly after, you can hit an  exception:
 * 
 * java.rmi.server.ExportException: Port already in use: 1098; nested exception is: 
 * java.net.BindException: Address already in use
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class WrappedJNDIServer
{
   private static final Logger log = Logger.getLogger(WrappedJNDIServer.class);
   
   private Main main;
   
   public WrappedJNDIServer()
   {
      this.main = new Main();
   }
   
   public void start() throws Exception
   {
      main.start();
   }
   
   public void stop()
   {
      main.stop();
      
      try
      {
         Thread.sleep(500);
      }
      catch (Exception e)
      {      
      }
   }
   
   public void setNamingInfo(NamingBean naming)
   {
      main.setNamingInfo(naming);
   }
   
   public void setPort(int port)
   {
      main.setPort(port);
   }
   
   public void setBindAddress(String bindAddress) throws UnknownHostException
   {
      main.setBindAddress(bindAddress);
   }
   
   public void setRmiPort(int port)
   {
      main.setRmiPort(port);
   }
   
   public void setRmiBindAddress(String address) throws UnknownHostException
   {
      main.setRmiBindAddress(address);
   }     
}
