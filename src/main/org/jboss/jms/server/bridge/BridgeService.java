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
package org.jboss.jms.server.bridge;

import java.io.ByteArrayInputStream;
import java.util.Properties;

import javax.jms.Destination;
import javax.naming.InitialContext;

import org.jboss.messaging.core.plugin.contract.MessagingComponent;
import org.jboss.messaging.core.plugin.contract.ServerPlugin;
import org.jboss.system.ServiceMBeanSupport;

/**
 * A BridgeService
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class BridgeService extends ServiceMBeanSupport
   implements ServerPlugin, BridgeMBean
{
   private Bridge bridge;
   
   private String sourceJNDIProperties;
   
   private String targetJNDIProperties;
   
   private String sourceConnectionFactoryLookup;
   
   private String targetConnectionFactoryLookup;
   
   private String sourceDestinationLookup;
   
   private String targetDestinationLookup;
      
   
   public BridgeService()
   {
      bridge = new Bridge();
   }
   
   // JMX attributes ----------------------------------------------------------------
   
   public synchronized String getSourceConnectionFactoryLookup()
   {
      return this.sourceConnectionFactoryLookup;
   }
   
   public synchronized String getTargetConnectionFactoryLookup()
   {
      return this.targetConnectionFactoryLookup;
   }
   
   public synchronized void setSourceConnectionFactoryLookup(String lookup)
   {
      if (getState() != STOPPED)
      {
         log.warn("Cannot set SourceConnectionFactoryLookup when bridge is started");
         return;
      }
      this.sourceConnectionFactoryLookup = lookup;
   }
   
   public synchronized void setTargetConnectionFactoryLookup(String lookup)
   {
      if (getState() != STOPPED)
      {
         log.warn("Cannot set DestConnectionFactoryLookup when bridge is started");
         return;
      }
      this.targetConnectionFactoryLookup = lookup;
   }
   
   public String getSourceDestinationLookup()
   {
      return sourceDestinationLookup;
   }

   public String getTargetDestinationLookup()
   {
      return targetDestinationLookup;
   }

   public void setSourceDestinationLookup(String lookup)
   {
      if (getState() != STOPPED)
      {
         log.warn("Cannot set SourceDestinationLookup when bridge is started");
         return;
      }
      this.sourceDestinationLookup = lookup;
   }

   public void setTargetDestinationLookup(String lookup)
   {
      if (getState() != STOPPED)
      {
         log.warn("Cannot set TargetDestinationLookup when bridge is started");
         return;
      }
      this.targetDestinationLookup = lookup;
   }
    
   public String getSourceUsername()
   {
      return bridge.getSourceUsername();
   }
   
   public String getSourcePassword()
   {
      return bridge.getSourcePassword();
   }
   
   public void setSourceUserName(String name)
   {
      bridge.setSourceUserName(name);
   }
   
   public void setSourcePassword(String pwd)
   {
      bridge.setSourcePassword(pwd);
   }

   public String getTargetUsername()
   {
      return bridge.getDestUsername();
   }

   public String getTargetPassword()
   {
      return bridge.getDestPassword();
   }
   
   public void setTargetUsername(String name)
   {
      bridge.setDestUserName(name);
   }
   
   public void setTargetPassword(String pwd)
   {
      bridge.setDestPassword(pwd);
   }
   
   public int getQualityOfServiceMode()
   {
      return bridge.getQualityOfServiceMode();
   }
   
   public void setQualityOfServiceMode(int mode)
   {
      bridge.setQualityOfServiceMode(mode);
   }
   
   public String getSelector()
   {
      return bridge.getSelector();
   }

   public void setSelector(String selector)
   {
      bridge.setSelector(selector);
   }

   public int getMaxBatchSize()
   {
      return bridge.getMaxBatchSize();
   }
   
   public void setMaxBatchSize(int size)
   {
      bridge.setMaxBatchSize(size);
   }

   public long getMaxBatchTime()
   {
      return bridge.getMaxBatchTime();
   }
   
   public void setMaxBatchTime(long time)
   {
      bridge.setMaxBatchTime(time);
   }

   public String getSubName()
   {
      return bridge.getSubName();
   }
   
   public void setSubName(String subname)
   {
      bridge.setSubName(subname);
   }

   public String getClientID()
   {
      return bridge.getClientID();
   }
     
   public void setClientID(String clientID)
   {
      bridge.setClientID(clientID);
   }
   
   public long getFailureRetryInterval()
   {
      return bridge.getFailureRetryInterval();
   }
   
   public void setFailureRetryInterval(long interval)
   {
      bridge.setFailureRetryInterval(interval);
   }
   
   public int getMaxRetries()
   {
      return bridge.getMaxRetries();
   }
   
   public void setMaxRetries(int retries)
   {
      bridge.setMaxRetries(retries);
   }
   
   public boolean isFailed()
   {
      return bridge.isFailed();
   }

   public boolean isPaused()
   {
      return bridge.isPaused();
   }

   public synchronized String getSourceJNDIProperties()
   {
      return sourceJNDIProperties;
   }
   
   public synchronized void setSourceJNDIProperties(String props)
   {
      if (props != null)
      {
         props = props.trim();
         if ("".equals(props))
         {
            props = null;
         }
      }
      this.sourceJNDIProperties = props;
   }
   
   public synchronized String getTargetJNDIProperties()
   {
      return targetJNDIProperties;
   }
   
   public synchronized void setTargetJNDIProperties(String props)
   {
      if (props != null)
      {
         props = props.trim();
         if ("".equals(props))
         {
            props = null;
         }
      }
      this.targetJNDIProperties = props;
   }

   public MessagingComponent getInstance()
   {
      return bridge;
   }
   
   // JMX operations ----------------------------------------------------------------
   
   public void pause() throws Exception
   {
      bridge.pause();
   }
   
   public void resume() throws Exception
   {
      bridge.resume();
   }
   
   // ServiceMBeanSupport overrides --------------------------------------------------

   protected void startService() throws Exception
   {
      super.startService();
      
      Properties sourceProps = null;
      
      if (sourceJNDIProperties != null)
      {
         sourceProps = createProps(sourceJNDIProperties);
      }
      
      Properties targetProps = null;
      
      if (targetJNDIProperties != null)
      {
         targetProps = createProps(targetJNDIProperties);
      }
      
      InitialContext icSource = null;
      
      if (sourceProps == null)
      {
         icSource = new InitialContext();
      }
      else
      {
         icSource = new InitialContext(sourceProps);
      }
      
      Destination sourceDest = (Destination)icSource.lookup(sourceDestinationLookup);
      
      InitialContext icDest = null;
      
      if (targetProps == null)
      {
         icDest = new InitialContext();
      }
      else
      {
         icDest = new InitialContext(sourceProps);
      }
      
      Destination targetDest = (Destination)icDest.lookup(targetDestinationLookup);
                     
      ConnectionFactoryFactory sourceCff =
         new JNDIConnectionFactoryFactory(sourceProps, sourceConnectionFactoryLookup);
      
      ConnectionFactoryFactory destCff =
         new JNDIConnectionFactoryFactory(targetProps, targetConnectionFactoryLookup);
      
      bridge.setSourceDestination(sourceDest);
      
      bridge.setTargetDestination(targetDest);
      
      bridge.setSourceConnectionFactoryFactory(sourceCff);
      
      bridge.setDestConnectionFactoryFactory(destCff);
      
      bridge.start();      
   }
   

   protected void stopService() throws Exception
   {
      bridge.stop();
   }
   
   // Private ---------------------------------------------------------------------------------
   
   private Properties createProps(String propsString) throws Exception
   {
      ByteArrayInputStream is = new ByteArrayInputStream(propsString.getBytes());
      
      Properties props = new Properties();
      
      props.load(is);   
      
      return props;
   }



}
