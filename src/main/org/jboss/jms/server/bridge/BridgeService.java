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

import java.util.Properties;

import javax.jms.Destination;
import javax.management.ObjectName;
import javax.naming.Context;
import javax.naming.InitialContext;

import org.jboss.messaging.core.contract.MessagingComponent;
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
public class BridgeService extends ServiceMBeanSupport implements BridgeMBean
{
   private Bridge bridge;
   
   private String sourceDestinationLookup;
   
   private String targetDestinationLookup;
   
   private ObjectName sourceProviderLoader;
   
   private ObjectName targetProviderLoader;
   
      
   public BridgeService()
   {
      bridge = new Bridge();
   }
   
   // JMX attributes ----------------------------------------------------------------
   
   public synchronized ObjectName getSourceProviderLoader()
   {
      return sourceProviderLoader;
   }
   
   public synchronized void setSourceProviderLoader(ObjectName sourceProvider)
   {
      if (bridge.isStarted())
      {
          log.warn("Cannot set SourceProvider when bridge is started");
          return;
      }
      this.sourceProviderLoader = sourceProvider;
   }
   
   public synchronized ObjectName getTargetProviderLoader()
   {
      return targetProviderLoader;
   }
   
   public synchronized void setTargetProviderLoader(ObjectName targetProvider)
   {
      if (bridge.isStarted())
      {
          log.warn("Cannot set TargetProvider when bridge is started");
          return;
      }
      this.targetProviderLoader = targetProvider;
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
      if (bridge.isStarted())
      {
         log.warn("Cannot set SourceDestinationLookup when bridge is started");
         return;
      }
      this.sourceDestinationLookup = checkAndTrim(lookup);
   }

   public void setTargetDestinationLookup(String lookup)
   {
      if (bridge.isStarted())
      {
         log.warn("Cannot set TargetDestinationLookup when bridge is started");
         return;
      }
      this.targetDestinationLookup = checkAndTrim(lookup);
   }
    
   public String getSourceUsername()
   {
      return bridge.getSourceUsername();
   }
   
   public String getSourcePassword()
   {
      return bridge.getSourcePassword();
   }
   
   public void setSourceUsername(String name)
   {
      bridge.setSourceUsername(name);
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
   
   public boolean isStarted()
   {
      return bridge.isStarted();
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
      if (log.isTraceEnabled()) { log.trace("Starting bridge"); }
      
      super.startService();
      
      if (this.sourceProviderLoader == null)
      {
         throw new IllegalArgumentException("sourceProvider cannot be null");
      }
      
      if (this.targetProviderLoader == null)
      {
         throw new IllegalArgumentException("targetProvider cannot be null");
      }
      
      if (sourceDestinationLookup == null)
      {
         throw new IllegalArgumentException("Source destination lookup cannot be null");
      }
      
      if (targetDestinationLookup == null)
      {
         throw new IllegalArgumentException("Target destination lookup cannot be null");
      }
      
      boolean sameSourceAndTarget = sourceProviderLoader.equals(targetProviderLoader);
      
      Properties sourceProps = (Properties)server.getAttribute(sourceProviderLoader, "Properties");
      
      Properties targetProps = (Properties)server.getAttribute(targetProviderLoader, "Properties");
      
      Context icSource = new InitialContext(sourceProps);
      
      Context icTarget = new InitialContext(targetProps);
      
      Destination sourceDest = (Destination)icSource.lookup(sourceDestinationLookup);
      
      Destination targetDest = (Destination)icTarget.lookup(targetDestinationLookup);
            
      String sourceCFRef = (String)server.getAttribute(sourceProviderLoader, "FactoryRef");
      
      String targetCFRef = (String)server.getAttribute(targetProviderLoader, "FactoryRef");
      
      ConnectionFactoryFactory sourceCff =
         new JNDIConnectionFactoryFactory(sourceProps, sourceCFRef);
      
      ConnectionFactoryFactory destCff;
      
      if (sameSourceAndTarget)
      {
      	destCff = sourceCff;
      }
      else
      {      
      	destCff= new JNDIConnectionFactoryFactory(targetProps, targetCFRef);
      }
      
      bridge.setSourceDestination(sourceDest);
      
      bridge.setTargetDestination(targetDest);
      
      bridge.setSourceConnectionFactoryFactory(sourceCff);
      
      bridge.setDestConnectionFactoryFactory(destCff);
      
      bridge.start();      
      
      log.info("Started bridge " + this.getName() + ". Source: " + sourceDestinationLookup + " Target: " + targetDestinationLookup);
   }
   

   protected void stopService() throws Exception
   {
      if (log.isTraceEnabled()) { log.trace("Stopping bridge"); }
      
      bridge.stop();
      
      log.info("Stopped bridge " + this.getName());
   }
   
   // Private ---------------------------------------------------------------------------------
   
   private String checkAndTrim(String s)
   {
      if (s != null)
      {
         s = s.trim();
         if ("".equals(s))
         {
            s = null;
         }
      }
      return s;
   }   
}
