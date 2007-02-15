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

import javax.jms.Destination;
import javax.naming.Context;
import javax.naming.InitialContext;

import org.jboss.jms.jndi.JMSProviderAdapter;
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
   
   private String sourceDestinationLookup;
   
   private String targetDestinationLookup;
   
   private String sourceProviderAdaptorLookup;
   
   private String targetProviderAdaptorLookup;
   
      
   public BridgeService()
   {
      bridge = new Bridge();
   }
   
   // JMX attributes ----------------------------------------------------------------
   
   public synchronized String getSourceProviderAdaptorLookup()
   {
      return sourceProviderAdaptorLookup;
   }
   
   public synchronized void setSourceProviderAdaptorLookup(String lookup)
   {
      if (bridge.isStarted())
      {
          log.warn("Cannot set SourceProviderAdaptorLookup when bridge is started");
          return;
      }
      sourceProviderAdaptorLookup = checkAndTrim(lookup);
   }
   
   public synchronized String getTargetProviderAdaptorLookup()
   {
      return targetProviderAdaptorLookup;
   }
   
   public synchronized void setTargetProviderAdaptorLookup(String lookup)
   {
      if (bridge.isStarted())
      {
          log.warn("Cannot set TargetProviderAdaptorLookup when bridge is started");
          return;
      }
      targetProviderAdaptorLookup = checkAndTrim(lookup);
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
      
      if (this.sourceProviderAdaptorLookup == null)
      {
         throw new IllegalArgumentException("sourceProviderAdaptorLookup cannot be null");
      }
      
      if (this.targetProviderAdaptorLookup == null)
      {
         throw new IllegalArgumentException("targetProviderAdaptorLookup cannot be null");
      }
      
      if (sourceDestinationLookup == null)
      {
         throw new IllegalArgumentException("Source destination lookup cannot be null");
      }
      
      if (targetDestinationLookup == null)
      {
         throw new IllegalArgumentException("Target destination lookup cannot be null");
      }
      
      InitialContext ic = new InitialContext();
      
      JMSProviderAdapter sourceAdaptor = (JMSProviderAdapter)ic.lookup(sourceProviderAdaptorLookup);

      boolean sameSourceAndTarget = sourceProviderAdaptorLookup.equals(targetProviderAdaptorLookup);
      
      JMSProviderAdapter targetAdaptor;
      
      if (sameSourceAndTarget)
      {
         targetAdaptor = sourceAdaptor;
      }
      else
      {
         targetAdaptor = (JMSProviderAdapter)ic.lookup(targetProviderAdaptorLookup);
      }
      
      Context icSource = sourceAdaptor.getInitialContext();
      
      Context icTarget = targetAdaptor.getInitialContext();
      
      Destination sourceDest = (Destination)icSource.lookup(sourceDestinationLookup);
      
      Destination targetDest = (Destination)icTarget.lookup(targetDestinationLookup);
            
      String sourceCFRef = sourceAdaptor.getFactoryRef();
      
      String targetCFRef = targetAdaptor.getFactoryRef();
      
      ConnectionFactoryFactory sourceCff =
         new JNDIConnectionFactoryFactory(sourceAdaptor.getProperties(), sourceCFRef);
      
      ConnectionFactoryFactory destCff =
         new JNDIConnectionFactoryFactory(targetAdaptor.getProperties(), targetCFRef);
      
      bridge.setSourceDestination(sourceDest);
      
      bridge.setTargetDestination(targetDest);
      
      bridge.setSourceConnectionFactoryFactory(sourceCff);
      
      bridge.setDestConnectionFactoryFactory(destCff);
      
      bridge.start();      
      
      if (log.isTraceEnabled()) { log.trace("Started bridge"); }
   }
   

   protected void stopService() throws Exception
   {
      if (log.isTraceEnabled()) { log.trace("Stopping bridge"); }
      
      bridge.stop();
      
      if (log.isTraceEnabled()) { log.trace("Stopped bridge"); }
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
