/*
 * Copyright 2009 Red Hat, Inc.
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

package org.hornetq.jms.bridge;

import javax.management.ObjectName;

import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.HornetQComponent;
import org.hornetq.jms.bridge.impl.JMSBridgeImpl;

/**
 * A BridgeService
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class BridgeService implements BridgeMBean
{
   private static final Logger log = Logger.getLogger(BridgeService.class);
   
   private JMSBridge bridge;
   
   private String sourceDestinationLookup;
   
   private String targetDestinationLookup;
   
   private ObjectName sourceProviderLoader;
   
   private ObjectName targetProviderLoader;
   
      
   public BridgeService()
   {
      bridge = new JMSBridgeImpl();
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
      return bridge.getTargetUsername();
   }

   public String getTargetPassword()
   {
      return bridge.getTargetPassword();
   }
   
   public void setTargetUsername(String name)
   {
      bridge.setTargetUsername(name);
   }
   
   public void setTargetPassword(String pwd)
   {
      bridge.setTargetPassword(pwd);
   }
   
   public int getQualityOfServiceMode()
   {
      return bridge.getQualityOfServiceMode().intValue();
   }
   
   public void setQualityOfServiceMode(int mode)
   {
      bridge.setQualityOfServiceMode(QualityOfServiceMode.valueOf(mode));
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
      return bridge.getSubscriptionName();
   }
   
   public void setSubName(String subname)
   {
      bridge.setSubscriptionName(subname);
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
   
   public boolean isAddMessageIDInHeader()
   {
   	return bridge.isAddMessageIDInHeader();
   }
   
   public void setAddMessageIDInHeader(boolean value)
   {
   	bridge.setAddMessageIDInHeader(value);
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

   public HornetQComponent getInstance()
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
      
      //super.startService();
      
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
      
     // Properties sourceProps = (Properties)server.getAttribute(sourceProviderLoader, "Properties");
      
     // Properties targetProps = (Properties)server.getAttribute(targetProviderLoader, "Properties");

      /* 
      // JBMESSAGING-1183: set the factory refs according to the destinations types
      Context icSource = new InitialContext(sourceProps);      
      Context icTarget = new InitialContext(targetProps);
      Destination sourceDest = (Destination)icSource.lookup(sourceDestinationLookup);
      Destination targetDest = (Destination)icTarget.lookup(targetDestinationLookup);
      String sourceFactoryRef = "QueueFactoryRef";
      if(sourceDest instanceof Topic)
      {
         sourceFactoryRef = "TopicFactoryRef";
      }
      String targetFactoryRef = "QueueFactoryRef";
      if(targetDest instanceof Topic)
      {
         targetFactoryRef = "TopicFactoryRef";
      }

      String sourceCFRef = (String)server.getAttribute(sourceProviderLoader, sourceFactoryRef);
      
      String targetCFRef = (String)server.getAttribute(targetProviderLoader, targetFactoryRef);
      */
      
      //ConnectionFactoryFactory sourceCff =
      //   new JNDIConnectionFactoryFactory(sourceProps, sourceCFRef);
      
     /* ConnectionFactoryFactory destCff;
      
      if (sameSourceAndTarget)
      {
      	destCff = sourceCff;
      }
      else
      {      
      	destCff= new JNDIConnectionFactoryFactory(targetProps, targetCFRef);
      }
      
      bridge.setSourceConnectionFactoryFactory(sourceCff);
      
      bridge.setDestConnectionFactoryFactory(destCff);
      
      DestinationFactory sourceDestinationFactory = new JNDIDestinationFactory(sourceProps, sourceDestinationLookup);
      
      DestinationFactory targetDestinationFactory = new JNDIDestinationFactory(targetProps, targetDestinationLookup);
      
      bridge.setSourceDestinationFactory(sourceDestinationFactory);
      
      bridge.setTargetDestinationFactory(targetDestinationFactory);

      bridge.start();
      
      log.info("Started bridge " + this.getName() + ". Source: " + sourceDestinationLookup + " Target: " + targetDestinationLookup);*/
   }
   

   protected void stopService() throws Exception
   {
      if (log.isTraceEnabled()) { log.trace("Stopping bridge"); }
      
      bridge.stop();
      
      //log.info("Stopped bridge " + this.getName());
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
