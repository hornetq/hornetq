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

package org.hornetq.jms.bridge.impl;

import javax.management.StandardMBean;

import org.hornetq.jms.bridge.JMSBridge;
import org.hornetq.jms.bridge.JMSBridgeControl;
import org.hornetq.jms.bridge.QualityOfServiceMode;

/**
 * A JMSBridgeControlImpl
 *
 * @author <a href="jose@voxeo.com">Jose de Castro</a>
 *
 */
public class JMSBridgeControlImpl extends StandardMBean implements JMSBridgeControl
{

   private JMSBridge bridge;

   // Constructors --------------------------------------------------

   public JMSBridgeControlImpl(JMSBridge bridge) throws Exception
   {
      super(JMSBridgeControl.class);
      this.bridge = bridge;
   }

   // Public --------------------------------------------------------

   public void pause() throws Exception
   {
      bridge.pause();
   }

   public void resume() throws Exception
   {
      bridge.resume();
   }

   public boolean isStarted()
   {
      return bridge.isStarted();
   }

   public void start() throws Exception
   {
      bridge.start();
   }

   public void stop() throws Exception
   {
      bridge.stop();
   }

   public String getClientID()
   {
      return bridge.getClientID();
   }

   public long getFailureRetryInterval()
   {
      return bridge.getFailureRetryInterval();
   }

   public int getMaxBatchSize()
   {
      return bridge.getMaxBatchSize();
   }

   public long getMaxBatchTime()
   {
      return bridge.getMaxBatchTime();
   }

   public int getMaxRetries()
   {
      return bridge.getMaxRetries();
   }

   public String getQualityOfServiceMode()
   {
      QualityOfServiceMode mode = bridge.getQualityOfServiceMode();
      if (mode != null)
      {
         return mode.name();
      }
      else
      {
         return null;
      }
   }

   public String getSelector()
   {
      return bridge.getSelector();
   }

   public String getSourcePassword()
   {
      return bridge.getSourcePassword();
   }

   public String getSourceUsername()
   {
      return bridge.getSourceUsername();
   }

   public String getSubscriptionName()
   {
      return bridge.getSubscriptionName();
   }

   public String getTargetPassword()
   {
      return bridge.getTargetPassword();
   }

   public String getTargetUsername()
   {
      return bridge.getTargetUsername();
   }

   public String getTransactionManagerLocatorClass()
   {
      return bridge.getTransactionManagerLocatorClass();
   }

   public String getTransactionManagerLocatorMethod()
   {
      return bridge.getTransactionManagerLocatorMethod();
   }

   public boolean isAddMessageIDInHeader()
   {
      return bridge.isAddMessageIDInHeader();
   }

   public boolean isFailed()
   {
      return bridge.isFailed();
   }

   public boolean isPaused()
   {
      return bridge.isPaused();
   }

   public void setAddMessageIDInHeader(boolean value)
   {
      bridge.setAddMessageIDInHeader(value);
   }

   public void setClientID(String clientID)
   {
      bridge.setClientID(clientID);
   }

   public void setFailureRetryInterval(long interval)
   {
      bridge.setFailureRetryInterval(interval);
   }

   public void setMaxBatchSize(int size)
   {
      bridge.setMaxBatchSize(size);
   }

   public void setMaxBatchTime(long time)
   {
      bridge.setMaxBatchTime(time);
   }

   public void setMaxRetries(int retries)
   {
      bridge.setMaxRetries(retries);
   }

   public void setQualityOfServiceMode(String mode)
   {
      if (mode != null)
      {
         bridge.setQualityOfServiceMode(QualityOfServiceMode.valueOf(mode));
      }
      else
      {
         mode = null;
      }
   }

   public void setSelector(String selector)
   {
      bridge.setSelector(selector);
   }

   public void setSourcePassword(String pwd)
   {
      bridge.setSourcePassword(pwd);
   }

   public void setSourceUsername(String name)
   {
      bridge.setSourceUsername(name);
   }

   public void setSubscriptionName(String subname)
   {
      bridge.setSubscriptionName(subname);
   }

   public void setTargetPassword(String pwd)
   {
      bridge.setTargetPassword(pwd);
   }

   public void setTargetUsername(String name)
   {
      bridge.setTargetUsername(name);
   }

   public void setTransactionManagerLocatorClass(String transactionManagerLocatorClass)
   {
      bridge.setTransactionManagerLocatorClass(transactionManagerLocatorClass);
   }

   public void setTransactionManagerLocatorMethod(String transactionManagerLocatorMethod)
   {
      bridge.setTransactionManagerLocatorMethod(transactionManagerLocatorMethod);
   }

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
