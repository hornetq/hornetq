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


/**
 * A BridgeMBean
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public interface BridgeMBean
{
   // JMX attributes
   
   void setSourceProviderLoader(ObjectName sourceProviderLoader);
   
   ObjectName getSourceProviderLoader();
   
   void setTargetProviderLoader(ObjectName targetProviderLoader);
   
   ObjectName getTargetProviderLoader();
   
   String getSourceDestinationLookup();

   String getTargetDestinationLookup();

   void setSourceDestinationLookup(String lookup);

   void setTargetDestinationLookup(String lookup);
    
   String getSourceUsername();
   
   String getSourcePassword();
   
   void setSourceUsername(String name);
   
   void setSourcePassword(String pwd);

   String getTargetUsername();

   String getTargetPassword();
   
   void setTargetUsername(String name);
   
   void setTargetPassword(String pwd);
   
   int getQualityOfServiceMode();
   
   void setQualityOfServiceMode(int mode);
   
   String getSelector();

   void setSelector(String selector);

   int getMaxBatchSize();
   
   void setMaxBatchSize(int size);

   long getMaxBatchTime();
   
   void setMaxBatchTime(long time);

   String getSubName();
   
   void setSubName(String subname);

   String getClientID();
     
   void setClientID(String clientID);
   
   long getFailureRetryInterval();
   
   void setFailureRetryInterval(long interval);
   
   int getMaxRetries();
   
   void setMaxRetries(int retries);
   
   boolean isFailed();

   boolean isPaused();

   // JMX operations
   
   void pause() throws Exception;
   
   void resume() throws Exception;
}
