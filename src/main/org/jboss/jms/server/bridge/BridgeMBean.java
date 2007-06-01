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
