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
   String getSourceDestinationLookup();
   
   void setSourceDestinationLookup(String lookup);
   
   String getTargetDestinationLookup();
   
   void setTargetDestinationLookup(String lookup);
      
   String getSourceUsername();
   
   void setSourceUserName(String name);
   
   String getSourcePassword();
   
   void setSourcePassword(String pwd);
      
   String getTargetUsername();
   
   void setTargetUsername(String name);
   
   String getTargetPassword();
   
   void setTargetPassword(String pwd);
      
   String getSelector();
   
   void setSelector(String selector);
   
   long getFailureRetryInterval();
   
   void setFailureRetryInterval(long interval);     
   
   int getMaxRetries();
   
   void setMaxRetries(int retries);
      
   int getQualityOfServiceMode();
   
   void setQualityOfServiceMode(int mode);   
   
   int getMaxBatchSize();
   
   void setMaxBatchSize(int size);
   
   long getMaxBatchTime();
   
   void setMaxBatchTime(long time);
      
   String getSubName();
   
   void setSubName(String subname);
      
   String getClientID();
   
   void setClientID(String id);
      
   boolean isPaused();
   
   boolean isFailed();
      
   void setSourceJNDIProperties(String props);
   
   void setTargetJNDIProperties(String props);
   
   String getSourceJNDIProperties();
   
   String getTargetJNDIProperties();
   
   void pause() throws Exception;
   
   void resume() throws Exception;  
}
