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

package org.hornetq.core.client;

import java.io.InputStream;
import java.io.OutputStream;

import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.message.Message;

/**
 * 
 * A ClientMessage
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public interface ClientMessage extends Message
{
   int getDeliveryCount();
   
   void setDeliveryCount(int deliveryCount);
   
   void acknowledge() throws HornetQException;  
   
   int getBodySize();
   
   //FIXME - these are only used for large messages - they should be moved elsewhere


   /** Sets the OutputStream that will receive the content of a message received in a non blocking way
    * @throws HornetQException */
   void setOutputStream(OutputStream out) throws HornetQException;
   
   /** Save the content of the message to the OutputStream. It will block until the entire content is transfered to the OutputStream. */
   void saveToOutputStream(OutputStream out) throws HornetQException;

   /**
    * Wait the outputStream completion of the message.
    * @param timeMilliseconds - 0 means wait forever
    * @return true if it reached the end
    * @throws HornetQException
    */
   boolean waitOutputStreamCompletion(long timeMilliseconds) throws HornetQException;
            
   void setBodyInputStream(InputStream bodyInputStream);  

}
