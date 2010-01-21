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

package org.hornetq.core.remoting;

import java.util.List;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQException;
import org.hornetq.spi.core.remoting.BufferHandler;
import org.hornetq.spi.core.remoting.Connection;

/**
 * A RemotingConnection is a connection between a client and a server.
 * <p/>
 * It allows multiple {@link org.hornetq.core.protocol.core.Channel}'s to be created and data multiplexed over them. It uses
 * and a {@link Connection} implementation and takes care of failures etc.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public interface RemotingConnection extends BufferHandler
{
   /**
    * returns the unique id of the Remoting Connection
    *
    * @return the id
    */
   Object getID();

   /**
    * returns a string representation of the remote address of this connection
    *
    * @return the remote address
    */
   String getRemoteAddress();

   /**
    * add a failure listener.
    * <p/>
    * The listener will be called in the event of connection failure.
    *
    * @param listener the listener
    */
   void addFailureListener(FailureListener listener);

   /**
    * remove the failure listener
    *
    * @param listener the lister to remove
    * @return true if removed
    */
   boolean removeFailureListener(FailureListener listener);

   /**
    * add a CloseListener.
    * <p/>
    * This will be called in the event of the connection being closed.
    *
    * @param listener the listener to add
    */
   void addCloseListener(CloseListener listener);

   /**
    * remove a Close Listener
    *
    * @param listener the listener to remove
    * @return true if removed
    */
   boolean removeCloseListener(CloseListener listener);

   /**
    * return all the failure listeners
    *
    * @return the listeners
    */
   List<FailureListener> getFailureListeners();

   /**
    * set the failure listeners.
    * <p/>
    * These will be called in the event of the connection being closed. Any previosuly added listeners will be removed.
    *
    * @param listeners the listeners to add.
    */
   void setFailureListeners(List<FailureListener> listeners);

   /**
    * creates a new HornetQBuffer of the specified size.
    *
    * @param size the size of buffer required
    * @return the buffer
    */
   HornetQBuffer createBuffer(int size);

   /**
    * called when the underlying connection fails.
    *
    * @param me the exception that caused the failure
    */
   void fail(HornetQException me);

   /**
    * destroys this connection.
    */
   void destroy();

   /**
    * return the underlying Connection.
    *
    * @return the connection
    */
   Connection getTransportConnection();

   /**
    * returns whether or not the Remoting Connection is a client
    *
    * @return true if client, false if a server
    */
   boolean isClient();

   /**
    * returns true if this Remoting Connection has been destroyed.
    *
    * @return true if destroyed, otherwise false
    */
   boolean isDestroyed();    
   
   /**
    * Disconnect the connection, closing all channels
    */
   void disconnect();
   
   /**
    * returns true if any data has been received since the last time this method was called.
    *
    * @return true if data has been received.
    */
   boolean checkDataReceived();
   
   /**
    * flush all outstanding data from the connection.
    */
   void flush();

}
