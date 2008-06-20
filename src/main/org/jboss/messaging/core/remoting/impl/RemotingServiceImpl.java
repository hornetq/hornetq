/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.core.remoting.impl;

import org.jboss.beans.metadata.api.annotations.Install;
import org.jboss.beans.metadata.api.annotations.Uninstall;
import org.jboss.messaging.core.client.RemotingSessionListener;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.ping.Pinger;
import org.jboss.messaging.core.ping.impl.PingerImpl;
import org.jboss.messaging.core.remoting.*;
import static org.jboss.messaging.core.remoting.impl.RemotingConfigurationValidator.validate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 * @version <tt>$Revision$</tt>
 */
public class RemotingServiceImpl implements RemotingService, CleanUpNotifier
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(RemotingServiceImpl.class);

   // Attributes ----------------------------------------------------

   private volatile boolean started = false;

   private Configuration config;

   private List<Acceptor> acceptors = null;

   private final PacketDispatcher dispatcher;

   private List<RemotingSessionListener> listeners = new ArrayList<RemotingSessionListener>();

   private ScheduledExecutorService scheduledExecutor;

   private Map<Long, ScheduledFuture<?>> currentScheduledPingers;

   private Map<Long, Pinger> currentPingers;

   private AcceptorFactory acceptorFactory = new AcceptorFactoryImpl();

   private List<Long> sessions = new ArrayList<Long>();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public RemotingServiceImpl(Configuration config)
   {
      validate(config);

      this.config = config;
      dispatcher = new PacketDispatcherImpl(null);

      scheduledExecutor = new ScheduledThreadPoolExecutor(config.getScheduledThreadPoolMaxSize());
      currentScheduledPingers = new ConcurrentHashMap<Long, ScheduledFuture<?>>();
      currentPingers = new ConcurrentHashMap<Long, Pinger>();
   }


   @Install
   public void addInterceptor(Interceptor filter)
   {
      dispatcher.addInterceptor(filter);
   }

   @Uninstall
   public void removeInterceptor(Interceptor filter)
   {
      dispatcher.removeInterceptor(filter);
   }

   public void addRemotingSessionListener(RemotingSessionListener listener)
   {
      if(listener == null)
      {
         throw new IllegalArgumentException("listener can not be null");
      }

      listeners.add(listener);
   }

   public void removeRemotingSessionListener(RemotingSessionListener listener)
   {
      if(listener == null)
      {
         throw new IllegalArgumentException("listener can not be null");
      }

      listeners.remove(listener);
   }

   // TransportService implementation -------------------------------

   public void start() throws Exception
   {
      if (started)
      {
         return;
      }
      if (log.isDebugEnabled())
      {
         log.debug("Start RemotingServiceImpl with configuration:" + config);
      }

      acceptors = acceptorFactory.createAcceptors(config);
      for (Acceptor acceptor : acceptors)
      {
         acceptor.startAccepting(this, this);
      }

      started = true;
   }

   public void stop()
   {
      for (Acceptor acceptor : acceptors)
      {
         acceptor.stopAccepting();
      }

      ConnectorRegistryFactory.getRegistry().unregister(config.getLocation());

      started = false;
   }
   
   public boolean isStarted()
   {
      return started;
   }

   public PacketDispatcher getDispatcher()
   {
      return dispatcher;
   }

   public Configuration getConfiguration()
   {
      return config;
   }

   public List<Acceptor> getAcceptors()
   {
      return acceptors;
   }

   public void setAcceptorFactory(AcceptorFactory acceptorFactory)
   {
      this.acceptorFactory = acceptorFactory;
   }

   public void registerPinger(RemotingSession session)
   {
      ResponseHandler pongHandler = new ResponseHandlerImpl(dispatcher.generateID());
      Pinger pinger = new PingerImpl(getDispatcher(), session, config.getConnectionParams().getPingTimeout(), pongHandler, RemotingServiceImpl.this);
      ScheduledFuture<?> future =
         scheduledExecutor.scheduleAtFixedRate(pinger, config.getConnectionParams().getPingInterval(),
                                                       config.getConnectionParams().getPingInterval(),
                                                       TimeUnit.MILLISECONDS);
      currentScheduledPingers.put(session.getID(), future);
      currentPingers.put(session.getID(), pinger);
      sessions.add(session.getID());
   }

   public void unregisterPinger(Long id)
   {
      ScheduledFuture<?> future = currentScheduledPingers.remove(id);
      if (future != null)
      {
         future.cancel(true);
      }
      Pinger pinger = currentPingers.remove(id);
      if (pinger != null)
      {
         pinger.close();
      }
   }

   public boolean isSession(Long sessionID)
   {
      return sessions.contains(sessionID);
   }
   
   
   // FailureNotifier implementation -------------------------------
      
   public void fireCleanup(long sessionID, MessagingException me)
   {
      if (sessions.contains(sessionID))
      {
         for (RemotingSessionListener listener : listeners)
         {
            listener.sessionDestroyed(sessionID, me);
         }
         sessions.remove(sessionID);
      }
   }

   // Public --------------------------------------------------------

   public List<Long> getSessions()
   {
      return sessions;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}