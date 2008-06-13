/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
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

   private boolean started = false;

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
      assert config != null;

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
      assert listener != null;

      listeners.add(listener);
   }

   public void removeRemotingSessionListener(RemotingSessionListener listener)
   {
      assert listener != null;

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
      Pinger pinger = new PingerImpl(getDispatcher(), session, config.getKeepAliveTimeout(), pongHandler, RemotingServiceImpl.this);
      ScheduledFuture<?> future = scheduledExecutor.scheduleAtFixedRate(pinger, config.getKeepAliveInterval(), config.getKeepAliveInterval(), TimeUnit.MILLISECONDS);
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