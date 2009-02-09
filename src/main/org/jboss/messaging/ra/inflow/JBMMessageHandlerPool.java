/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package org.jboss.messaging.ra.inflow;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jboss.messaging.core.logging.Logger;

/**
 * The message handler pool
 * 
 * @author <a href="adrian@jboss.com">Adrian Brock</a>
 * @author <a href="jesper.pedersen@jboss.org">Jesper Pedersen</a>
 * @version $Revision:  $
 */
public class JBMMessageHandlerPool
{
    /** The logger */
   private static final Logger log = Logger.getLogger(JBMMessageHandlerPool.class);

   /** Trace enabled */
   private static boolean trace = log.isTraceEnabled();
      
   /** The activation */
   private JBMActivation activation;

   /** The active sessions */
   private List<JBMMessageHandler> activeSessions;
   
   /** Whether the pool is stopped */
   private AtomicBoolean stopped;
   
   /**
    * Constructor
    * @param activation The activation
    */
   public JBMMessageHandlerPool(JBMActivation activation)
   {
      if (trace)
         log.trace("constructor(" + activation + ")");

      this.activation = activation;
      this.activeSessions = new ArrayList<JBMMessageHandler>();
      this.stopped = new AtomicBoolean(false);
   }

   /**
    * Get the activation
    * @return The value
    */
   public JBMActivation getActivation()
   {
      if (trace)
         log.trace("getActivation()");

      return activation;
   }
   
   /**
    * Start the pool
    * @exception Exception Thrown if an error occurs
    */
   public void start() throws Exception
   {
      if (trace)
         log.trace("start()");

      setupSessions();
   }

   /**
    * Stop the server session pool
    */
   public void stop()
   {
      if (trace)
         log.trace("stop()");

      // Disallow any new sessions
      stopped.set(true);
      
      teardownSessions();
   }
   
   /**
    * Remove message handler
    * @param handler The handler
    */
   protected void removeHandler(JBMMessageHandler handler)
   {
      if (trace)
         log.trace("removeHandler(" + handler + ")");

      synchronized (activeSessions)
      {
         activeSessions.remove(handler);
         
         if (!stopped.get())
         {
            try
            {
               setupSession();
            }
            catch (Exception e)
            {
               log.error("Unable to restart handler", e);
            }
         }
      }
      activeSessions.notifyAll();
   }
   
   /**
    * Starts the sessions
    * @exception Exception Thrown if an error occurs
    */
   protected void setupSessions() throws Exception
   {
      if (trace)
         log.trace("setupSessions()");

      JBMActivationSpec spec = activation.getActivationSpec();

      // Create the sessions
      synchronized (activeSessions)
      {
         for (int i = 0; i < spec.getMaxSessionInt(); ++i)
         {
            setupSession();
         }
      }
   }

   /**
    * Setup a session
    * @exception Exception Thrown if an error occurs
    */
   protected void setupSession() throws Exception
   {
      if (trace)
         log.trace("setupSession()");

      // Create the session
      JBMMessageHandler handler = new JBMMessageHandler(this);
      handler.setup();

      activeSessions.add(handler);
   }

   /**
    * Stop the sessions
    */
   protected void teardownSessions()
   {
      if (trace)
         log.trace("teardownSessions()");

      synchronized (activeSessions)
      {
         if (activation.getActivationSpec().isForceClearOnShutdown())
         {        
            int attempts = 0;
            int forceClearAttempts = activation.getActivationSpec().getForceClearAttempts();
            long forceClearInterval = activation.getActivationSpec().getForceClearOnShutdownInterval();

            if (trace)
               log.trace(this + " force clear behavior in effect. Waiting for " + forceClearInterval + " milliseconds for " + forceClearAttempts + " attempts.");
           
            while((activeSessions.size() > 0) && (attempts < forceClearAttempts))
            {
               try
               {
                  int currentSessions = activeSessions.size();
                  activeSessions.wait(forceClearInterval);
                  // Number of session didn't change
                  if (activeSessions.size() == currentSessions)
                  {
                     ++attempts;
                     log.trace(this + " clear attempt failed " + attempts); 
                  }
               }
               catch(InterruptedException ignore)
               {
               }            
            }
         }
         else
         {
            // Wait for inuse sessions
            while (activeSessions.size() > 0)
            {
               try
               {
                  activeSessions.wait();
               }
               catch (InterruptedException ignore)
               {
               }
            }
         }
      }
   }
}
