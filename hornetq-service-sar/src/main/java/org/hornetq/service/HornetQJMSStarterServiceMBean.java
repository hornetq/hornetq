package org.hornetq.service;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public interface HornetQJMSStarterServiceMBean
{
   void create() throws Exception;

   void start() throws Exception;

   void stop() throws Exception;

   void setHornetQServer(HornetQStarterServiceMBean server);
}
