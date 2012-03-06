package org.hornetq.service;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public interface HornetQJMSStarterServiceMBean
{
   void create() throws Exception;

   public void start() throws Exception;

   public void stop() throws Exception;

   void setHornetQServer(HornetQStarterServiceMBean server);
}
