package org.hornetq.tests.unit.ra;

import javax.resource.spi.UnavailableException;
import javax.resource.spi.endpoint.MessageEndpoint;
import javax.transaction.xa.XAResource;
import java.lang.reflect.Method;

public class MessageEndpointFactory implements javax.resource.spi.endpoint.MessageEndpointFactory
{

   @Override
   public MessageEndpoint createEndpoint(final XAResource arg0) throws UnavailableException
   {
      return null;
   }

   @Override
   public boolean isDeliveryTransacted(final Method arg0) throws NoSuchMethodException
   {
      return false;
   }

}