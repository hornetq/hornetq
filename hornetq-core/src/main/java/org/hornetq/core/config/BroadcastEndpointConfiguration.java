package org.hornetq.core.config;

import java.io.Serializable;
import java.util.Map;

public class BroadcastEndpointConfiguration implements Serializable
{
   private static final long serialVersionUID = -6621509377498920815L;

   private String name;
   private String clazz;
   private Map<String, Object> params;

   public BroadcastEndpointConfiguration(String name, String clazz, Map<String, Object> params)
   {
      this.name = name;
      this.clazz = clazz;
      this.params = params;
   }

   public String getName()
   {
      return name;
   }

   @Override
   public int hashCode()
   {
      return name.hashCode();
   }

   @Override
   public boolean equals(Object o)
   {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      BroadcastEndpointConfiguration that = (BroadcastEndpointConfiguration)o;
      
      if (!name.equals(that.name)) return false;
      if (!clazz.equals(that.clazz)) return false;
      
      if (params != null)
      {
         return params.equals(that.params);
      }
      
      if (that.params != null)
      {
         return false;
      }
      
      return true;
   }

   public String getClazz()
   {
      return this.clazz;
   }

   public Map<String, Object> getParams()
   {
      return this.params;
   }

}
