package com.expertzlab.generics;

/**
 * Created by admin on 21/03/18.
 */
public class Factory<T> {

  public T getInstance(Class clz) throws IllegalAccessException, InstantiationException {

      Product p = ((Product) clz.newInstance());

      if(clz.equals(Soap.class)) {
          p.setName("Soap");
      } else {
          p.setName("Oil");
      }
      return (T) p;
    }
}
