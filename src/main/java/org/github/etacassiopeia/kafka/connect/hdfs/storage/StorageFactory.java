package org.github.etacassiopeia.kafka.connect.hdfs.storage;

import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.connect.errors.ConnectException;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class StorageFactory {
    public static Storage createStorage(Class<? extends Storage> storageClass, Configuration conf, String url) {
        try {
            Constructor<? extends Storage> ctor =
                    storageClass.getConstructor(Configuration.class, String.class);
            return ctor.newInstance(conf, url);
        } catch (NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            throw new ConnectException(e);
        }
    }
}
