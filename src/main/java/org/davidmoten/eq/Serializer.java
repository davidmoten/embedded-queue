package org.davidmoten.eq;

public interface Serializer<T> {

    int serialize(byte[] bytes, int offset);
    
    T deserialize(byte[] bytes, int offset, int length);

}
