package org.davidmoten.eq2;

import java.nio.charset.StandardCharsets;

import org.junit.Assert;
import org.junit.Test;

public class StoreTest {

    private static final byte[] MSG = "hello".getBytes(StandardCharsets.UTF_8);

    @Test
    public void test() {
        Store store = new Store();
        Assert.assertTrue(store.add(MSG));
    }

}
