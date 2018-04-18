package org.davidmoten.eq.internal;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import org.davidmoten.eq.internal.event.NewReader;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivex.Flowable;
import io.reactivex.internal.subscriptions.SubscriptionHelper;

public class FlowableRead extends Flowable<ByteBuffer> {

    private final FileSystemStore store;
    private final long positionGlobal;

    FlowableRead(FileSystemStore store, long positionGlobal) {
        this.store = store;
        this.positionGlobal = positionGlobal;
    }

    @Override
    protected void subscribeActual(Subscriber<? super ByteBuffer> subscriber) {
        subscriber.onSubscribe(new ReadSubscription(subscriber, store, positionGlobal));
    }

    private static final class ReadSubscription implements Subscription {

        private final Subscriber<? super ByteBuffer> subscriber;
        private final FileSystemStore store;
        private final AtomicBoolean once = new AtomicBoolean();
        private final long positionGlobal;

        public ReadSubscription(Subscriber<? super ByteBuffer> subscriber, FileSystemStore store, long positionGlobal) {
            this.subscriber = subscriber;
            this.store = store;
            this.positionGlobal = positionGlobal;
        }

        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                 if (once.compareAndSet(false, true)) {
                     store.send(new NewReader(subscriber, positionGlobal));
                 }
            }
        }

        @Override
        public void cancel() {
            // TODO Auto-generated method stub

        }

    }

}
