package org.davidmoten.eq.internal;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.davidmoten.eq.internal.event.NewReader;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivex.Flowable;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
import io.reactivex.internal.util.BackpressureHelper;

public final class FlowableRead extends Flowable<ByteBuffer> {

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

    public static final class ReadSubscription extends AtomicInteger implements Subscription {

        private static final long serialVersionUID = -4997944041440021717L;

        private final Subscriber<? super ByteBuffer> subscriber;
        private final FileSystemStore store;
        private final AtomicBoolean once = new AtomicBoolean();
        private final long positionGlobal;
        private final AtomicLong requested = new AtomicLong();

        public ReadSubscription(Subscriber<? super ByteBuffer> subscriber, FileSystemStore store, long positionGlobal) {
            this.subscriber = subscriber;
            this.store = store;
            this.positionGlobal = positionGlobal;
        }

        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                if (once.compareAndSet(false, true)) {
                    store.send(new NewReader(this));
                }
                BackpressureHelper.add(requested, n);
                drain();
            }
        }

        private void drain() {
            if (getAndIncrement() == 0) {
                int missed = 1;
                while (true) {
                    while (true) {
                        // TODO
                        break;
                    }
                    missed = addAndGet(-missed);
                    if (missed == 0) {
                        return;
                    }
                }
            }
        }

        @Override
        public void cancel() {
            // TODO Auto-generated method stub

        }

    }

}
