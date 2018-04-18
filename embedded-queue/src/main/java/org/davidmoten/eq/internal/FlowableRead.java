package org.davidmoten.eq.internal;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.davidmoten.eq.internal.event.NewReader;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivex.Flowable;
import io.reactivex.internal.fuseable.SimplePlainQueue;
import io.reactivex.internal.queue.SpscLinkedArrayQueue;
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
        private final SimplePlainQueue<Object> queue = new SpscLinkedArrayQueue<Object>(16);

        private static final int NOT_REQUESTED_NOT_AVAILABLE = 0;
        private static final int REQUESTED_NOT_AVAILABLE = 1;
        private static final int NOT_REQUESTED_AVAILABLE = 2;
        private static final int REQUESTED_AVAILABLE = 3;

        private final AtomicInteger state = new AtomicInteger();

        private volatile boolean cancelled;

        public ReadSubscription(Subscriber<? super ByteBuffer> subscriber, FileSystemStore store,
                long positionGlobal) {
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

        public void batchFinished() {
            while (true) {
                int s = state.get();
                if (s == REQUESTED_AVAILABLE) {
                    if (state.compareAndSet(s, NOT_REQUESTED_AVAILABLE)) {
                        break;
                    }
                } else if (s == REQUESTED_NOT_AVAILABLE) {
                    if (state.compareAndSet(s, NOT_REQUESTED_NOT_AVAILABLE)) {
                        break;
                    }
                } else {
                    break;
                }
            }
        }

        public void available() {
            while (true) {
                int s = state.get();
                if (s == REQUESTED_NOT_AVAILABLE) {
                    if (state.compareAndSet(s, REQUESTED_AVAILABLE)) {
                        break;
                    }
                } else if (s == NOT_REQUESTED_NOT_AVAILABLE) {
                    if (state.compareAndSet(s, NOT_REQUESTED_AVAILABLE)) {
                        break;
                    }
                } else {
                    break;
                }
            }
            drain();
        }

        public void notAvailable() {
            while (true) {
                int s = state.get();
                if (s == REQUESTED_AVAILABLE) {
                    if (state.compareAndSet(s, REQUESTED_NOT_AVAILABLE)) {
                        break;
                    }
                } else if (s == NOT_REQUESTED_AVAILABLE) {
                    if (state.compareAndSet(s, NOT_REQUESTED_NOT_AVAILABLE)) {
                        break;
                    }
                } else {
                    break;
                }
            }
            drain();
        }

        private void drain() {
            if (getAndIncrement() == 0) {
                int missed = 1;
                while (true) {
                    if (cancelled) {
                        return;
                    }
                    long r = requested.get();
                    long e = 0;
                    while (e != r) {
                        Object o = queue.poll();
                        if (o == null) {
                            while (true) {
                                int s = state.get();
                                if (s == NOT_REQUESTED_AVAILABLE) {
                                    if (state.compareAndSet(s, REQUESTED_AVAILABLE)) {
                                        store.requestBatch();
                                        break;
                                    }
                                } else {
                                    break;
                                }
                            }
                            break;
                        } else {
                            e++;
                            subscriber.onNext((ByteBuffer) o);
                        }
                        if (cancelled) {
                            return;
                        }
                    }
                    if (e != 0) {
                        BackpressureHelper.produced(requested, e);
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
            cancelled = true;
            // TODO remove reader from Store
        }

    }

}
