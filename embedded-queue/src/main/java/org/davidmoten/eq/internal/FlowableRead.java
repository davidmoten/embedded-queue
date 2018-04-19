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

public final class FlowableRead extends Flowable<Object> {

    private final StoreReader storeReader;
    private final long positionGlobal;

    FlowableRead(StoreReader storeReader, long positionGlobal) {
        this.storeReader = storeReader;
        this.positionGlobal = positionGlobal;
    }

    @Override
    protected void subscribeActual(Subscriber<? super Object> subscriber) {
        subscriber.onSubscribe(new ReadSubscription(subscriber, storeReader, positionGlobal));
    }

    public static final class ReadSubscription extends AtomicInteger
            implements Subscription, Reader {

        private static final long serialVersionUID = -4997944041440021717L;

        private final Subscriber<? super Object> subscriber;
        private final StoreReader storeReader;
        private final AtomicBoolean once = new AtomicBoolean();
        private final long positionGlobal;
        private final AtomicLong requested = new AtomicLong();
        private final SimplePlainQueue<Object> queue = new SpscLinkedArrayQueue<Object>(16);

        private static final int REQUESTED_AVAILABLE = 0;
        private static final int REQUESTED_NOT_AVAILABLE = 1;
        private static final int NOT_REQUESTED_AVAILABLE = 2;
        private static final int NOT_REQUESTED_NOT_AVAILABLE = 3;

        private final AtomicInteger state = new AtomicInteger();

        private volatile boolean cancelled;

        public ReadSubscription(Subscriber<? super Object> subscriber, StoreReader storeReader,
                long positionGlobal) {
            this.subscriber = subscriber;
            this.storeReader = storeReader;
            this.positionGlobal = positionGlobal;
        }

        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                BackpressureHelper.add(requested, n);
                drain();
            }
        }

        @Override
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

        @Override
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

        @Override
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

        @Override
        public long startPositionGlobal() {
            return positionGlobal;
        }

        private void drain() {
            if (getAndIncrement() == 0) {
                int missed = 1;
                while (true) {
                    long r = requested.get();
                    long e = 0;
                    while (e != r) {
                        if (cancelled) {
                            return;
                        }
                        Object o = queue.poll();
                        if (o == null) {
                            tryRequestBatch();
                            break;
                        } else {
                            e++;
                            subscriber.onNext(o);
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

        private void tryRequestBatch() {
            while (true) {
                int s = state.get();
                if (s == NOT_REQUESTED_AVAILABLE) {
                    if (state.compareAndSet(s, REQUESTED_AVAILABLE)) {
                        storeReader.requestBatch(this);
                        break;
                    }
                } else {
                    break;
                }
            }
        }

        @Override
        public void cancel() {
            cancelled = true;
            storeReader.cancel(this);
        }

        @Override
        public void messagePart(ByteBuffer bb) {
            queue.offer(bb);
            drain();
        }

        @Override
        public void messageFinished() {
            // anything that is not a ByteBuffer will be used to delimit groups
            queue.offer(0);
            drain();
        }

        @Override
        public void messageError(Throwable error) {
            // TODO
        }
    }
}
