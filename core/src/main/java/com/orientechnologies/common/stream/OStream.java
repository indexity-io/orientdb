package com.orientechnologies.common.stream;

import java.util.*;
import java.util.function.*;
import java.util.stream.*;

/** Wrapper on {@link Stream} that supports propagation of stream size hints. */
public class OStream<T> implements Stream<T> {

  private final Stream<T> stream;
  private final long sizeHint;

  private OStream(final Stream<T> stream, final long sizeHint) {
    this.stream = stream;
    this.sizeHint = sizeHint;
  }

  public static <T> Stream<T> empty() {
    return stream(Spliterators.emptySpliterator());
  }

  private static long sumSizeHints(final long sizeHintOne, final long sizeHintTwo) {
    if (sizeHintOne == Long.MAX_VALUE || sizeHintTwo == Long.MAX_VALUE) {
      return Long.MAX_VALUE;
    } else {
      return sizeHintOne + sizeHintTwo;
    }
  }

  private static long minSizeHints(long sizeOne, long sizeTwo) {
    if (sizeOne == Long.MAX_VALUE || sizeTwo == Long.MAX_VALUE) {
      return Long.MAX_VALUE;
    }
    return Math.min(sizeOne, sizeTwo);
  }

  public static <T> Stream<T> concat(Stream<? extends T> a, Stream<? extends T> b) {
    return biCombine(a, b, Stream::concat);
  }

  public static <T> Stream<T> of(T t) {
    return new OStream<>(Stream.of(t), 1);
  }

  public static <T> Stream<T> mergeSortedSpliterators(
      Stream<T> streamOne, Stream<T> streamTwo, Comparator<? super T> comparator) {
    return biCombine(
        streamOne,
        streamTwo,
        (s1, s2) -> Streams.mergeSortedSpliterators(widen(s1), widen(s2), comparator));
  }

  private static <T, R> Stream<R> biCombine(
      Stream<? extends T> streamOne,
      Stream<? extends T> streamTwo,
      BiFunction<Stream<? extends T>, Stream<? extends T>, Stream<R>> combiner) {
    Stream<? extends T> s1 = streamOne;
    Stream<? extends T> s2 = streamTwo;
    long sh1 = Long.MAX_VALUE;
    long sh2 = Long.MAX_VALUE;
    if (streamOne instanceof OStream) {
      s1 = ((OStream<? extends T>) streamOne).stream;
      sh1 = ((OStream<? extends T>) streamOne).sizeHint;
    }
    if (streamTwo instanceof OStream) {
      s2 = ((OStream<? extends T>) streamTwo).stream;
      sh2 = ((OStream<? extends T>) streamTwo).sizeHint;
    }
    return new OStream<>(combiner.apply(s1, s2), sumSizeHints(sh1, sh2));
  }

  public static <R> OStream<R> stream(Collection<R> items) {
    return new OStream<>(items.stream(), items.size());
  }

  public static <T> OStream<T> stream(Spliterator<T> spliterator) {
    return new OStream<>(StreamSupport.stream(spliterator, false), spliterator.estimateSize());
  }

  @SuppressWarnings("unchecked")
  public static <T> Stream<T> widen(Stream<? extends T> stream) {
    return (Stream<T>) stream;
  }

  @Override
  public Stream<T> distinct() {
    return stream.distinct();
  }

  @Override
  public Stream<T> filter(Predicate<? super T> predicate) {
    return stream.filter(predicate);
  }

  private <R> OStream<R> withSize(Stream<R> newStream) {
    return new OStream<>(newStream, sizeHint);
  }

  @Override
  public <R> Stream<R> map(Function<? super T, ? extends R> mapper) {
    return withSize(stream.map(mapper));
  }

  @Override
  public OStream<T> onClose(Runnable closeHandler) {
    return withSize(stream.onClose(closeHandler));
  }

  @Override
  public Stream<T> sorted(Comparator<? super T> comparator) {
    return withSize(stream.sorted(comparator));
  }

  @Override
  public Stream<T> sorted() {
    return withSize(stream.sorted());
  }

  /**
   * See {@link Spliterator#getExactSizeIfKnown()}
   *
   * @return the exact size of this stream, or -1 if the size is unknown.
   */
  public long getExactSizeIfKnown() {
    return (sizeHint == Long.MAX_VALUE) ? -1 : sizeHint;
  }

  @Override
  public void close() {
    stream.close();
  }

  /**
   * Computes the count of elements in this stream. If an exact stream size is known, consumption of
   * the stream will be skipped, but this will still be a terminal operation.
   *
   * @see Stream#count()
   * @see #getExactSizeIfKnown()
   */
  public long count() {
    if (sizeHint != Long.MAX_VALUE) {
      // Size of stream is known, but close to make sure this is a terminal operation.
      stream.close();
      return sizeHint;
    }
    return stream.count();
  }

  public <R, A> R collect(Collector<? super T, A, R> collector) {
    return stream.collect(collector);
  }

  public Optional<T> findAny() {
    return stream.findAny();
  }

  public Optional<T> findFirst() {
    return stream.findFirst();
  }

  public void forEach(Consumer<? super T> action) {
    stream.forEach(action);
  }

  public Iterator<T> iterator() {
    return stream.iterator();
  }

  public IntStream mapToInt(ToIntFunction<? super T> mapper) {
    return stream.mapToInt(mapper);
  }

  public boolean noneMatch(Predicate<? super T> predicate) {
    return stream.noneMatch(predicate);
  }

  @Override
  public LongStream mapToLong(ToLongFunction<? super T> mapper) {
    return stream.mapToLong(mapper);
  }

  @Override
  public DoubleStream mapToDouble(ToDoubleFunction<? super T> mapper) {
    return stream.mapToDouble(mapper);
  }

  @Override
  public <R> Stream<R> flatMap(Function<? super T, ? extends Stream<? extends R>> mapper) {
    return stream.flatMap(mapper);
  }

  @Override
  public IntStream flatMapToInt(Function<? super T, ? extends IntStream> mapper) {
    return stream.flatMapToInt(mapper);
  }

  @Override
  public LongStream flatMapToLong(Function<? super T, ? extends LongStream> mapper) {
    return stream.flatMapToLong(mapper);
  }

  @Override
  public DoubleStream flatMapToDouble(Function<? super T, ? extends DoubleStream> mapper) {
    return stream.flatMapToDouble(mapper);
  }

  @Override
  public Stream<T> peek(Consumer<? super T> action) {
    return withSize(stream.peek(action));
  }

  @Override
  public Stream<T> limit(long maxSize) {
    return new OStream<>(stream.limit(maxSize), minSizeHints(sizeHint, maxSize));
  }

  @Override
  public Stream<T> skip(long n) {
    return new OStream<>(stream.skip(n), sumSizeHints(sizeHint, -n));
  }

  @Override
  public void forEachOrdered(Consumer<? super T> action) {
    stream.forEachOrdered(action);
  }

  @Override
  public Object[] toArray() {
    return stream.toArray();
  }

  @Override
  public <A> A[] toArray(IntFunction<A[]> generator) {
    return stream.toArray(generator);
  }

  @Override
  public T reduce(T identity, BinaryOperator<T> accumulator) {
    return stream.reduce(identity, accumulator);
  }

  @Override
  public Optional<T> reduce(BinaryOperator<T> accumulator) {
    return stream.reduce(accumulator);
  }

  @Override
  public <U> U reduce(
      U identity, BiFunction<U, ? super T, U> accumulator, BinaryOperator<U> combiner) {
    return stream.reduce(identity, accumulator, combiner);
  }

  @Override
  public <R> R collect(
      Supplier<R> supplier, BiConsumer<R, ? super T> accumulator, BiConsumer<R, R> combiner) {
    return stream.collect(supplier, accumulator, combiner);
  }

  @Override
  public Optional<T> min(Comparator<? super T> comparator) {
    return stream.min(comparator);
  }

  @Override
  public Optional<T> max(Comparator<? super T> comparator) {
    return stream.max(comparator);
  }

  @Override
  public boolean anyMatch(Predicate<? super T> predicate) {
    return stream.anyMatch(predicate);
  }

  @Override
  public boolean allMatch(Predicate<? super T> predicate) {
    return stream.allMatch(predicate);
  }

  @Override
  public Spliterator<T> spliterator() {
    return stream.spliterator();
  }

  @Override
  public boolean isParallel() {
    return stream.isParallel();
  }

  @Override
  public Stream<T> sequential() {
    return withSize(stream.sequential());
  }

  @Override
  public Stream<T> parallel() {
    return withSize(stream.parallel());
  }

  @Override
  public Stream<T> unordered() {
    return withSize(stream.unordered());
  }
}
