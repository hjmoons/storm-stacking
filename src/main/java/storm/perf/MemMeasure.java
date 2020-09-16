package storm.perf;

public class MemMeasure {
    private long _mem = 0;
    private long _time = 0;

    public synchronized void update(long mem) {
        _mem = mem;
        _time = System.currentTimeMillis();
    }

    public synchronized long get() {
        return isExpired() ? 0l : _mem;
    }

    public synchronized boolean isExpired() {
        return (System.currentTimeMillis() - _time) >= 20000;
    }
}
