package com.sankuai.inf.leaf.segment;

import com.sankuai.inf.leaf.IDGen;
import com.sankuai.inf.leaf.common.Result;
import com.sankuai.inf.leaf.common.Status;
import com.sankuai.inf.leaf.segment.dao.IDAllocDao;
import com.sankuai.inf.leaf.segment.model.*;
import org.perf4j.StopWatch;
import org.perf4j.slf4j.Slf4JStopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class SegmentIDGenImpl implements IDGen {
    private static final Logger logger = LoggerFactory.getLogger(SegmentIDGenImpl.class);

    /**
     * IDCache未初始化成功时的异常码
     */
    private static final long EXCEPTION_ID_IDCACHE_INIT_FALSE = -1;
    /**
     * key不存在时的异常码
     */
    private static final long EXCEPTION_ID_KEY_NOT_EXISTS = -2;
    /**
     * SegmentBuffer中的两个Segment均未从DB中装载时的异常码
     */
    private static final long EXCEPTION_ID_TWO_SEGMENTS_ARE_NULL = -3;
    /**
     * 最大步长不超过100,0000
     */
    private static final int MAX_STEP = 1000000;
    /**
     * 一个Segment维持时间为15分钟
     */
    private static final long SEGMENT_DURATION = 15 * 60 * 1000L;
    private ExecutorService service = new ThreadPoolExecutor(5, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(), new UpdateThreadFactory());
    private volatile boolean initOK = false;
    private Map<String, SegmentBuffer> cache = new ConcurrentHashMap<String, SegmentBuffer>();
    private IDAllocDao dao;

    public static class UpdateThreadFactory implements ThreadFactory {

        private static int threadInitNumber = 0;

        private static synchronized int nextThreadNum() {
            return threadInitNumber++;
        }

        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "Thread-Segment-Update-" + nextThreadNum());
        }
    }

    @Override
    public boolean init() {
        logger.info("Init ...");
        // 确保加载到kv后才初始化成功
        // 刷新一次DB的数据到cache中
        updateCacheFromDb();
        initOK = true;
        // 开启一个定时任务, 每60秒刷新一次DB的数据到cache中
        updateCacheFromDbAtEveryMinute();
        return initOK;
    }

    private void updateCacheFromDbAtEveryMinute() {
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName("check-idCache-thread");
                // 开启一个后台线程
                t.setDaemon(true);
                return t;
            }
        });
        // 用这个后台线程执行定时任务, 每60秒刷新一次DB的数据到cache中
        service.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                updateCacheFromDb();
            }
        }, 60, 60, TimeUnit.SECONDS);
    }

    // 应用刚启动时执行一次
    // 每60秒执行一次
    // 刷新一次DB的数据到cache中, 有则加, 无则删
    private void updateCacheFromDb() {
        logger.info("update cache from db");
        StopWatch sw = new Slf4JStopWatch();
        try {
            // SELECT biz_tag FROM leaf_alloc
            // leaf_alloc表的数据是提前用SQL初始化的, 标记有哪些业务类型
            List<String> dbTags = dao.getAllTags();
            if (dbTags == null || dbTags.isEmpty()) {
                return;
            }
            List<String> cacheTags = new ArrayList<String>(cache.keySet());
            Set<String> insertTagsSet = new HashSet<>(dbTags);
            Set<String> removeTagsSet = new HashSet<>(cacheTags);
            //db中新加的tags灌进cache
            for(int i = 0; i < cacheTags.size(); i++){
                String tmp = cacheTags.get(i);
                if(insertTagsSet.contains(tmp)){
                    // 在缓存cache中有的biz_tag就忽略掉, 不插入
                    insertTagsSet.remove(tmp);
                }
            }
            // 为每个biz_tag初始化一个SegmentBuffer, 放入缓存中
            for (String tag : insertTagsSet) {
                // 内部有一个Segment数组, 长度为2, 用作双缓冲优化
                SegmentBuffer buffer = new SegmentBuffer();
                buffer.setKey(tag);
                Segment segment = buffer.getCurrent();
                segment.setValue(new AtomicLong(0));
                segment.setMax(0);
                segment.setStep(0);
                cache.put(tag, buffer);
                logger.info("Add tag {} from db to IdCache, SegmentBuffer {}", tag, buffer);
            }
            //cache中已失效的tags从cache删除
            for(int i = 0; i < dbTags.size(); i++){
                String tmp = dbTags.get(i);
                if(removeTagsSet.contains(tmp)){
                    // 在数据库中有的biz_tag就忽略掉, 不移除
                    removeTagsSet.remove(tmp);
                }
            }
            // 从缓冲cache中移除biz_tag
            for (String tag : removeTagsSet) {
                cache.remove(tag);
                logger.info("Remove tag {} from IdCache", tag);
            }
        } catch (Exception e) {
            logger.warn("update cache from db exception", e);
        } finally {
            sw.stop("updateCacheFromDb");
        }
    }

    @Override
    public Result get(final String key) {
        if (!initOK) {
            // 保证初始化init()完毕再执行后面的代码
            return new Result(EXCEPTION_ID_IDCACHE_INIT_FALSE, Status.EXCEPTION);
        }
        // 这里的cache是在init()中初始化的, 这里的key就是biz_tag
        if (cache.containsKey(key)) {
            SegmentBuffer buffer = cache.get(key);
            // 如果没有初始化, 就进行一次初始化, 双重if判断
            if (!buffer.isInitOk()) {
                synchronized (buffer) {
                    if (!buffer.isInitOk()) {
                        try {
                            // 进行Segment的初始化, 设置本号段的初值1, 最大值2001, 以及步长2000
                            updateSegmentFromDb(key, buffer.getCurrent());
                            logger.info("Init buffer. Update leafkey {} {} from db", key, buffer.getCurrent());
                            // 标记初始化成功, 后面就不用再初始化这个biz_tag的Segment了
                            buffer.setInitOk(true);
                        } catch (Exception e) {
                            logger.warn("Init buffer {} exception", buffer.getCurrent(), e);
                        }
                    }
                }
            }
            // 直接从号段里取出Id
            return getIdFromSegmentBuffer(cache.get(key));
        }
        return new Result(EXCEPTION_ID_KEY_NOT_EXISTS, Status.EXCEPTION);
    }

    public void updateSegmentFromDb(String key, Segment segment) {
        StopWatch sw = new Slf4JStopWatch();
        SegmentBuffer buffer = segment.getBuffer();
        LeafAlloc leafAlloc;
        if (!buffer.isInitOk()) {
            // Segment第一次进来, 还没有初始化, 就走这个if判断
            // UPDATE leaf_alloc SET max_id = max_id + step WHERE biz_tag = #{tag}
            // SELECT biz_tag, max_id, step FROM leaf_alloc WHERE biz_tag = #{tag}
            leafAlloc = dao.updateMaxIdAndGetLeafAlloc(key);
            // 获取step步长2000, 设置到SegmentBuffer里
            buffer.setStep(leafAlloc.getStep());
            buffer.setMinStep(leafAlloc.getStep());//leafAlloc中的step为DB中的step
        } else if (buffer.getUpdateTimestamp() == 0) {
            // UPDATE leaf_alloc SET max_id = max_id + step WHERE biz_tag = #{tag}
            // SELECT biz_tag, max_id, step FROM leaf_alloc WHERE biz_tag = #{tag}
            leafAlloc = dao.updateMaxIdAndGetLeafAlloc(key);
            // Segment第二次进来, 就走这个if判断
            // 获取step步长2000, 设置到SegmentBuffer里
            buffer.setUpdateTimestamp(System.currentTimeMillis());
            buffer.setStep(leafAlloc.getStep());
            buffer.setMinStep(leafAlloc.getStep());//leafAlloc中的step为DB中的step
        } else {
            // 如果是第N次进来, 就走这个if判断
            long duration = System.currentTimeMillis() - buffer.getUpdateTimestamp();
            int nextStep = buffer.getStep();
            if (duration < SEGMENT_DURATION) {
                // 15分钟内, 号段就用尽了, 就将nextStep扩大两倍, 最大不超过1000000
                if (nextStep * 2 > MAX_STEP) {
                    //do nothing
                } else {
                    nextStep = nextStep * 2;
                }
            } else if (duration < SEGMENT_DURATION * 2) {
                // 30分钟内, 号段就用尽了, 不做任何处理
                //do nothing with nextStep
            } else {
                // 30分钟外, 号段用尽了, 就将nextStep缩小两倍, 直到最初的大小
                nextStep = nextStep / 2 >= buffer.getMinStep() ? nextStep / 2 : nextStep;
            }
            logger.info("leafKey[{}], step[{}], duration[{}mins], nextStep[{}]", key, buffer.getStep(), String.format("%.2f",((double)duration / (1000 * 60))), nextStep);
            LeafAlloc temp = new LeafAlloc();
            temp.setKey(key);
            temp.setStep(nextStep);
            // UPDATE leaf_alloc SET max_id = max_id + #{step} WHERE biz_tag = #{key}
            // SELECT biz_tag, max_id, step FROM leaf_alloc WHERE biz_tag = #{tag}
            // 这里和前两次不同的地方在于, step步长是自定义的
            leafAlloc = dao.updateMaxIdByCustomStepAndGetLeafAlloc(temp);
            buffer.setUpdateTimestamp(System.currentTimeMillis());
            buffer.setStep(nextStep);
            buffer.setMinStep(leafAlloc.getStep());//leafAlloc的step为DB中的step
        }
        // must set value before set max
        long value = leafAlloc.getMaxId() - buffer.getStep();
        // 设置本号段的初值, 最大值, 以及步长
        segment.getValue().set(value);
        segment.setMax(leafAlloc.getMaxId());
        segment.setStep(buffer.getStep());
        sw.stop("updateSegmentFromDb", key + " " + segment);
    }

    public Result getIdFromSegmentBuffer(final SegmentBuffer buffer) {
        while (true) {
            // 加读锁
            buffer.rLock().lock();
            try {
                // 拿到当前号段
                final Segment segment = buffer.getCurrent();
                // 每次取号时, 都判断是否需要准备下一个号段
                if (!buffer.isNextReady() && (segment.getIdle() < 0.9 * segment.getStep()) && buffer.getThreadRunning().compareAndSet(false, true)) {
                    // 下一个号段没有准备好 && 空闲号数量 < 0.9 * 步长 && 没有后台刷新号段异步任务在跑
                    // 如果满足条件, 就让并发进来的其中一个线程(CAS), 开启一个后台异步任务, 去更新号段
                    service.execute(new Runnable() {
                        @Override
                        public void run() {
                            // 取出下一个Segment
                            Segment next = buffer.getSegments()[buffer.nextPos()];
                            boolean updateOk = false;
                            try {
                                // 从数据库取出下一个号段, 赋值给这下一个Segment
                                updateSegmentFromDb(buffer.getKey(), next);
                                updateOk = true;
                                logger.info("update segment {} from db {}", buffer.getKey(), next);
                            } catch (Exception e) {
                                logger.warn(buffer.getKey() + " updateSegmentFromDb exception", e);
                            } finally {
                                if (updateOk) {
                                    // 更新成功, 就修改nextReady=true, 标记已经准备好下一个号段了
                                    buffer.wLock().lock();
                                    buffer.setNextReady(true);
                                    buffer.getThreadRunning().set(false);
                                    buffer.wLock().unlock();
                                } else {
                                    // 更新失败
                                    buffer.getThreadRunning().set(false);
                                }
                            }
                        }
                    });
                }
                // 从当前号段中取号, 内存运算特别快
                long value = segment.getValue().getAndIncrement();
                // 如果取的号在当前号段范围内, 就返回取号成功
                if (value < segment.getMax()) {
                    return new Result(value, Status.SUCCESS);
                }
                // 否则就继续waitAndSleep(buffer)
            } finally {
                // 解锁读锁
                buffer.rLock().unlock();
            }
            // 取号失败了, 超出了当前号段的范围, 这时后台异步任务肯定在准备下一个号段
            // 如果刷新下一个号段的任务正在执行, 就睡眠10ms
            waitAndSleep(buffer);
            // 加写锁, 只有一个线程能进来
            buffer.wLock().lock();
            try {
                // 取当前号段, 可能睡眠10ms后，下一号段已经加载完毕了
                final Segment segment = buffer.getCurrent();
                long value = segment.getValue().getAndIncrement();
                if (value < segment.getMax()) {
                    // 当第一个加写锁成功的线程切换了双缓冲, 后续所有线程都可以进行取号了
                    return new Result(value, Status.SUCCESS);
                }
                // 第一个加写锁成功的线程
                // 如果下一个号段加载完毕了, 就切换双缓冲, 重新再执行一下这个while(true)循环
                if (buffer.isNextReady()) {
                    buffer.switchPos();
                    buffer.setNextReady(false);
                } else {
                    logger.error("Both two segments in {} are not ready!", buffer);
                    return new Result(EXCEPTION_ID_TWO_SEGMENTS_ARE_NULL, Status.EXCEPTION);
                }
            } finally {
                // 解锁写锁
                buffer.wLock().unlock();
            }
        }
    }

    private void waitAndSleep(SegmentBuffer buffer) {
        int roll = 0;
        // 如果刷新下一个号段的任务正在执行, 就睡眠10ms
        while (buffer.getThreadRunning().get()) {
            roll += 1;
            if(roll > 10000) {
                try {
                    TimeUnit.MILLISECONDS.sleep(10);
                    break;
                } catch (InterruptedException e) {
                    logger.warn("Thread {} Interrupted",Thread.currentThread().getName());
                    break;
                }
            }
        }
    }

    public List<LeafAlloc> getAllLeafAllocs() {
        return dao.getAllLeafAllocs();
    }

    public Map<String, SegmentBuffer> getCache() {
        return cache;
    }

    public IDAllocDao getDao() {
        return dao;
    }

    public void setDao(IDAllocDao dao) {
        this.dao = dao;
    }
}
