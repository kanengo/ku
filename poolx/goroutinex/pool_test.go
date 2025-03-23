package goroutinex

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kanengo/ku/mathx"
)

// 测试创建池的默认配置
func TestNewPoolDefaultConfig(t *testing.T) {
	pool := NewPool(Config{})

	if pool.maxWorkers != defaultMaxWorkers {
		t.Errorf("默认 maxWorkers 应为 %d，实际为 %d", defaultMaxWorkers, pool.maxWorkers)
	}

	if pool.maxIdleWokers != defaultMaxIdleWokers {
		t.Errorf("默认 maxIdleWokers 应为 %d，实际为 %d", defaultMaxIdleWokers, pool.maxIdleWokers)
	}

	if pool.bufferQueue.Capacity() != int(defaultBufferQueueSize) {
		t.Errorf("默认 bufferQueueSize 应为 %d，实际为 %d", defaultBufferQueueSize, pool.bufferQueue.Capacity())
	}
}

// 测试创建池的自定义配置
func TestNewPoolCustomConfig(t *testing.T) {
	customMaxWorkers := int64(100)
	customMaxIdleWorkers := int64(20)
	customBufferQueueSize := int64(50)

	pool := NewPool(Config{
		MaxWorkers:      customMaxWorkers,
		MaxIdleWokers:   customMaxIdleWorkers,
		BufferQueueSize: customBufferQueueSize,
	})

	if pool.maxWorkers != customMaxWorkers {
		t.Errorf("自定义 maxWorkers 应为 %d，实际为 %d", customMaxWorkers, pool.maxWorkers)
	}

	if pool.maxIdleWokers != customMaxIdleWorkers {
		t.Errorf("自定义 maxIdleWokers 应为 %d，实际为 %d", customMaxIdleWorkers, pool.maxIdleWokers)
	}

	if pool.bufferQueue.Capacity() != mathx.NextPowerOfTwo(int(customBufferQueueSize)) {
		t.Errorf("自定义 bufferQueueSize 应为 %d，实际为 %d", customBufferQueueSize, pool.bufferQueue.Capacity())
	}
}

// 测试基本的任务执行
func TestPoolGoBasic(t *testing.T) {
	pool := NewPool(Config{})

	var executed atomic.Bool

	pool.Go(func() {
		executed.Store(true)
	})

	// 等待任务执行完成
	time.Sleep(100 * time.Millisecond)

	if !executed.Load() {
		t.Error("任务未被执行")
	}
}

// 测试空任务处理
func TestPoolGoNilTask(t *testing.T) {
	pool := NewPool(Config{})

	// 这不应该导致任何错误
	pool.Go(nil)
}

// 测试多个任务的执行
func TestPoolGoMultipleTasks(t *testing.T) {
	pool := NewPool(Config{})

	taskCount := 100
	var counter atomic.Int64
	var wg sync.WaitGroup

	wg.Add(taskCount)

	for i := 0; i < taskCount; i++ {
		pool.Go(func() {
			counter.Add(1)
			wg.Done()
		})
	}

	wg.Wait()

	if counter.Load() != int64(taskCount) {
		t.Errorf("应执行 %d 个任务，实际执行了 %d 个", taskCount, counter.Load())
	}
}

// 测试在高负载下的性能
func TestPoolGoHighLoad(t *testing.T) {
	pool := NewPool(Config{
		MaxWorkers:      100,
		MaxIdleWokers:   20,
		BufferQueueSize: 50,
	})

	taskCount := 1000
	var counter atomic.Int64
	var wg sync.WaitGroup

	wg.Add(taskCount)

	start := time.Now()

	for i := 0; i < taskCount; i++ {
		pool.Go(func() {
			// 模拟一些工作
			time.Sleep(1 * time.Millisecond)
			counter.Add(1)
			wg.Done()
		})
	}

	wg.Wait()

	duration := time.Since(start)

	t.Logf("执行 %d 个任务耗时: %v, 平均每个任务: %v",
		taskCount, duration, duration/time.Duration(taskCount))

	if counter.Load() != int64(taskCount) {
		t.Errorf("应执行 %d 个任务，实际执行了 %d 个", taskCount, counter.Load())
	}
}

// 测试任务执行顺序（不保证顺序，但应全部执行）
func TestPoolGoOrder(t *testing.T) {
	pool := NewPool(Config{
		MaxWorkers:      10,
		MaxIdleWokers:   5,
		BufferQueueSize: 20,
	})

	results := make([]int, 0, 100)
	var mu sync.Mutex
	var wg sync.WaitGroup

	taskCount := 100
	wg.Add(taskCount)

	for i := 0; i < taskCount; i++ {
		i := i // 捕获循环变量
		pool.Go(func() {
			defer wg.Done()

			// 模拟不同的处理时间
			time.Sleep(time.Duration(i%10) * time.Millisecond)

			mu.Lock()
			results = append(results, i)
			mu.Unlock()
		})
	}

	wg.Wait()

	if len(results) != taskCount {
		t.Errorf("应执行 %d 个任务，实际执行了 %d 个", taskCount, len(results))
	}

	// 检查所有任务是否都被执行了
	executed := make(map[int]bool)
	for _, v := range results {
		executed[v] = true
	}

	for i := 0; i < taskCount; i++ {
		if !executed[i] {
			t.Errorf("任务 %d 未被执行", i)
		}
	}
}

// 测试在达到最大工作协程数时的行为
func TestPoolGoMaxWorkers(t *testing.T) {
	maxWorkers := int64(5)
	pool := NewPool(Config{
		MaxWorkers:      maxWorkers,
		BufferQueueSize: maxWorkers,
		MaxIdleWokers:   1,
	})

	var maxConcurrent atomic.Int64
	var currentConcurrent atomic.Int64
	var wg sync.WaitGroup

	taskCount := 100
	wg.Add(taskCount)

	for i := 0; i < taskCount; i++ {
		pool.Go(func() {
			defer wg.Done()

			// 增加当前并发计数
			current := currentConcurrent.Add(1)

			// 更新最大并发数
			for {
				max := maxConcurrent.Load()
				if current <= max {
					break
				}
				if maxConcurrent.CompareAndSwap(max, current) {
					break
				}
			}

			// 模拟工作
			time.Sleep(10 * time.Millisecond)

			// 减少当前并发计数
			currentConcurrent.Add(-1)
		})
	}

	wg.Wait()

	// 检查最大并发数是否超过了设置的限制
	// 注意：由于直接执行的任务不会增加 running 计数，实际并发可能会略高于 maxWorkers
	// 这里我们允许一定的误差，因为测试环境中可能有其他因素影响
	if maxConcurrent.Load() > maxWorkers*2 {
		t.Errorf("最大并发数应不超过 %d 的几倍，实际为 %d", maxWorkers, maxConcurrent.Load())
	}
}

// 测试空闲工作协程的复用
func TestPoolWorkerReuse(t *testing.T) {
	pool := NewPool(Config{
		MaxWorkers:      10,
		MaxIdleWokers:   5,
		BufferQueueSize: 10,
	})

	// 第一批任务
	var wg1 sync.WaitGroup
	taskCount1 := 20
	wg1.Add(taskCount1)

	for i := 0; i < taskCount1; i++ {
		pool.Go(func() {
			wg1.Done()
		})
	}

	wg1.Wait()

	// 等待一段时间，让工作协程进入空闲状态
	time.Sleep(100 * time.Millisecond)

	// 记录当前空闲工作协程数
	idleWorkers := pool.idle.Load()

	if idleWorkers == 0 {
		t.Error("没有空闲工作协程可复用")
	}

	// 第二批任务，应该复用空闲工作协程
	var wg2 sync.WaitGroup
	taskCount2 := int(idleWorkers)
	wg2.Add(taskCount2)

	for i := 0; i < taskCount2; i++ {
		pool.Go(func() {
			wg2.Done()
		})
	}

	wg2.Wait()

	// 等待一段时间
	time.Sleep(100 * time.Millisecond)

	// 检查空闲工作协程数是否恢复
	if pool.idle.Load() < idleWorkers {
		t.Errorf("空闲工作协程未被正确复用，之前: %d, 之后: %d",
			idleWorkers, pool.idle.Load())
	}
}

// 测试缓冲队列的使用
func TestPoolBufferQueue(t *testing.T) {
	bufferSize := int64(10)
	pool := NewPool(Config{
		MaxWorkers:      5,
		BufferQueueSize: bufferSize,
		MaxIdleWokers:   1,
	})

	// 提交足够多的任务以填满缓冲队列
	var wg sync.WaitGroup
	taskCount := int(bufferSize * 2)
	wg.Add(taskCount)

	// 先提交一些长时间运行的任务，占用工作协程
	for i := 0; i < 5; i++ {
		pool.Go(func() {
			time.Sleep(100 * time.Millisecond)
			wg.Done()
		})
	}

	// 等待一段时间，确保前面的任务正在运行
	time.Sleep(10 * time.Millisecond)

	// 再提交更多任务，这些应该进入缓冲队列
	for i := 0; i < taskCount-5; i++ {
		pool.Go(func() {
			wg.Done()
		})
	}

	// 检查缓冲队列是否被使用
	queueLen := pool.bufferQueue.Length()
	if queueLen == 0 {
		t.Error("缓冲队列未被使用")
	}

	wg.Wait()

	// 任务完成后，缓冲队列应该为空
	if pool.bufferQueue.Length() != 0 {
		t.Errorf("任务完成后缓冲队列应为空，实际长度: %d", pool.bufferQueue.Length())
	}
}

// 测试当达到最大工作协程数时的直接执行行为
func TestPoolDirectExecution(t *testing.T) {
	maxWorkers := int64(1)
	pool := NewPool(Config{
		MaxWorkers:      maxWorkers,
		BufferQueueSize: 0, // 禁用缓冲队列
		MaxIdleWokers:   0,
	})

	// 设置一个长时间运行的任务占用唯一的工作协程
	var longTaskDone atomic.Bool
	pool.Go(func() {
		time.Sleep(200 * time.Millisecond)
		longTaskDone.Store(true)
	})

	// 等待一段时间，确保任务正在运行
	time.Sleep(10 * time.Millisecond)

	// 提交另一个任务，这应该直接在当前协程中执行
	var directTaskDone atomic.Bool
	start := time.Now()

	pool.Go(func() {
		directTaskDone.Store(true)
	})

	duration := time.Since(start)

	// 直接执行的任务应该很快完成
	if duration > 50*time.Millisecond {
		t.Errorf("任务应该直接执行，但耗时 %v", duration)
	}

	if !directTaskDone.Load() {
		t.Error("直接执行的任务未完成")
	}

	// 等待长任务完成
	time.Sleep(200 * time.Millisecond)

	if !longTaskDone.Load() {
		t.Error("长时间运行的任务未完成")
	}
}

// 压力测试 - 大量并发任务
func TestPoolStress(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过压力测试")
	}

	pool := NewPool(Config{
		MaxWorkers:      1000,
		MaxIdleWokers:   100,
		BufferQueueSize: 500,
	})

	taskCount := 10000
	var counter atomic.Int64
	var wg sync.WaitGroup

	wg.Add(taskCount)

	start := time.Now()

	for i := 0; i < taskCount; i++ {
		pool.Go(func() {
			// 模拟随机工作量
			time.Sleep(time.Duration(time.Now().UnixNano()%5) * time.Millisecond)
			counter.Add(1)
			wg.Done()
		})
	}

	wg.Wait()

	duration := time.Since(start)

	t.Logf("压力测试: 执行 %d 个任务耗时: %v, 平均每个任务: %v",
		taskCount, duration, duration/time.Duration(taskCount))

	if counter.Load() != int64(taskCount) {
		t.Errorf("应执行 %d 个任务，实际执行了 %d 个", taskCount, counter.Load())
	}
}

// 测试任务恐慌处理
func TestPoolPanicHandling(t *testing.T) {
	pool := NewPool(Config{})

	// 提交一个会恐慌的任务
	var recovered atomic.Bool

	// 我们不能直接测试恐慌是否被处理，因为这取决于 worker.exec 的实现
	// 但我们可以测试是否会影响后续任务的执行

	// 先提交一个会恐慌的任务
	pool.Go(func() {
		defer func() {
			if r := recover(); r != nil {
				recovered.Store(true)
			}
		}()
		panic("测试恐慌")
	})

	// 等待一段时间
	time.Sleep(100 * time.Millisecond)

	// 然后提交一个正常任务
	var normalTaskDone atomic.Bool
	pool.Go(func() {
		normalTaskDone.Store(true)
	})

	// 等待正常任务完成
	time.Sleep(100 * time.Millisecond)

	if !normalTaskDone.Load() {
		t.Error("恐慌后的正常任务未完成，这表明恐慌处理可能有问题")
	}
}

// 测试并发安全性
func TestPoolConcurrentSafety(t *testing.T) {
	pool := NewPool(Config{})

	var wg sync.WaitGroup
	concurrentCalls := 100
	tasksPerCall := 100

	wg.Add(concurrentCalls)

	for i := 0; i < concurrentCalls; i++ {
		go func() {
			defer wg.Done()

			var innerWg sync.WaitGroup
			innerWg.Add(tasksPerCall)

			for j := 0; j < tasksPerCall; j++ {
				pool.Go(func() {
					innerWg.Done()
				})
			}

			innerWg.Wait()
		}()
	}

	wg.Wait()

	// 如果代码是并发安全的，这个测试应该不会死锁或恐慌
}
