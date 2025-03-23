package queuex

import (
	"sync"
	"sync/atomic"
	"testing"
)

// 测试数据结构体
type testData struct {
	id    int
	value string
}

// 测试1: 基本功能测试
func TestBasicOperations(t *testing.T) {
	q := NewRingQueue[int](8)

	// 空队列检测
	if _, ok := q.Dequeue(); ok {
		t.Error("空队列返回了数据")
	}

	// 单元素入队出队
	if !q.Enqueue(42) {
		t.Error("入队失败")
	}
	if val, ok := q.Dequeue(); !ok || val != 42 {
		t.Error("出队数据错误")
	}

	// 队列满检测
	for i := 0; i < 8; i++ {
		if !q.Enqueue(i) {
			t.Fatal("队列未满时入队失败")
		}
	}
	if q.Enqueue(9) {
		t.Error("队列已满时入队成功")
	}
}

// 测试2: 指针类型操作
func TestPointerOperations(t *testing.T) {
	q := NewRingQueue[*testData](4)
	data := &testData{id: 1, value: "test"}

	if !q.Enqueue(data) {
		t.Error("指针入队失败")
	}
	if ptr, ok := q.Dequeue(); !ok || ptr != data {
		t.Error("指针出队错误")
	}
}

// 测试3: 并发生产者
func TestConcurrentProducers(t *testing.T) {
	var wg sync.WaitGroup
	const producers = 8
	const perCount = 1000

	q := NewRingQueue[int](producers * perCount)

	wg.Add(producers)
	for i := 0; i < producers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perCount; j++ {
				for !q.Enqueue(j) {
				}
			}
		}()
	}

	//消费者

	wg.Wait()

	// 验证总元素数量
	if q.Length() != producers*perCount {
		t.Errorf("队列长度错误, 期望 %d 实际 %d", producers*perCount, q.Length())
	}

	consumed := 0
	for i := 0; i < producers*perCount; i++ {
		if _, ok := q.Dequeue(); ok {
			consumed++
		}
	}
	if _, ok := q.Dequeue(); ok {
		t.Error("队列已空时出队成功")
	}
	if consumed != producers*perCount {
		t.Errorf("消费数量错误, 期望 %d 实际 %d", producers*perCount, consumed)
	}
}

// 测试4: 生产消费同时进行
func TestConcurrentProducersConsumers(t *testing.T) {
	q := NewRingQueue[int](1024)
	var wg sync.WaitGroup
	const workers = 8
	const items = 1000000

	// 生产者
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < items; j++ {
				for !q.Enqueue(1) {
				}
			}
		}()
	}

	// 消费者
	total := int64(0)
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			sum := 0
			for j := 0; j < items; j++ {
				if val, ok := q.Dequeue(); ok {
					sum += val
				} else {
					j-- // 重试
				}
			}
			atomic.AddInt64(&total, int64(sum))
		}()
	}

	wg.Wait()

	// 验证数据完整性

	if total != workers*items {
		t.Errorf("数据校验错误, 期望 %d 实际 %d", workers*items, total)
	}
}

// 测试5: 类型安全验证
func TestTypeSafety(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Error("泛型类型检查失败")
		}
	}()

	q1 := NewRingQueue[string](2)
	q1.Enqueue("hello")
	if v, _ := q1.Dequeue(); v != "hello" {
		t.Error("字符串类型错误")
	}

	q2 := NewRingQueue[testData](2)
	q2.Enqueue(testData{id: 1})
	if v, _ := q2.Dequeue(); v.id != 1 {
		t.Error("结构体类型错误")
	}
}

// 测试6: 边界条件
func TestEdgeCases(t *testing.T) {
	// 测试大小为1的队列
	q := NewRingQueue[int](1)
	if !q.Enqueue(1) {
		t.Error("大小1队列入队失败")
	}
	if q.Enqueue(2) {
		t.Error("大小1队列满时入队成功")
	}
	if v, _ := q.Dequeue(); v != 1 {
		t.Error("大小1队列出队错误")
	}
}

func TestFull(t *testing.T) {
	q := NewRingQueue[int](8)
	for i := 0; i < 8; i++ {
		if !q.Enqueue(i) {
			t.Error("队列未满时入队失败")
		}
	}
	if q.Enqueue(9) {
		t.Error("队列已满时入队成功")
	}

	if q.Full() != true {
		t.Error("队列已满时Full返回false")
	}
}

// 测试7: 构造函数校验
func TestInvalidSize(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("非2的幂次大小未触发panic")
		}
	}()
	NewRingQueue[int](7) // 应该触发panic
}

// 测试8: 指针生命周期管理
func TestPointerLifecycle(t *testing.T) {
	q := NewRingQueue[*int](4)
	var val int = 42

	q.Enqueue(&val)
	if ptr, ok := q.Dequeue(); !ok || *ptr != 42 {
		t.Error("指针数据错误")
	}

	// 修改原始值不应影响队列
	val = 100
	if _, ok := q.Dequeue(); ok {
		t.Error("已出队指针重复返回")
	}
}

// 基准测试: 单线程性能
func BenchmarkSingleThread(b *testing.B) {
	q := NewRingQueue[int](8192)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Enqueue(i)
		q.Dequeue()
	}
}

// 基准测试: 多生产者竞争
func BenchmarkConcurrentEnqueue(b *testing.B) {
	q := NewRingQueue[int](1000000)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			q.Enqueue(i)
			i++
		}
	})
}
