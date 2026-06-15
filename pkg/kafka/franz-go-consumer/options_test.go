package franzgoconsumer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Тест: DefaultOptions - значения по умолчанию
// ============================================================================

func TestDefaultOptions(t *testing.T) {
	// Вызываем функцию
	opts := DefaultOptions()

	// Проверяем, что результат не nil
	require.NotNil(t, opts, "DefaultOptions should never return nil")

	// Проверяем каждое поле
	assert.Equal(t, 10*time.Second, opts.StatsPrintInterval,
		"StatsPrintInterval default should be 10 seconds")

	assert.Equal(t, time.Duration(0), opts.CommitInterval,
		"CommitInterval default should be 0 (manual commit)")

	assert.False(t, opts.EnableDebugLog,
		"EnableDebugLog default should be false")
}

// ============================================================================
// Тест доп.: DefaultOptions возвращает новый экземпляр каждый раз
// ============================================================================

func TestDefaultOptions_ReturnsNewInstance(t *testing.T) {
	opts1 := DefaultOptions()
	opts2 := DefaultOptions()

	// Изменяем первый экземпляр
	opts1.StatsPrintInterval = 30 * time.Second
	opts1.EnableDebugLog = true

	// Второй экземпляр не должен измениться
	assert.Equal(t, 10*time.Second, opts2.StatsPrintInterval,
		"Second instance should still have default values")
	assert.False(t, opts2.EnableDebugLog,
		"Second instance should still have default values")

	// Это разные объекты в памяти
	assert.NotSame(t, opts1, opts2, "Should return different instances")
}

// ============================================================================
// Тест: Validate - коррекция некорректных значений
// ============================================================================

func TestValidate_CorrectsNegativeStatsPrintInterval(t *testing.T) {
	opts := &ConsumerOptions{
		StatsPrintInterval: -5 * time.Second, // отрицательное значение
		CommitInterval:     1 * time.Second,
		EnableDebugLog:     true,
	}

	err := opts.Validate()

	assert.NoError(t, err, "Validate should not return error for negative interval")
	assert.Equal(t, 10*time.Second, opts.StatsPrintInterval,
		"Negative StatsPrintInterval should be corrected to default (10s)")
	assert.Equal(t, 1*time.Second, opts.CommitInterval,
		"CommitInterval should remain unchanged")
	assert.True(t, opts.EnableDebugLog, "EnableDebugLog should remain unchanged")
}

func TestValidate_CorrectsZeroStatsPrintInterval(t *testing.T) {
	opts := &ConsumerOptions{
		StatsPrintInterval: 0, // ноль
		CommitInterval:     0,
		EnableDebugLog:     false,
	}

	err := opts.Validate()

	assert.NoError(t, err)
	assert.Equal(t, 10*time.Second, opts.StatsPrintInterval,
		"Zero StatsPrintInterval should be corrected to 10s")
}

func TestValidate_KeepsValidStatsPrintInterval(t *testing.T) {
	opts := &ConsumerOptions{
		StatsPrintInterval: 30 * time.Second,
		CommitInterval:     5 * time.Second,
		EnableDebugLog:     true,
	}

	err := opts.Validate()

	assert.NoError(t, err)
	assert.Equal(t, 30*time.Second, opts.StatsPrintInterval,
		"Valid value should not be changed")
	assert.Equal(t, 5*time.Second, opts.CommitInterval)
	assert.True(t, opts.EnableDebugLog)
}

// ============================================================================
// Тест: Validate - коррекция отрицательного CommitInterval
// ============================================================================

func TestValidate_CorrectsNegativeCommitInterval(t *testing.T) {
	opts := &ConsumerOptions{
		StatsPrintInterval: 10 * time.Second,
		CommitInterval:     -3 * time.Second, // отрицательное значение
		EnableDebugLog:     false,
	}

	err := opts.Validate()

	assert.NoError(t, err)
	assert.Equal(t, time.Duration(0), opts.CommitInterval,
		"Negative CommitInterval should be corrected to 0 (manual commit)")
}

func TestValidate_KeepsValidCommitInterval(t *testing.T) {
	// табличные тесты
	testCases := []struct {
		name     string
		input    time.Duration
		expected time.Duration
	}{
		{"Zero (manual commit)", 0, 0},
		{"Positive (auto commit)", 5 * time.Second, 5 * time.Second},
		{"Large value", 1 * time.Hour, 1 * time.Hour},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			opts := &ConsumerOptions{
				StatsPrintInterval: 10 * time.Second,
				CommitInterval:     tc.input,
				EnableDebugLog:     false,
			}

			err := opts.Validate()

			assert.NoError(t, err)
			assert.Equal(t, tc.expected, opts.CommitInterval)
		})
	}
}

// ============================================================================
// Тест: Validate - граничные случаи
// ============================================================================

func TestValidate_WithNilOptions(t *testing.T) {
	// Внимание: вызов Validate на nil приведёт к панике
	// Этот тест показывает, что мы НЕ должны вызывать Validate на nil
	var opts *ConsumerOptions

	// Проверяем, что при nil будет паника (ожидаемое поведение)
	assert.Panics(t, func() {
		opts.Validate()
	}, "Calling Validate on nil should panic")
}

// ============================================================================
// Тест: Validate - все поля корректны
// ============================================================================

func TestValidate_AllFieldsValid(t *testing.T) {
	opts := &ConsumerOptions{
		StatsPrintInterval: 15 * time.Second,
		CommitInterval:     2 * time.Second,
		EnableDebugLog:     true,
	}

	originalOpts := *opts // копируем для сравнения

	err := opts.Validate()

	assert.NoError(t, err)
	assert.Equal(t, originalOpts.StatsPrintInterval, opts.StatsPrintInterval,
		"StatsPrintInterval should not change")
	assert.Equal(t, originalOpts.CommitInterval, opts.CommitInterval,
		"CommitInterval should not change")
	assert.Equal(t, originalOpts.EnableDebugLog, opts.EnableDebugLog,
		"EnableDebugLog should not change")
}

// ============================================================================
// Тест: Validate - не меняет значения при валидных данных
// ============================================================================

func TestValidate_Idempotent(t *testing.T) {
	opts := &ConsumerOptions{
		StatsPrintInterval: 20 * time.Second,
		CommitInterval:     3 * time.Second,
		EnableDebugLog:     true,
	}

	// Первая валидация
	err1 := opts.Validate()
	assert.NoError(t, err1)

	valuesAfterFirst := *opts

	// Вторая валидация
	err2 := opts.Validate()
	assert.NoError(t, err2)

	// Значения не должны измениться после повторной валидации
	assert.Equal(t, valuesAfterFirst.StatsPrintInterval, opts.StatsPrintInterval)
	assert.Equal(t, valuesAfterFirst.CommitInterval, opts.CommitInterval)
	assert.Equal(t, valuesAfterFirst.EnableDebugLog, opts.EnableDebugLog)
}

// ============================================================================
// Дополнительный тест: создание ConsumerOptions через структуру
// ============================================================================

func TestConsumerOptions_StructInitialization(t *testing.T) {
	// Разные способы инициализации должны работать корректно

	// Способ 1: через DefaultOptions
	opts1 := DefaultOptions()

	// Способ 2: прямая инициализация
	opts2 := &ConsumerOptions{
		StatsPrintInterval: 10 * time.Second,
		CommitInterval:     0,
		EnableDebugLog:     false,
	}

	// Они должны быть эквивалентны
	assert.Equal(t, opts1.StatsPrintInterval, opts2.StatsPrintInterval)
	assert.Equal(t, opts1.CommitInterval, opts2.CommitInterval)
	assert.Equal(t, opts1.EnableDebugLog, opts2.EnableDebugLog)
}
