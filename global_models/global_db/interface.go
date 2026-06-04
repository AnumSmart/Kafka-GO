// Абстракция для самой БД
package global_db

import "context"

// Pool — абстракция БД, общая для всех
type Pool interface {
	Exec(ctx context.Context, sql string, args ...any) (int64, error)
	QueryRow(ctx context.Context, sql string, args ...any) Row
	Query(ctx context.Context, sql string, args ...any) (Rows, error)
	Begin(ctx context.Context) (Tx, error)
	Close() error
}

// обстракция для row (для одной записи)
type Row interface {
	Scan(dest ...any) error
}

// обстракция для rowы (для нескольких записей)
type Rows interface {
	Next() bool
	Scan(dest ...any) error
	Close()
	Err() error
}

// абстракция для транзакций
type Tx interface {
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
	Exec(ctx context.Context, sql string, arguments ...any) (int64, error)
	QueryRow(ctx context.Context, sql string, arguments ...any) Row
	Query(ctx context.Context, sql string, arguments ...any) (Rows, error)
}
