package troupe

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	_ "modernc.org/sqlite" // 纯 Go SQLite 驱动（无 CGO，保住交叉编译）
)

// SQLitePropStore 本地开发 Provider（M2）：进程级持久（重开恢复），
// CrossNodeSafe=false——不承诺节点丢失或跨机恢复（DESIGN §7）。
// CAS 与 fencing 均为单条 SQL 原子语句；幂等 journal 与 Props 同事务提交。
type SQLitePropStore struct {
	db        *sql.DB
	ownership *SQLiteOwnershipStore
}

// SQLiteOwnershipStore SQLite 所有权表：acquire 单调递增 epoch。
type SQLiteOwnershipStore struct {
	db *sql.DB
}

// OpenSQLite 打开（或创建）SQLite 存储；path 为文件路径（WAL 模式）。
// 返回 PropStore 与配套 OwnershipStore（同一数据库）。
func OpenSQLite(path string) (*SQLitePropStore, *SQLiteOwnershipStore, error) {
	dsn := "file:" + path + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(ON)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("打开 SQLite 失败：%w", err)
	}
	// modernc/sqlite 单写者：限制连接避免 database locked
	db.SetMaxOpenConns(1)
	own := &SQLiteOwnershipStore{db: db}
	store := &SQLitePropStore{db: db, ownership: own}
	if err := store.migrate(); err != nil {
		db.Close()
		return nil, nil, err
	}
	return store, own, nil
}

func (s *SQLitePropStore) migrate() error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS troupe_props (
			actor_id TEXT PRIMARY KEY,
			revision INTEGER NOT NULL,
			schema_version INTEGER NOT NULL,
			data TEXT NOT NULL,
			epoch INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS troupe_ownership (
			actor_id TEXT PRIMARY KEY,
			epoch INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS troupe_journal (
			actor_id TEXT NOT NULL,
			key TEXT NOT NULL,
			fingerprint TEXT NOT NULL,
			ok INTEGER NOT NULL,
			value TEXT NOT NULL DEFAULT '',
			error_class TEXT NOT NULL DEFAULT '',
			error_message TEXT NOT NULL DEFAULT '',
			expires_at INTEGER NOT NULL,
			PRIMARY KEY (actor_id, key)
		)`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.Exec(stmt); err != nil {
			return fmt.Errorf("SQLite 迁移失败：%w", err)
		}
	}
	return nil
}

func (s *SQLitePropStore) Close() error { return s.db.Close() }

func (s *SQLitePropStore) CrossNodeSafe() bool { return false }

func (s *SQLitePropStore) OwnershipStore() *SQLiteOwnershipStore { return s.ownership }

func (s *SQLitePropStore) Read(actorID ActorID) (*PropRecord, error) {
	row := s.db.QueryRow(
		`SELECT revision, schema_version, data, epoch FROM troupe_props WHERE actor_id = ?`,
		actorID.Canonical(),
	)
	var rec PropRecord
	err := row.Scan(&rec.Revision, &rec.SchemaVersion, &rec.Data, &rec.Epoch)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("SQLite read 失败：%w", err)
	}
	return &rec, nil
}

func (s *SQLitePropStore) Create(actorID ActorID, schemaVersion int, data string, epoch uint64) (PropRecord, error) {
	// 存在检查优先于 fencing（与 troupe.cr 顺序一致）。
	var exists int
	err := s.db.QueryRow(`SELECT 1 FROM troupe_props WHERE actor_id = ?`, actorID.Canonical()).Scan(&exists)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return PropRecord{}, fmt.Errorf("SQLite create 检查失败：%w", err)
	}
	if exists == 1 {
		return PropRecord{}, &ConflictError{Kind: ConflictExists, Actor: actorID.Canonical()}
	}
	if err := s.ownership.assertFencing(actorID, epoch); err != nil {
		return PropRecord{}, err
	}
	_, err = s.db.Exec(
		`INSERT INTO troupe_props (actor_id, revision, schema_version, data, epoch) VALUES (?, 1, ?, ?, ?)`,
		actorID.Canonical(), schemaVersion, data, epoch,
	)
	if err != nil {
		// 并发创建兜底：唯一约束冲突按 exists 分类。
		if strings.Contains(err.Error(), "UNIQUE") {
			return PropRecord{}, &ConflictError{Kind: ConflictExists, Actor: actorID.Canonical()}
		}
		return PropRecord{}, fmt.Errorf("SQLite create 失败：%w", err)
	}
	return PropRecord{Revision: 1, SchemaVersion: schemaVersion, Data: data, Epoch: epoch}, nil
}

func (s *SQLitePropStore) Write(actorID ActorID, expectedRevision uint64, schemaVersion int, data string, epoch uint64) (PropRecord, error) {
	return s.writeCore(actorID, expectedRevision, schemaVersion, data, epoch, nil)
}

// writeCore 单条 UPDATE 原子 CAS+fencing（WHERE 内同时校验 revision 与当前 epoch），
// journal 非空时同事务 upsert（CAS 失败整体不生效）。
func (s *SQLitePropStore) writeCore(actorID ActorID, expectedRevision uint64, schemaVersion int, data string, epoch uint64, journal []IdempotencyRecord) (PropRecord, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return PropRecord{}, fmt.Errorf("SQLite 事务失败：%w", err)
	}
	defer tx.Rollback()

	res, err := tx.Exec(
		`UPDATE troupe_props
		 SET revision = revision + 1, schema_version = ?, data = ?, epoch = ?
		 WHERE actor_id = ? AND revision = ?
		   AND ? = (SELECT epoch FROM troupe_ownership WHERE actor_id = ?)`,
		schemaVersion, data, epoch, actorID.Canonical(), expectedRevision, epoch, actorID.Canonical(),
	)
	if err != nil {
		return PropRecord{}, fmt.Errorf("SQLite write 失败：%w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return PropRecord{}, fmt.Errorf("SQLite rows 失败：%w", err)
	}
	if affected == 0 {
		// 拒绝分类（fencing 优先）：在**同一事务连接内**读当前 epoch，
		// 避免单连接池下与外部查询互相等锁。
		var curEpoch uint64
		err := tx.QueryRow(`SELECT epoch FROM troupe_ownership WHERE actor_id = ?`, actorID.Canonical()).Scan(&curEpoch)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return PropRecord{}, fmt.Errorf("SQLite fencing 分类失败：%w", err)
		}
		if errors.Is(err, sql.ErrNoRows) {
			curEpoch = 0
		}
		if curEpoch != epoch {
			return PropRecord{}, &CallRejectedError{
				Reason: RejectOwnership, Retryable: false, Actor: actorID.Canonical(),
				Detail: fmt.Sprintf("epoch %d 已被 epoch %d 取代，拒绝写入", epoch, curEpoch),
			}
		}
		return PropRecord{}, &ConflictError{Kind: ConflictRevision, Actor: actorID.Canonical()}
	}
	for _, entry := range journal {
		_, err = tx.Exec(
			`INSERT INTO troupe_journal (actor_id, key, fingerprint, ok, value, error_class, error_message, expires_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?)
			 ON CONFLICT(actor_id, key) DO UPDATE SET
			   fingerprint = excluded.fingerprint, ok = excluded.ok, value = excluded.value,
			   error_class = excluded.error_class, error_message = excluded.error_message,
			   expires_at = excluded.expires_at`,
			actorID.Canonical(), entry.Key, entry.Fingerprint, entry.OK, entry.Value,
			entry.ErrorClass, entry.ErrorMessage, entry.ExpiresAt,
		)
		if err != nil {
			return PropRecord{}, fmt.Errorf("SQLite journal 写失败：%w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return PropRecord{}, fmt.Errorf("SQLite 提交失败：%w", err)
	}
	return PropRecord{Revision: expectedRevision + 1, SchemaVersion: schemaVersion, Data: data, Epoch: epoch}, nil
}

func (s *SQLitePropStore) WriteJournaled(actorID ActorID, expectedRevision uint64, schemaVersion int, data string, epoch uint64, journal []IdempotencyRecord) (PropRecord, error) {
	return s.writeCore(actorID, expectedRevision, schemaVersion, data, epoch, journal)
}

func (s *SQLitePropStore) LoadJournal(actorID ActorID, nowUnix int64) ([]IdempotencyRecord, error) {
	rows, err := s.db.Query(
		`SELECT key, fingerprint, ok, value, error_class, error_message, expires_at
		 FROM troupe_journal WHERE actor_id = ? AND expires_at > ?`,
		actorID.Canonical(), nowUnix,
	)
	if err != nil {
		return nil, fmt.Errorf("SQLite journal 读失败：%w", err)
	}
	defer rows.Close()
	var out []IdempotencyRecord
	for rows.Next() {
		var r IdempotencyRecord
		if err := rows.Scan(&r.Key, &r.Fingerprint, &r.OK, &r.Value, &r.ErrorClass, &r.ErrorMessage, &r.ExpiresAt); err != nil {
			return nil, fmt.Errorf("SQLite journal scan 失败：%w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// Acquire 取得下一个单调 epoch（upsert + RETURNING 单语句）。
func (o *SQLiteOwnershipStore) Acquire(actorID ActorID) uint64 {
	var epoch uint64
	err := o.db.QueryRow(
		`INSERT INTO troupe_ownership (actor_id, epoch) VALUES (?, 1)
		 ON CONFLICT(actor_id) DO UPDATE SET epoch = troupe_ownership.epoch + 1
		 RETURNING epoch`,
		actorID.Canonical(),
	).Scan(&epoch)
	if err != nil {
		// 兼容无 RETURNING 的构建：退化为查询+更新（单连接内安全）。
		_ = o.db.QueryRow(`SELECT epoch FROM troupe_ownership WHERE actor_id = ?`, actorID.Canonical()).Scan(&epoch)
		epoch++
		_, _ = o.db.Exec(`UPDATE troupe_ownership SET epoch = ? WHERE actor_id = ?`, epoch, actorID.Canonical())
	}
	return epoch
}

func (o *SQLiteOwnershipStore) Current(actorID ActorID) uint64 {
	var epoch uint64
	err := o.db.QueryRow(`SELECT epoch FROM troupe_ownership WHERE actor_id = ?`, actorID.Canonical()).Scan(&epoch)
	if err != nil {
		return 0
	}
	return epoch
}

func (o *SQLiteOwnershipStore) Renew(actorID ActorID, epoch uint64) bool {
	res, err := o.db.Exec(
		`UPDATE troupe_ownership SET epoch = epoch WHERE actor_id = ? AND epoch = ?`,
		actorID.Canonical(), epoch,
	)
	if err != nil {
		return false
	}
	n, _ := res.RowsAffected()
	return n == 1
}

func (o *SQLiteOwnershipStore) assertFencing(actorID ActorID, epoch uint64) error {
	current := o.Current(actorID)
	if current != epoch {
		return &CallRejectedError{
			Reason:    RejectOwnership,
			Retryable: false,
			Actor:     actorID.Canonical(),
			Detail:    fmt.Sprintf("epoch %d 已被 epoch %d 取代，拒绝写入", epoch, current),
		}
	}
	return nil
}
