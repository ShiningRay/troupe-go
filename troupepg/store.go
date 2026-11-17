// Package troupepg 提供 PostgreSQL 生产 Provider（M3）：跨节点接管语义
// （CrossNodeSafe=true）。所有权带租期（数据库服务器时钟判定）；fencing
// （epoch+租期有效性）在单条 UPDATE 的 WHERE 内原子校验；幂等 journal 与
// Props 同事务提交。依赖 github.com/jackc/pgx/v5——核心包不 import 本包。
package troupepg

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	troupe "github.com/ShiningRay/troupe-go"
)

type config struct {
	instance string
	lease    time.Duration
}

// Option 配置 Provider。
type Option func(*config)

// WithInstance 设置本实例标识（ownership holder；跨实例接管判定依据）。
func WithInstance(id string) Option { return func(c *config) { c.instance = id } }

// WithLease 设置所有权租期（DB 时钟判定；默认 30s）。
func WithLease(d time.Duration) Option { return func(c *config) { c.lease = d } }

// OwnershipStore PostgreSQL 所有权：租期内跨实例 acquire 显式拒绝（可稍后重试），
// 同实例重取/过期重取 epoch 单调递增（fencing token）。
type OwnershipStore struct {
	pool *pgxpool.Pool
	cfg  config
}

// Store PostgreSQL PropStore + JournalingStore。
type Store struct {
	pool *pgxpool.Pool
	own  *OwnershipStore
	cfg  config
}

var _ troupe.PropStore = (*Store)(nil)
var _ troupe.JournalingStore = (*Store)(nil)
var _ troupe.OwnershipStore = (*OwnershipStore)(nil)

// Open 建立 连接池并迁移 schema。
func Open(ctx context.Context, url string, opts ...Option) (*Store, *OwnershipStore, error) {
	cfg := config{instance: "troupe-instance", lease: 30 * time.Second}
	for _, opt := range opts {
		opt(&cfg)
	}
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, nil, fmt.Errorf("pgxpool 建立失败：%w", err)
	}
	store := &Store{pool: pool, cfg: cfg}
	store.own = &OwnershipStore{pool: pool, cfg: cfg}
	if err := store.migrate(ctx); err != nil {
		pool.Close()
		return nil, nil, err
	}
	return store, store.own, nil
}

func (s *Store) migrate(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS troupe_props (
			actor_id TEXT PRIMARY KEY,
			revision BIGINT NOT NULL,
			schema_version INTEGER NOT NULL,
			data TEXT NOT NULL,
			epoch BIGINT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS troupe_ownership (
			actor_id TEXT PRIMARY KEY,
			holder TEXT NOT NULL,
			epoch BIGINT NOT NULL,
			lease_expires_at TIMESTAMPTZ NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS troupe_journal (
			actor_id TEXT NOT NULL,
			key TEXT NOT NULL,
			fingerprint TEXT NOT NULL,
			ok BOOLEAN NOT NULL,
			value TEXT NOT NULL DEFAULT '',
			error_class TEXT NOT NULL DEFAULT '',
			error_message TEXT NOT NULL DEFAULT '',
			expires_at BIGINT NOT NULL,
			PRIMARY KEY (actor_id, key)
		)`,
	}
	for _, stmt := range stmts {
		if _, err := s.pool.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("PostgreSQL 迁移失败：%w", err)
		}
	}
	return nil
}

func (s *Store) Close() { s.pool.Close() }

// CrossNodeSafe PostgreSQL Provider 支持跨节点接管语义。
func (s *Store) CrossNodeSafe() bool { return true }

func (s *Store) OwnershipStore() *OwnershipStore { return s.own }

func (s *Store) Read(ctx context.Context, actorID troupe.ActorID) (*troupe.PropRecord, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT revision, schema_version, data, epoch FROM troupe_props WHERE actor_id = $1`,
		actorID.Canonical(),
	)
	var rec troupe.PropRecord
	err := row.Scan(&rec.Revision, &rec.SchemaVersion, &rec.Data, &rec.Epoch)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("PG read 失败：%w", err)
	}
	return &rec, nil
}

func (s *Store) Create(ctx context.Context, actorID troupe.ActorID, schemaVersion int, data string, epoch uint64) (troupe.PropRecord, error) {
	// 存在检查优先于 fencing（与 troupe.cr 顺序一致）。
	var exists int
	err := s.pool.QueryRow(ctx, `SELECT 1 FROM troupe_props WHERE actor_id = $1`, actorID.Canonical()).Scan(&exists)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return troupe.PropRecord{}, fmt.Errorf("PG create 检查失败：%w", err)
	}
	if exists == 1 {
		return troupe.PropRecord{}, &troupe.ConflictError{Kind: troupe.ConflictExists, Actor: actorID.Canonical()}
	}
	if err := s.own.assertFencing(ctx, actorID, epoch); err != nil {
		return troupe.PropRecord{}, err
	}
	_, err = s.pool.Exec(ctx,
		`INSERT INTO troupe_props (actor_id, revision, schema_version, data, epoch) VALUES ($1, 1, $2, $3, $4)`,
		actorID.Canonical(), schemaVersion, data, epoch,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return troupe.PropRecord{}, &troupe.ConflictError{Kind: troupe.ConflictExists, Actor: actorID.Canonical()}
		}
		return troupe.PropRecord{}, fmt.Errorf("PG create 失败：%w", err)
	}
	return troupe.PropRecord{Revision: 1, SchemaVersion: schemaVersion, Data: data, Epoch: epoch}, nil
}

func (s *Store) Write(ctx context.Context, actorID troupe.ActorID, expectedRevision uint64, schemaVersion int, data string, epoch uint64) (troupe.PropRecord, error) {
	return s.writeCore(ctx, actorID, expectedRevision, schemaVersion, data, epoch, nil)
}

// writeCore 单条 UPDATE 原子 CAS+fencing：WHERE 同时校验 revision、当前 epoch
// 与租期有效性（DB 时钟 now()）；journal 非空时同事务 upsert（CAS 失败整体回滚）。
func (s *Store) writeCore(ctx context.Context, actorID troupe.ActorID, expectedRevision uint64, schemaVersion int, data string, epoch uint64, journal []troupe.IdempotencyRecord) (troupe.PropRecord, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return troupe.PropRecord{}, fmt.Errorf("PG 事务失败：%w", err)
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx,
		`UPDATE troupe_props
		 SET revision = revision + 1, schema_version = $1, data = $2, epoch = $3
		 WHERE actor_id = $4 AND revision = $5
		   AND $3 = (SELECT epoch FROM troupe_ownership WHERE actor_id = $4)
		   AND (SELECT lease_expires_at FROM troupe_ownership WHERE actor_id = $4) > now()`,
		schemaVersion, data, epoch, actorID.Canonical(), expectedRevision,
	)
	if err != nil {
		return troupe.PropRecord{}, fmt.Errorf("PG write 失败：%w", err)
	}
	if tag.RowsAffected() == 0 {
		// 拒绝分类（fencing 优先，同事务连接内读）。
		var holder string
		var curEpoch uint64
		var leaseLeft time.Duration
		err := tx.QueryRow(ctx,
			`SELECT holder, epoch, lease_expires_at - now() FROM troupe_ownership WHERE actor_id = $1`,
			actorID.Canonical(),
		).Scan(&holder, &curEpoch, &leaseLeft)
		if errors.Is(err, pgx.ErrNoRows) {
			return troupe.PropRecord{}, &troupe.ConflictError{Kind: troupe.ConflictRevision, Actor: actorID.Canonical()}
		}
		if err != nil {
			return troupe.PropRecord{}, fmt.Errorf("PG fencing 分类失败：%w", err)
		}
		if holder != s.cfg.instance || curEpoch != epoch || leaseLeft <= 0 {
			return troupe.PropRecord{}, &troupe.CallRejectedError{
				Reason:    troupe.RejectOwnership,
				Retryable: false,
				Actor:     actorID.Canonical(),
				Detail: fmt.Sprintf("epoch %d 已失效（当前 holder=%s epoch=%d 租期剩余=%v），拒绝写入",
					epoch, holder, curEpoch, leaseLeft),
			}
		}
		return troupe.PropRecord{}, &troupe.ConflictError{Kind: troupe.ConflictRevision, Actor: actorID.Canonical()}
	}
	for _, entry := range journal {
		_, err = tx.Exec(ctx,
			`INSERT INTO troupe_journal (actor_id, key, fingerprint, ok, value, error_class, error_message, expires_at)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			 ON CONFLICT (actor_id, key) DO UPDATE SET
			   fingerprint = EXCLUDED.fingerprint, ok = EXCLUDED.ok, value = EXCLUDED.value,
			   error_class = EXCLUDED.error_class, error_message = EXCLUDED.error_message,
			   expires_at = EXCLUDED.expires_at`,
			actorID.Canonical(), entry.Key, entry.Fingerprint, entry.OK, entry.Value,
			entry.ErrorClass, entry.ErrorMessage, entry.ExpiresAt,
		)
		if err != nil {
			return troupe.PropRecord{}, fmt.Errorf("PG journal 写失败：%w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return troupe.PropRecord{}, fmt.Errorf("PG 提交失败：%w", err)
	}
	return troupe.PropRecord{Revision: expectedRevision + 1, SchemaVersion: schemaVersion, Data: data, Epoch: epoch}, nil
}

func (s *Store) WriteJournaled(ctx context.Context, actorID troupe.ActorID, expectedRevision uint64, schemaVersion int, data string, epoch uint64, journal []troupe.IdempotencyRecord) (troupe.PropRecord, error) {
	return s.writeCore(ctx, actorID, expectedRevision, schemaVersion, data, epoch, journal)
}

func (s *Store) LoadJournal(ctx context.Context, actorID troupe.ActorID, nowUnix int64) ([]troupe.IdempotencyRecord, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT key, fingerprint, ok, value, error_class, error_message, expires_at
		 FROM troupe_journal WHERE actor_id = $1 AND expires_at > $2`,
		actorID.Canonical(), nowUnix,
	)
	if err != nil {
		return nil, fmt.Errorf("PG journal 读失败：%w", err)
	}
	defer rows.Close()
	var out []troupe.IdempotencyRecord
	for rows.Next() {
		var r troupe.IdempotencyRecord
		if err := rows.Scan(&r.Key, &r.Fingerprint, &r.OK, &r.Value, &r.ErrorClass, &r.ErrorMessage, &r.ExpiresAt); err != nil {
			return nil, fmt.Errorf("PG journal scan 失败：%w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// Acquire 取得所有权：单语句 upsert 带租期守卫——租期内其他实例拒绝（可稍后重试），
// 同实例重取/过期重取 epoch 单调 +1。无行返回即租期冲突。
func (o *OwnershipStore) Acquire(ctx context.Context, actorID troupe.ActorID) (uint64, error) {
	var epoch uint64
	err := o.pool.QueryRow(ctx,
		`INSERT INTO troupe_ownership (actor_id, holder, epoch, lease_expires_at)
		 VALUES ($1, $2, 1, now() + make_interval(secs => $3))
		 ON CONFLICT (actor_id) DO UPDATE SET
		   holder = EXCLUDED.holder,
		   epoch = troupe_ownership.epoch + 1,
		   lease_expires_at = now() + make_interval(secs => $3)
		 WHERE troupe_ownership.lease_expires_at < now() OR troupe_ownership.holder = EXCLUDED.holder
		 RETURNING epoch`,
		actorID.Canonical(), o.cfg.instance, o.cfg.lease.Seconds(),
	).Scan(&epoch)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, &troupe.CallRejectedError{
			Reason:    troupe.RejectOwnership,
			Retryable: false,
			Actor:     actorID.Canonical(),
			Detail: fmt.Sprintf("租期内另一实例持有所有权（本实例 %s，租期 %v）；租期过期后可重试",
				o.cfg.instance, o.cfg.lease),
		}
	}
	if err != nil {
		return 0, fmt.Errorf("PG acquire 失败：%w", err)
	}
	return epoch, nil
}

// Current 返回当前 epoch（无记录为 0）。
func (o *OwnershipStore) Current(ctx context.Context, actorID troupe.ActorID) uint64 {
	var epoch uint64
	err := o.pool.QueryRow(ctx,
		`SELECT epoch FROM troupe_ownership WHERE actor_id = $1`, actorID.Canonical(),
	).Scan(&epoch)
	if err != nil {
		return 0
	}
	return epoch
}

// Renew 同 epoch 同 holder 续租；epoch 已被取代或非持有者返回 false。
func (o *OwnershipStore) Renew(ctx context.Context, actorID troupe.ActorID, epoch uint64) bool {
	tag, err := o.pool.Exec(ctx,
		`UPDATE troupe_ownership SET lease_expires_at = now() + make_interval(secs => $3)
		 WHERE actor_id = $1 AND epoch = $2 AND holder = $4`,
		actorID.Canonical(), epoch, o.cfg.lease.Seconds(), o.cfg.instance,
	)
	if err != nil {
		return false
	}
	return tag.RowsAffected() == 1
}

func (o *OwnershipStore) assertFencing(ctx context.Context, actorID troupe.ActorID, epoch uint64) error {
	var holder string
	var curEpoch uint64
	var leaseLeft time.Duration
	err := o.pool.QueryRow(ctx,
		`SELECT holder, epoch, lease_expires_at - now() FROM troupe_ownership WHERE actor_id = $1`,
		actorID.Canonical(),
	).Scan(&holder, &curEpoch, &leaseLeft)
	if errors.Is(err, pgx.ErrNoRows) {
		// 尚无所有权记录：任何 epoch 均无效（必须先 Acquire）。
		return &troupe.CallRejectedError{
			Reason: troupe.RejectOwnership, Retryable: false, Actor: actorID.Canonical(),
			Detail: "尚无所有权记录（激活顺序要求先 acquire 再 create/write）",
		}
	}
	if err != nil {
		return fmt.Errorf("PG fencing 检查失败：%w", err)
	}
	if holder != o.cfg.instance || curEpoch != epoch || leaseLeft <= 0 {
		return &troupe.CallRejectedError{
			Reason: troupe.RejectOwnership, Retryable: false, Actor: actorID.Canonical(),
			Detail: fmt.Sprintf("epoch %d 已失效（当前 holder=%s epoch=%d 租期剩余=%v）", epoch, holder, curEpoch, leaseLeft),
		}
	}
	return nil
}

// ExpireLease 测试/运维辅助：立即令指定 Actor 的租期过期（模拟接管前条件）。
// 生产运维应通过租期自然过期，勿在业务路径调用。
func (o *OwnershipStore) ExpireLease(ctx context.Context, actorID troupe.ActorID) error {
	_, err := o.pool.Exec(ctx,
		`UPDATE troupe_ownership SET lease_expires_at = now() - interval '1 minute' WHERE actor_id = $1`,
		actorID.Canonical(),
	)
	if err != nil {
		return fmt.Errorf("PG 租期过期辅助失败：%w", err)
	}
	return nil
}
