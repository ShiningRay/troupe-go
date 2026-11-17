package troupepg

// PG 持久 Reminder 与 Outbox（M3-D）：claim 带归属过滤（troupe_ownership 持有者，
// 无记录任何节点可认领）与租期；complete/release 以 status_version+holder+租期
// fencing（DB 时钟判定）；outbox 与 Props 同事务提交、派发 at-least-once。

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	troupe "github.com/ShiningRay/troupego"
)

// ReminderStore PG 提醒存储。
type ReminderStore struct {
	pool *pgxpool.Pool
}

// OpenReminders 建立 PG 提醒存储（复用 Store 的 schema 前缀）。
func (s *Store) OpenReminders() *ReminderStore {
	s.migrateReminders()
	return &ReminderStore{pool: s.pool}
}

func (s *Store) migrateReminders() error {
	_, err := s.pool.Exec(context.Background(),
		`CREATE TABLE IF NOT EXISTS troupe_reminders (
			actor_id TEXT NOT NULL,
			name TEXT NOT NULL,
			method TEXT NOT NULL,
			payload TEXT NOT NULL DEFAULT 'null',
			next_due TIMESTAMPTZ NOT NULL,
			claim_holder TEXT NOT NULL DEFAULT '',
			claim_expires_at TIMESTAMPTZ,
			status_version BIGINT NOT NULL DEFAULT 0,
			PRIMARY KEY (actor_id, name)
		)`)
	if err != nil {
		return fmt.Errorf("PG reminders 迁移失败：%w", err)
	}
	return nil
}

func (r *ReminderStore) Schedule(ctx context.Context, actorID troupe.ActorID, name, method string, payload json.RawMessage, dueAt time.Time) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO troupe_reminders (actor_id, name, method, payload, next_due)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (actor_id, name) DO UPDATE SET
		   method = EXCLUDED.method, payload = EXCLUDED.payload, next_due = EXCLUDED.next_due`,
		actorID.Canonical(), name, method, string(payload), dueAt,
	)
	if err != nil {
		return fmt.Errorf("PG reminder schedule 失败：%w", err)
	}
	return nil
}

func (r *ReminderStore) Cancel(ctx context.Context, actorID troupe.ActorID, name string) bool {
	tag, err := r.pool.Exec(ctx, `DELETE FROM troupe_reminders WHERE actor_id = $1 AND name = $2`,
		actorID.Canonical(), name)
	return err == nil && tag.RowsAffected() == 1
}

// ClaimDue 原子认领：单语句 UPDATE ... WHERE next_due <= now()
// AND (claim_expires_at IS NULL OR claim_expires_at < now() OR claim_holder = $holder)
// AND（归属过滤：troupe_ownership.holder = 本实例 OR 无所有权记录）。
// 归属过滤保证跨实例只认领本节点拥有的 actor；无所有权记录的 actor 任何节点可认领。
func (r *ReminderStore) ClaimDue(ctx context.Context, now time.Time, holder string, lease time.Duration) ([]troupe.ReminderRecord, error) {
	rows, err := r.pool.Query(ctx,
		`WITH claimed AS (
			UPDATE troupe_reminders
			SET claim_holder = $1::text, claim_expires_at = now() + make_interval(secs => $2::float8), status_version = status_version + 1
			WHERE next_due <= now()
			  AND (claim_expires_at IS NULL OR claim_expires_at < now() OR claim_holder = $1::text)
			  AND (EXISTS (SELECT 1 FROM troupe_ownership o
			               WHERE o.actor_id = troupe_reminders.actor_id AND o.holder = $1::text)
			       OR NOT EXISTS (SELECT 1 FROM troupe_ownership o WHERE o.actor_id = troupe_reminders.actor_id))
			RETURNING actor_id, name, method, payload, next_due, claim_holder, claim_expires_at, status_version
		) SELECT actor_id, name, method, payload, next_due, claim_holder, claim_expires_at, status_version
		  FROM claimed ORDER BY actor_id, name`,
		// now 参数弃用：认领判定以 DB 时钟 now() 为权威（跨节点时钟不受本机偏移影响）
		holder, lease.Seconds(),
	)
	if err != nil {
		return nil, fmt.Errorf("PG reminder claim 失败：%w", err)
	}
	defer rows.Close()
	var out []troupe.ReminderRecord
	for rows.Next() {
		var rec troupe.ReminderRecord
		var payload string
		if err := rows.Scan(&rec.ActorID, &rec.Name, &rec.Method, &payload, &rec.NextDue,
			&rec.ClaimHolder, &rec.ClaimExpiresAt, &rec.StatusVersion); err != nil {
			return nil, fmt.Errorf("PG reminder scan 失败：%w", err)
		}
		rec.Payload = json.RawMessage(payload)
		out = append(out, rec)
	}
	return out, rows.Err()
}

func (r *ReminderStore) Complete(ctx context.Context, actorID troupe.ActorID, name string, statusVersion uint64, holder string) (bool, error) {
	tag, err := r.pool.Exec(ctx,
		`DELETE FROM troupe_reminders
		 WHERE actor_id = $1 AND name = $2 AND status_version = $3 AND claim_holder = $4
		   AND claim_expires_at > now()`,
		actorID.Canonical(), name, statusVersion, holder,
	)
	if err != nil {
		return false, fmt.Errorf("PG reminder complete 失败：%w", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (r *ReminderStore) Release(ctx context.Context, actorID troupe.ActorID, name string, statusVersion uint64, holder string, retryIn time.Duration) (bool, error) {
	tag, err := r.pool.Exec(ctx,
		`UPDATE troupe_reminders
		 SET next_due = now() + make_interval(secs => $5), claim_holder = '', claim_expires_at = NULL
		 WHERE actor_id = $1 AND name = $2 AND status_version = $3 AND claim_holder = $4
		   AND claim_expires_at > now()`,
		actorID.Canonical(), name, statusVersion, holder, retryIn.Seconds(),
	)
	if err != nil {
		return false, fmt.Errorf("PG reminder release 失败：%w", err)
	}
	return tag.RowsAffected() == 1, nil
}

var _ troupe.ReminderStore = (*ReminderStore)(nil)

// Outbox PG outbox：WriteWithOutbox 在 Store 的 writeCore 同事务内提交；
// PollOutbox/MarkSent 未投递过滤。
type pgOutbox struct {
	*Store // 嵌入补齐 JournalingStore 方法集（OutboxStore 扩展）
}

// Outbox 返回 PG OutboxStore（复用同一 Store/连接池；schema 迁移）。
func (s *Store) Outbox() troupe.OutboxStore {
	for _, stmt := range []string{
		`CREATE TABLE IF NOT EXISTS troupe_outbox (
			actor_id TEXT NOT NULL,
			event_id TEXT NOT NULL,
			topic TEXT NOT NULL,
			payload TEXT NOT NULL DEFAULT 'null',
			created_at TIMESTAMPTZ NOT NULL,
			PRIMARY KEY (actor_id, event_id)
		)`,
		`CREATE TABLE IF NOT EXISTS troupe_outbox_sent (
			actor_id TEXT NOT NULL,
			event_id TEXT NOT NULL,
			sent_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			PRIMARY KEY (actor_id, event_id)
		)`,
	} {
		if _, err := s.pool.Exec(context.Background(), stmt); err != nil {
			panic(fmt.Sprintf("PG outbox 迁移失败：%v", err))
		}
	}
	return &pgOutbox{Store: s}
}

// WriteWithOutbox 在 writeCore 的同一事务内提交 outbox（CAS 失败整体回滚）。
func (o *pgOutbox) WriteWithOutbox(ctx context.Context, actorID troupe.ActorID, expectedRevision uint64, schemaVersion int, data string, epoch uint64, journal []troupe.IdempotencyRecord, outbox []troupe.OutboxRecord) (troupe.PropRecord, error) {
	// 事务内联版 writeCore + outbox
	tx, err := o.pool.Begin(ctx)
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
		return troupe.PropRecord{}, &troupe.ConflictError{Kind: troupe.ConflictRevision, Actor: actorID.Canonical()}
	}
	for _, entry := range journal {
		if _, err = tx.Exec(ctx,
			`INSERT INTO troupe_journal (actor_id, key, fingerprint, ok, value, error_class, error_message, expires_at)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			 ON CONFLICT (actor_id, key) DO UPDATE SET fingerprint = EXCLUDED.fingerprint, ok = EXCLUDED.ok,
			   value = EXCLUDED.value, error_class = EXCLUDED.error_class,
			   error_message = EXCLUDED.error_message, expires_at = EXCLUDED.expires_at`,
			actorID.Canonical(), entry.Key, entry.Fingerprint, entry.OK, entry.Value,
			entry.ErrorClass, entry.ErrorMessage, entry.ExpiresAt,
		); err != nil {
			return troupe.PropRecord{}, fmt.Errorf("PG journal 写失败：%w", err)
		}
	}
	for _, ev := range outbox {
		if _, err = tx.Exec(ctx,
			`INSERT INTO troupe_outbox (actor_id, event_id, topic, payload, created_at)
			 VALUES ($1, $2, $3, $4, $5)
			 ON CONFLICT (actor_id, event_id) DO NOTHING`,
			actorID.Canonical(), ev.EventID, ev.Topic, string(ev.Payload), ev.CreatedAt,
		); err != nil {
			return troupe.PropRecord{}, fmt.Errorf("PG outbox 写失败：%w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return troupe.PropRecord{}, fmt.Errorf("PG 提交失败：%w", err)
	}
	return troupe.PropRecord{Revision: expectedRevision + 1, SchemaVersion: schemaVersion, Data: data, Epoch: epoch}, nil
}

func (o *pgOutbox) PollOutbox(ctx context.Context, actorID troupe.ActorID, limit int) ([]troupe.OutboxRecord, error) {
	rows, err := o.pool.Query(ctx,
		`SELECT event_id, topic, payload, created_at FROM troupe_outbox
		 WHERE actor_id = $1
		   AND NOT EXISTS (SELECT 1 FROM troupe_outbox_sent s
		                   WHERE s.actor_id = troupe_outbox.actor_id AND s.event_id = troupe_outbox.event_id)
		 ORDER BY created_at LIMIT $2`,
		actorID.Canonical(), limit,
	)
	if err != nil {
		return nil, fmt.Errorf("PG outbox poll 失败：%w", err)
	}
	defer rows.Close()
	var out []troupe.OutboxRecord
	for rows.Next() {
		var rec troupe.OutboxRecord
		var payload string
		if err := rows.Scan(&rec.EventID, &rec.Topic, &payload, &rec.CreatedAt); err != nil {
			return nil, fmt.Errorf("PG outbox scan 失败：%w", err)
		}
		rec.ActorID = actorID.Canonical()
		rec.Payload = json.RawMessage(payload)
		out = append(out, rec)
	}
	return out, rows.Err()
}

func (o *pgOutbox) MarkSent(ctx context.Context, actorID troupe.ActorID, eventID string) bool {
	tag, err := o.pool.Exec(ctx,
		`INSERT INTO troupe_outbox_sent (actor_id, event_id) VALUES ($1, $2)
		 ON CONFLICT DO NOTHING`,
		actorID.Canonical(), eventID,
	)
	if err != nil {
		return false
	}
	return tag.RowsAffected() == 1
}

var _ troupe.OutboxStore = (*pgOutbox)(nil)
