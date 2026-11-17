package troupepg

// PG 持久流（M5）：事件日志 per-topic 单调 seq、per-consumer 持久游标
// （ack GREATEST 只前进——回退显式拒绝）、不 ack 重放 at-least-once、
// retain_before 保留裁剪。跨运行持久（PG 表）。

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	troupe "github.com/ShiningRay/troupe-go"
)

// StreamStore PG 流存储。
type StreamStore struct {
	pool *pgxpool.Pool
}

// OpenStreams 建立 PG 流存储（schema 迁移）。
func (s *Store) OpenStreams() (*StreamStore, error) {
	for _, stmt := range []string{
		`CREATE TABLE IF NOT EXISTS troupe_streams (
			topic TEXT NOT NULL,
			seq BIGINT NOT NULL,
			payload TEXT NOT NULL DEFAULT 'null',
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			PRIMARY KEY (topic, seq)
		)`,
		`CREATE TABLE IF NOT EXISTS troupe_stream_consumers (
			topic TEXT NOT NULL,
			consumer TEXT NOT NULL,
			cursor BIGINT NOT NULL DEFAULT 0,
			PRIMARY KEY (topic, consumer)
		)`,
	} {
		if _, err := s.pool.Exec(context.Background(), stmt); err != nil {
			return nil, fmt.Errorf("PG streams 迁移失败：%w", err)
		}
	}
	return &StreamStore{pool: s.pool}, nil
}

func (s *StreamStore) Append(ctx context.Context, topic string, payload json.RawMessage) (uint64, error) {
	var seq uint64
	err := s.pool.QueryRow(ctx,
		`INSERT INTO troupe_streams (topic, seq, payload)
		 VALUES ($1,
		         COALESCE((SELECT MAX(seq) + 1 FROM troupe_streams WHERE topic = $1), 1),
		         $2)
		 RETURNING seq`,
		topic, string(payload),
	).Scan(&seq)
	if err != nil {
		return 0, fmt.Errorf("PG stream append 失败：%w", err)
	}
	return seq, nil
}

func (s *StreamStore) Poll(ctx context.Context, topic, consumer string, limit int) ([]troupe.StreamRecord, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT seq, payload, created_at FROM troupe_streams
		 WHERE topic = $1 AND seq > COALESCE((SELECT cursor FROM troupe_stream_consumers
		                                     WHERE topic = $1 AND consumer = $2), 0)
		 ORDER BY seq LIMIT $3`,
		topic, consumer, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("PG stream poll 失败：%w", err)
	}
	defer rows.Close()
	var out []troupe.StreamRecord
	for rows.Next() {
		var rec troupe.StreamRecord
		var payload string
		if err := rows.Scan(&rec.Seq, &payload, &rec.CreatedAt); err != nil {
			return nil, fmt.Errorf("PG stream scan 失败：%w", err)
		}
		rec.Topic = topic
		rec.Payload = json.RawMessage(payload)
		out = append(out, rec)
	}
	return out, rows.Err()
}

func (s *StreamStore) Ack(ctx context.Context, topic, consumer string, seq uint64) error {
	tag, err := s.pool.Exec(ctx,
		`INSERT INTO troupe_stream_consumers (topic, consumer, cursor) VALUES ($1, $2, $3)
		 ON CONFLICT (topic, consumer) DO UPDATE SET cursor = GREATEST(troupe_stream_consumers.cursor, EXCLUDED.cursor)
		 WHERE troupe_stream_consumers.cursor < EXCLUDED.cursor`,
		topic, consumer, seq,
	)
	if err != nil {
		return fmt.Errorf("PG stream ack 失败：%w", err)
	}
	// 回退拒绝：GREATEST 未生效（现有游标 ≥ seq）→ 显式错误
	if tag.RowsAffected() == 0 {
		var cur uint64
		_ = s.pool.QueryRow(ctx,
			`SELECT cursor FROM troupe_stream_consumers WHERE topic = $1 AND consumer = $2`,
			topic, consumer).Scan(&cur)
		return fmt.Errorf("%w：%s/%s 游标 %d ≥ ack %d", troupe.ErrCursorBackward, topic, consumer, cur, seq)
	}
	return nil
}

func (s *StreamStore) Cursor(ctx context.Context, topic, consumer string) uint64 {
	var cur uint64
	err := s.pool.QueryRow(ctx,
		`SELECT cursor FROM troupe_stream_consumers WHERE topic = $1 AND consumer = $2`,
		topic, consumer).Scan(&cur)
	if err != nil {
		return 0
	}
	return cur
}

func (s *StreamStore) RetainBefore(ctx context.Context, topic string, before uint64) (int, error) {
	tag, err := s.pool.Exec(ctx,
		`DELETE FROM troupe_streams WHERE topic = $1 AND seq < $2`,
		topic, before,
	)
	if err != nil {
		return 0, fmt.Errorf("PG stream retain 失败：%w", err)
	}
	return int(tag.RowsAffected()), nil
}

func (s *StreamStore) Topics(ctx context.Context) []string {
	rows, err := s.pool.Query(ctx, `SELECT DISTINCT topic FROM troupe_streams ORDER BY topic`)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			continue
		}
		out = append(out, t)
	}
	return out
}

var _ troupe.StreamStore = (*StreamStore)(nil)
