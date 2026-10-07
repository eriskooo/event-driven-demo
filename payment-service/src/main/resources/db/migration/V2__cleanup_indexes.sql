-- Indexy pro RetentionCleanup: mazání dokončených řádků starších než retence bez full scanu.
CREATE INDEX inbox_finished_idx ON inbox (processed_at) WHERE status <> 'PENDING';
CREATE INDEX outbox_published_idx ON outbox (published_at) WHERE published_at IS NOT NULL;
