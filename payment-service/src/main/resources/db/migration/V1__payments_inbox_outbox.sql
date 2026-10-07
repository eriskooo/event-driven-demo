-- Platby (doménový stav); jedna platba na objednávku.
CREATE TABLE payments (
    id             VARCHAR(36)    PRIMARY KEY,
    order_id       VARCHAR(36)    NOT NULL UNIQUE,
    status         VARCHAR(20)    NOT NULL,
    amount         NUMERIC(14, 2) NOT NULL,
    currency       CHAR(3)        NOT NULL,
    failure_reason VARCHAR(500),
    created_at     TIMESTAMPTZ    NOT NULL,
    CONSTRAINT payments_status_chk CHECK (status IN ('COMPLETED', 'FAILED'))
);

-- Transactional inbox: listener sem jen uloží přijatou zprávu (PK event_id = deduplikace) a potvrdí offset,
-- zpracování s retry/backoffem obstará InboxProcessor.
CREATE TABLE inbox (
    event_id        UUID          PRIMARY KEY,
    topic           VARCHAR(200)  NOT NULL,
    message_key     VARCHAR(200),
    payload         JSONB         NOT NULL,
    correlation_id  VARCHAR(100),
    status          VARCHAR(20)   NOT NULL DEFAULT 'PENDING',
    attempts        INT           NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ   NOT NULL DEFAULT now(),
    last_error      VARCHAR(2000),
    received_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),
    processed_at    TIMESTAMPTZ,
    CONSTRAINT inbox_status_chk CHECK (status IN ('PENDING', 'PROCESSED', 'FAILED'))
);

-- Processor hledá jen čekající zprávy, jejichž čas dalšího pokusu nastal.
CREATE INDEX inbox_due_idx ON inbox (next_attempt_at) WHERE status = 'PENDING';

-- Transactional outbox: zapisuje se ve stejné transakci jako doménová změna, do Kafky ho posílá OutboxRelay.
CREATE TABLE outbox (
    id             BIGSERIAL    PRIMARY KEY,
    event_id       UUID         NOT NULL UNIQUE,
    topic          VARCHAR(200) NOT NULL,
    message_key    VARCHAR(200) NOT NULL,
    payload        JSONB        NOT NULL,
    headers        JSONB        NOT NULL DEFAULT '{}'::jsonb,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    published_at   TIMESTAMPTZ
);

-- Relay čte jen nepublikované řádky v pořadí vložení.
CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;
