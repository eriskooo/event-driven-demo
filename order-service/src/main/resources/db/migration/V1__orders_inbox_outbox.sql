-- Objednávky (doménový stav).
CREATE TABLE orders (
    id             VARCHAR(36)    PRIMARY KEY,
    customer_id    VARCHAR(100)   NOT NULL,
    amount         NUMERIC(14, 2) NOT NULL,
    currency       CHAR(3)        NOT NULL,
    status         VARCHAR(20)    NOT NULL,
    payment_id     VARCHAR(64),
    failure_reason VARCHAR(500),
    created_at     TIMESTAMPTZ    NOT NULL,
    updated_at     TIMESTAMPTZ    NOT NULL
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
