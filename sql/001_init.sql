-- Initial schema. Source of truth: docs/norway_stock_data_system_design.md §6.
-- Never edit after it has been applied; add sql/002_*.sql instead.

CREATE TABLE instruments (
    instrument_id BIGSERIAL PRIMARY KEY,
    isin VARCHAR(12),
    exchange_symbol TEXT NOT NULL,
    name TEXT NOT NULL,
    market TEXT NOT NULL,
    currency CHAR(3) NOT NULL,              -- 报价币种，取自 Euronext 清单，不设默认值
    instrument_type TEXT DEFAULT 'common_stock',
    status TEXT NOT NULL DEFAULT 'active',  -- active / suspended / inactive_candidate / inactive / mapping_review
    first_seen_at TIMESTAMPTZ DEFAULT now(),
    last_seen_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (market, exchange_symbol)
);

CREATE TABLE symbol_aliases (
    alias_id BIGSERIAL PRIMARY KEY,
    instrument_id BIGINT NOT NULL REFERENCES instruments(instrument_id),
    provider TEXT NOT NULL,
    provider_symbol TEXT NOT NULL,
    valid_from DATE NOT NULL DEFAULT CURRENT_DATE,
    valid_to DATE,
    is_verified BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE (provider, provider_symbol, valid_from)
);

-- 同一证券在同一 provider 下同时只能有一条生效映射
CREATE UNIQUE INDEX uq_alias_open_per_instrument
    ON symbol_aliases (instrument_id, provider) WHERE valid_to IS NULL;
-- 同一 provider 代码同时只能指向一只证券
CREATE UNIQUE INDEX uq_alias_open_per_symbol
    ON symbol_aliases (provider, provider_symbol) WHERE valid_to IS NULL;

CREATE TABLE daily_prices (
    instrument_id BIGINT NOT NULL REFERENCES instruments(instrument_id),
    trade_date DATE NOT NULL,
    open DOUBLE PRECISION, high DOUBLE PRECISION,
    low DOUBLE PRECISION, close DOUBLE PRECISION,   -- Yahoo Close：已拆股调整，未分红调整
    adj_close DOUBLE PRECISION,                     -- Yahoo Adj Close：拆股 + 分红调整
    volume BIGINT,
    source_id SMALLINT NOT NULL DEFAULT 1,          -- 1 = yahoo；用编码代替 TEXT 以缩小行宽
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (instrument_id, trade_date, source_id),
    CHECK (volume IS NULL OR volume >= 0)
);
-- 主键已覆盖按股票查询；按日期横截面查询的索引待实际需要时再加，以节省空间

CREATE TABLE corporate_actions (
    instrument_id BIGINT NOT NULL REFERENCES instruments(instrument_id),
    action_date DATE NOT NULL,
    action_type TEXT NOT NULL,              -- 'split' / 'dividend'
    value DOUBLE PRECISION NOT NULL,        -- 拆股比例或每股分红金额（报价币种）
    source_id SMALLINT NOT NULL DEFAULT 1,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (instrument_id, action_date, action_type, source_id)
);

CREATE TABLE instrument_sync_state (
    instrument_id BIGINT PRIMARY KEY REFERENCES instruments(instrument_id),
    backfill_status TEXT NOT NULL DEFAULT 'pending',  -- pending / in_progress / done / no_data
    backfilled_from DATE,
    last_price_date DATE,
    last_success_at TIMESTAMPTZ,
    consecutive_failures INT NOT NULL DEFAULT 0,
    consecutive_missing INT NOT NULL DEFAULT 0,       -- 清单同步中连续缺席次数
    needs_full_refetch BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE ingestion_runs (
    run_id UUID PRIMARY KEY,                -- 使用 Prefect flow_run_id，便于对照日志
    flow_name TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL,
    total_symbols INT DEFAULT 0,
    successful_symbols INT DEFAULT 0,
    rows_upserted INT DEFAULT 0,
    error_count INT DEFAULT 0,
    details JSONB DEFAULT '{}'::jsonb
);

CREATE TABLE ingestion_errors (
    error_id BIGSERIAL PRIMARY KEY,
    run_id UUID REFERENCES ingestion_runs(run_id),
    instrument_id BIGINT REFERENCES instruments(instrument_id),
    error_type TEXT NOT NULL,
    message TEXT,
    occurred_at TIMESTAMPTZ DEFAULT now()
);
