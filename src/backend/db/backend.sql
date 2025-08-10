CREATE TABLE "event" (
	"event_id" SERIAL PRIMARY KEY,
	"symbol" VARCHAR(255) NOT NULL,
	"name" VARCHAR(100),
	"interval" VARCHAR(4),
	"starttime" TIMESTAMPTZ NOT NULL,
	"endtime" TIMESTAMPTZ NOT NULL,
	"volume" NUMERIC NOT NULL,
	"quotevolume" NUMERIC NOT NULL,
	"open" NUMERIC NOT NULL,
	"close" NUMERIC NOT NULL,
	"highest" NUMERIC,
	"lowest" NUMERIC
);

CREATE INDEX idx_event_symbol_time ON event(symbol, endtime);

-- Bảng technical
CREATE TABLE "technical" (
	"id" SERIAL PRIMARY KEY,
	"event_id" INTEGER,
	"endtime" TIMESTAMPTZ NOT NULL,
	"symbol" VARCHAR(255) NOT NULL,
	"sma20" NUMERIC NOT NULL UNIQUE,
	"ema12" NUMERIC NOT NULL,
	"rsi10" NUMERIC NOT NULL,
	"macd" NUMERIC NOT NULL,
	"bb" NUMERIC NOT NULL,
	"atr" NUMERIC NOT NULL,
	"va_high" NUMERIC NOT NULL,
	"va_low" NUMERIC NOT NULL,
	"POC" NUMERIC NOT NULL,
	CONSTRAINT fk_event
        FOREIGN KEY("event_id")
        REFERENCES "event"("event_id")
        ON DELETE CASCADE
);

-- Bảng news (giữ bản hợp lệ)
CREATE TABLE "news" (
	"id" SERIAL PRIMARY KEY,
	"title" VARCHAR(255) NOT NULL,
	"published" TIMESTAMPTZ NOT NULL,
	"text" VARCHAR(255) NOT NULL,
	"url" VARCHAR(255) NOT NULL,
	"tag" VARCHAR(60) NOT NULL
);

-- Bảng sentiment
CREATE TABLE "sentiment" (
	"id" SERIAL PRIMARY KEY,
	"event_id" INTEGER,
	"endtime" TIMESTAMPTZ NOT NULL,
	"RSI_sen" VARCHAR(255) NOT NULL,
	"MACD_sen" VARCHAR(255) NOT NULL,
	"EMA_sen" VARCHAR(255) NOT NULL,
	"bb_sen" VARCHAR(255) NOT NULL,
	"SMA_sen" VARCHAR(255) NOT NULL,
	"ATR_sen" VARCHAR(255) NOT NULL
);

CREATE TABLE "event_max_cache" (
    "symbol" VARCHAR(10) PRIMARY KEY,
    "max_value" NUMERIC NOT NULL,
    "check_count_max" INT NOT NULL
);