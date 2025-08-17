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
	"lowest" NUMERIC,
	"tag" VARCHAR(20)
);

CREATE INDEX idx_event_symbol_time ON event(symbol, endtime);

-- Bảng technical
CREATE TABLE "technical" (
	"id" SERIAL PRIMARY KEY,
	"event_id" INTEGER,
	"endtime" TIMESTAMPTZ,
	"symbol" VARCHAR(255),
	"sma20" NUMERIC,
	"ema12" NUMERIC,
	"rsi10" NUMERIC,
	"macd" NUMERIC,
	"bb" NUMERIC,
	"atr" NUMERIC,
	"va_high" NUMERIC,
	"va_low" NUMERIC,
	"poc" NUMERIC,
	CONSTRAINT fk_event
        FOREIGN KEY("event_id")
        REFERENCES "event"("event_id")
        ON DELETE CASCADE
);

-- Bảng news (giữ bản hợp lệ)
CREATE TABLE "news" (
	"id" SERIAL PRIMARY KEY,
	"domain" VARCHAR(100) NOT NULL,
	"title" TEXT NOT NULL,
	"url" VARCHAR(255) NOT NULL,
	"published" TIMESTAMPTZ NOT NULL,
	"author" VARCHAR(255) NOT NULL,
	"tag" VARCHAR(60) NOT NULL
);

-- Bảng sentiment
CREATE TABLE "sentiment" (
	"id" SERIAL PRIMARY KEY,
	"event_id" INTEGER,
	"endtime" TIMESTAMPTZ NOT NULL,
	"rsi_sen" VARCHAR(255) NOT NULL,
	"macd_sen" VARCHAR(255) NOT NULL,
	"ema_sen" VARCHAR(255) NOT NULL,
	"bb_sen" VARCHAR(255) NOT NULL,
	"sma_sen" VARCHAR(255) NOT NULL,
	"atr_sen" VARCHAR(255) NOT NULL
);

CREATE TABLE "event_max_cache" (
    "symbol" VARCHAR(10) PRIMARY KEY,
    "max_value" NUMERIC NOT NULL,
    "max_timestamp" TIMESTAMPTZ NOT NULL
);