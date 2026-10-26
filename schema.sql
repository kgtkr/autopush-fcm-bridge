CREATE TABLE "public"."autopush_sessions" (
  "uaid" VARCHAR(128) PRIMARY KEY,
  "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
  "last_connected_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE "public"."channels" (
  "channel_id" UUID PRIMARY KEY,
  "uaid" VARCHAR(128) NOT NULL REFERENCES "public"."autopush_sessions" ("uaid") ON DELETE CASCADE,
  "fcm_token" TEXT NOT NULL,
  "app_server_key" VARCHAR(255) NOT NULL,
  "push_endpoint" TEXT NOT NULL,
  "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
  "updated_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX "channels_uaid_idx" ON "public"."channels" ("uaid");
CREATE INDEX "channels_fcm_token_idx" ON "public"."channels" ("fcm_token");
