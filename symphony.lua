--[[
  SYMPHONY -- the fleet's biggest-sounding node, tuned for full, layered playback.
  LuaJIT port of symphony.py onto the shared swarmlua foundation (lib/swarmlua/).

  Command namespace: /symphony_main_*  (matches the Python bot's namespace so
  existing docs/links/muscle-memory keep working).

  This port keeps full command-surface parity with symphony.py's slash commands
  (see README.md's command list) but intentionally does not reimplement every
  layer of the Python file's defensive infrastructure 1:1 -- see the final
  report for the specific list of simplifications (yt-dlp audio caching/BPM/
  loudness analysis, the Aria direct-order swarm bridge, Redis cross-process
  locks, and the continuous background playlist-resync loop are the big ones;
  none of them have a LuaJIT-stack equivalent library available, and the
  shared gateway/lavalink reconnect logic already covers the reliability need
  they existed for).
]]

package.path = "./lib/?.lua;./lib/?/init.lua;" .. package.path

local Bot = require("swarmlua.bot")
local cjson = require("cjson")
local https = require("ssl.https")
local ltn12 = require("ltn12")
local bit = require("bit")
local copas = require("copas")

math.randomseed(os.time())

-- ============================================================================
-- CONFIG
-- ============================================================================

local BOT_ENV_PREFIX = "SYMPHONY"
local BOT_NAME = "symphony"
local CMD_PREFIX = "symphony_main_"

local function env(name, default)
  local v = os.getenv(name)
  if v == nil or v == "" then return default end
  return v
end

-- File-based logging was entirely missing from this bot (stdout-only, so
-- anything not caught live via `docker logs` was gone on the next log
-- rotation/restart) -- unlike alucard.py's RotatingFileHandler, which every
-- Python-era bot had. Rather than hand-migrate every existing print() call
-- site (large, error-prone for one pass), print itself is wrapped here so
-- every call -- old and new -- is transparently timestamped, tagged, and
-- persisted to a rotating file (same 10MB x 5 backups policy as the other
-- bots' logline()/LOG_FILE and Lavalink's logback config), with zero change
-- to any call site's behavior or arguments.
local LOG_DIR = env("SYMPHONY_LOG_DIR", env("MUSIC_BOT_LOG_DIR", "/app/logs"))
pcall(function() os.execute("mkdir -p '" .. LOG_DIR .. "'") end)
local LOG_FILE = LOG_DIR .. "/symphony.log"
local LOG_MAX_BYTES = tonumber(env("MUSIC_BOT_LOG_MAX_BYTES", "10485760")) or 10485760
local LOG_BACKUP_COUNT = tonumber(env("MUSIC_BOT_LOG_BACKUP_COUNT", "5")) or 5

local function rotate_log_if_needed()
  local f = io.open(LOG_FILE, "r")
  if not f then return end
  local size = f:seek("end")
  f:close()
  if not size or size < LOG_MAX_BYTES then return end
  for i = LOG_BACKUP_COUNT - 1, 1, -1 do
    os.rename(LOG_FILE .. "." .. i, LOG_FILE .. "." .. (i + 1))
  end
  os.rename(LOG_FILE, LOG_FILE .. ".1")
end

local raw_print = print
local function logline(level, fmt, ...)
  local ok, msg = pcall(string.format, fmt, ...)
  if not ok then msg = fmt end
  local line = string.format("%s %-5s symphony %s", os.date("%Y-%m-%d %H:%M:%S"), level, msg)
  raw_print(line)
  rotate_log_if_needed()
  local f = io.open(LOG_FILE, "a")
  if f then f:write(line .. "\n"); f:close() end
end
local function log_info(fmt, ...) logline("INFO", fmt, ...) end
local function log_warn(fmt, ...) logline("WARN", fmt, ...) end
local function log_error(fmt, ...) logline("ERROR", fmt, ...) end

-- Existing print("[bot] message") calls throughout this file already embed
-- their own tag/timestamp-free format; route them through the same
-- rotating file (tagged INFO, since plain print() carries no level) instead
-- of rewriting every call site.
print = function(...)
  local n = select("#", ...)
  local parts = {{}}
  for i = 1, n do parts[i] = tostring((select(i, ...))) end
  logline("INFO", "%s", table.concat(parts, "\t"))
end

local TOKEN = env(BOT_ENV_PREFIX .. "_DISCORD_TOKEN", "")
if TOKEN == "" then
  io.stderr:write("[symphony] " .. BOT_ENV_PREFIX .. "_DISCORD_TOKEN is not set; refusing to start.\n")
  os.exit(1)
end

local DB_CONFIG = {
  host = env(BOT_ENV_PREFIX .. "_DB_HOST", env("DB_HOST", "127.0.0.1")),
  port = tonumber(env(BOT_ENV_PREFIX .. "_DB_PORT", env("DB_PORT", "5432"))),
  user = env(BOT_ENV_PREFIX .. "_DB_USER", env("DB_USER", "botuser")),
  password = env(BOT_ENV_PREFIX .. "_DB_PASSWORD", env("DB_PASSWORD", "bot_logins")),
  database = env(BOT_ENV_PREFIX .. "_DB_NAME", "discord_music_symphony"),
}

local LAVALINK_URI = env(BOT_ENV_PREFIX .. "_LAVALINK_URI", env("LAVALINK_URI", env("LAVALINK_URL", "http://127.0.0.1:2333")))
local LAVALINK_PASSWORD = env(BOT_ENV_PREFIX .. "_LAVALINK_PASSWORD", env("LAVALINK_PASSWORD", ""))
if LAVALINK_PASSWORD == "" then
  io.stderr:write("[symphony] Set " .. BOT_ENV_PREFIX .. "_LAVALINK_PASSWORD or LAVALINK_PASSWORD before starting symphony.\n")
  os.exit(1)
end

-- Parse host/port out of a LAVALINK_URI like "http://127.0.0.1:2333"
local ll_host, ll_port = LAVALINK_URI:match("^https?://([^:/]+):?(%d*)")
ll_host = ll_host or "127.0.0.1"
ll_port = tonumber(ll_port) or 2333

local WEBHOOK_URL = env(BOT_ENV_PREFIX .. "_WEBHOOK_URL", "")
local ERROR_WEBHOOK_URL = env(BOT_ENV_PREFIX .. "_ERROR_WEBHOOK_URL", env("SWARM_ERROR_WEBHOOK_URL", ""))

-- Narrowed intents: GUILDS (1) + GUILD_VOICE_STATES (128). Symphony never reads
-- message content and doesn't need the privileged GUILD_MEMBERS intent --
-- member/user info is resolved on demand over REST (see resolve_display_name).
local INTENTS = 1 + 128

local bot = Bot.new({
  token = TOKEN,
  intents = INTENTS,
  db = DB_CONFIG,
  lavalink = {
    host = ll_host,
    port = ll_port,
    password = LAVALINK_PASSWORD,
  },
})

local START_TIME = os.time()

-- ============================================================================
-- CONSTANTS
-- ============================================================================

local COLOR = {
  red = 0xE74C3C, green = 0x2ECC71, blue = 0x3498DB, blurple = 0x5865F2,
  gold = 0xF1C40F, orange = 0xE67E22, dark_purple = 0x71368A, panel = 0x2B2D31,
}

local OPT = { STRING = 3, INTEGER = 4, BOOLEAN = 5, USER = 6, CHANNEL = 7, ROLE = 8, NUMBER = 10 }

local FILTER_CHOICES = {
  { name = "None (Standard high quality audio)", value = "none" },
  { name = "Bassboost", value = "bassboost" },
  { name = "Nightcore", value = "nightcore" },
  { name = "Vaporwave", value = "vaporwave" },
  { name = "8D Rotation", value = "8d" },
  { name = "Karaoke", value = "karaoke" },
  { name = "Tremolo", value = "tremolo" },
  { name = "Vibrato", value = "vibrato" },
  { name = "Low Pass", value = "lowpass" },
  { name = "Lo-fi", value = "lofi" },
  { name = "Electronic", value = "electronic" },
  { name = "Party", value = "party" },
  { name = "Radio", value = "radio" },
  { name = "Cinema", value = "cinema" },
  { name = "Soft", value = "soft" },
  { name = "Pop", value = "pop" },
  { name = "Rock", value = "rock" },
  { name = "Classical", value = "classical" },
  { name = "Ear Rape", value = "earrape" },
  { name = "Double Time (2x)", value = "doubletime" },
  { name = "Slow Mo (0.5x)", value = "slowmo" },
  { name = "Pitch Up", value = "pitch_up" },
  { name = "Pitch Down", value = "pitch_down" },
  { name = "Deep", value = "deep" },
  { name = "Anime", value = "anime" },
}
local FILTER_VALUES = {}
local FILTER_LABELS = {}
for _, c in ipairs(FILTER_CHOICES) do FILTER_VALUES[c.value] = true; FILTER_LABELS[c.value] = c.name end

local LOUDNORM_EQ = {
  {0,-0.05},{1,-0.03},{2,0.0},{3,0.0},{4,0.0},{5,0.0},{6,0.0},{7,-0.01},
  {8,-0.02},{9,-0.02},{10,-0.03},{11,-0.02},{12,0.01},{13,0.01},{14,0.0},
}

-- ============================================================================
-- SMALL UTILITIES
-- ============================================================================

local function iuser(interaction)
  if interaction.member and interaction.member.user then return interaction.member.user end
  return interaction.user
end

local function iuid(interaction)
  local u = iuser(interaction)
  return u and u.id
end

local function idisplay(interaction)
  if interaction.member and interaction.member.nick and interaction.member.nick ~= cjson.null then
    return interaction.member.nick
  end
  local u = iuser(interaction)
  if not u then return "Unknown" end
  return (u.global_name and u.global_name ~= cjson.null and u.global_name) or u.username or "Unknown"
end

local guild_owner_cache = {} -- guild_id -> owner user_id

local function get_guild_owner_id(guild_id)
  if not guild_id then return nil end
  local cached = guild_owner_cache[guild_id]
  if cached then return cached end
  local g = bot.rest:get("/guilds/" .. tostring(guild_id))
  local owner_id = g and g.owner_id
  if owner_id then guild_owner_cache[guild_id] = owner_id end
  return owner_id
end

local function has_admin(interaction)
  local perms = interaction.member and interaction.member.permissions
  local v = perms and tonumber(perms)
  if v and (math.floor(v / 0x8) % 2) == 1 then return true end
  -- The guild owner always effectively has admin, but Discord's resolved
  -- member.permissions bitfield on the interaction has been observed to not
  -- reliably reflect that -- fall back to an explicit ownership check
  -- rather than ever locking the actual owner out of admin-gated commands.
  local uid = interaction.member and interaction.member.user and interaction.member.user.id
  local owner_id = uid and get_guild_owner_id(interaction.guild_id)
  return owner_id ~= nil and tostring(owner_id) == tostring(uid)
end

local function new_track_uid()
  local chars = "0123456789abcdef"
  local out = {}
  for i = 1, 32 do
    local idx = math.random(1, 16)
    out[i] = chars:sub(idx, idx)
  end
  return table.concat(out)
end

local function url_key(video_url, title)
  local s = (video_url or "") .. "|" .. (title or "")
  local hash = 2166136261
  for i = 1, #s do
    hash = bit.bxor(hash, s:byte(i))
    hash = (hash * 16777619) % 4294967296
  end
  return ("%08x"):format(hash)
end

local function is_null(v) return v == nil or v == cjson.null end

local function coalesce(v, default)
  if is_null(v) then return default end
  return v
end

local function is_explicit_query(s)
  local text = (s or ""):lower()
  if text:match("^https?://") then return true end
  if text:match("^[%a%d_]+search:") then return true end
  return false
end

local function is_playlist_url(s)
  local text = s or ""
  if not text:match("^https?://") then return false end
  if text:match("[?&]list=") then return true end
  if text:lower():match("/playlist") or text:lower():match("/sets/") then return true end
  return false
end

local function trunc(s, n)
  s = tostring(s or "")
  if #s <= n then return s end
  return s:sub(1, n - 1) .. "\xe2\x80\xa6"
end

local function urlencode(s)
  s = tostring(s or "")
  return (s:gsub("[^%w%-%.%_%~]", function(c) return ("%%%02X"):format(c:byte()) end))
end

local function make_progress_bar(current, total, length)
  length = length or 15
  current = math.max(0, math.floor(current or 0))
  if not total or total <= 0 then
    return ("[%s] %d:%02d / Live"):format(("\xe2\x96\xac"):rep(length), math.floor(current/60), current % 60)
  end
  total = math.floor(total)
  local progress = math.max(0, math.min(length, math.floor((current/total) * length)))
  local bar = ("\xe2\x96\xac"):rep(progress) .. "\xf0\x9f\x94\x98" .. ("\xe2\x96\xac"):rep(math.max(0, length - progress - 1))
  return ("[%s] %d:%02d / %d:%02d"):format(bar, math.floor(current/60), current % 60, math.floor(total/60), total % 60)
end

-- ============================================================================
-- DB LAYER (Postgres via bot.db / pgmoon -- see lib/swarmlua/pg.lua)
-- ============================================================================

local function Q(sql, ...)
  local rows, err = bot.db:query(sql, ...)
  if not rows then
    io.stderr:write("[symphony] db query failed: " .. tostring(err) .. "\n")
  end
  return rows, err
end

-- ============================================================================
-- ERROR EVENTS + ERROR WEBHOOK (mirrors symphony.py's _persist_error_event() /
-- send_error_webhook_log()) -- writes into the shared symphony_error_events
-- table (same table Aria/SwarmPanel read from for the cross-bot error
-- dashboard) and, if SYMPHONY_ERROR_WEBHOOK_URL (or SWARM_ERROR_WEBHOOK_URL)
-- is configured, posts to a dedicated error webhook. Ported the same way
-- gws.lua/harmonic.lua/nexus.lua/sapphire.lua added this: command handler
-- failures are wired in via bot.on_error (an additive, optional hook -- see
-- lib/swarmlua/bot.lua), and the heartbeat loop below calls report_error
-- directly on failure.
-- ============================================================================
local function persist_error_event(guild_id, level, error_type, title, description)
  Q([[INSERT INTO symphony_error_events (bot_name, guild_id, error_level, error_type, title, description)
      VALUES ('symphony', %s, %s, %s, %s, %s)]],
    guild_id, level, error_type, trunc(title, 255), trunc(description, 5000))
end

local function send_error_webhook(title, description)
  if ERROR_WEBHOOK_URL == "" then return end
  local ok, body = pcall(function()
    local response_body = {}
    local payload = cjson.encode({
      embeds = { { title = "\xf0\x9f\x94\xb4 Symphony Error: " .. trunc(title, 200), description = trunc(description, 4000),
                   color = COLOR.red, footer = { text = "Symphony Music Bot" } } },
    })
    https.request({
      url = ERROR_WEBHOOK_URL, method = "POST",
      headers = { ["Content-Type"] = "application/json", ["Content-Length"] = tostring(#payload) },
      source = ltn12.source.string(payload), sink = ltn12.sink.table(response_body),
    })
  end)
  if not ok then print("[symphony] error webhook failed: " .. tostring(body)) end
end

-- guild_id may be nil (process-wide failures, e.g. the heartbeat loop).
local function report_error(guild_id, error_type, title, description)
  local ok, err = pcall(persist_error_event, guild_id, "error", error_type, title, description)
  if not ok then print("[symphony] failed to persist error event: " .. tostring(err)) end
  send_error_webhook(title, description)
end

-- Wired into swarmlua.bot's additive/optional on_error hook (see
-- lib/swarmlua/bot.lua) so command handler failures also land in
-- symphony_error_events / the error webhook.
bot.on_error = function(name, interaction, err)
  report_error(interaction and interaction.guild_id, "command_error", tostring(name), tostring(err))
end

local function ensure_guild_settings(guild_id)
  Q([[INSERT INTO symphony_guild_settings
        (guild_id, volume, loop_mode, filter_mode, transition_mode, fade_seconds,
         fade_curve, custom_speed, custom_pitch, custom_modifiers_left, dj_only_mode, stay_in_vc)
      VALUES (%s, 100, 'queue', 'none', 'off', 3.0, 'smooth', 1.0, 1.0, 0, false, false)
      ON CONFLICT (guild_id) DO NOTHING]], guild_id)
end

local function get_settings(guild_id)
  ensure_guild_settings(guild_id)
  local rows = Q([[SELECT home_vc_id, COALESCE(volume,100) AS volume, COALESCE(loop_mode,'queue') AS loop_mode,
      COALESCE(filter_mode,'none') AS filter_mode, dj_role_id, feedback_channel_id,
      COALESCE(transition_mode,'off') AS transition_mode, COALESCE(fade_seconds,3.0) AS fade_seconds,
      COALESCE(fade_curve,'smooth') AS fade_curve, COALESCE(custom_speed,1.0) AS custom_speed,
      COALESCE(custom_pitch,1.0) AS custom_pitch, COALESCE(custom_modifiers_left,0) AS custom_modifiers_left,
      COALESCE(dj_only_mode,false) AS dj_only_mode, COALESCE(stay_in_vc,false) AS stay_in_vc, filter_stack
    FROM symphony_guild_settings WHERE guild_id = %s]], guild_id)
  if rows and rows[1] then return rows[1] end
  return {
    home_vc_id = nil, volume = 100, loop_mode = "queue", filter_mode = "none", dj_role_id = nil,
    feedback_channel_id = nil, transition_mode = "off", fade_seconds = 3.0, fade_curve = "smooth",
    custom_speed = 1.0, custom_pitch = 1.0, custom_modifiers_left = 0, dj_only_mode = false, stay_in_vc = false,
  }
end

local function get_autodj(guild_id)
  local rows = Q("SELECT auto_dj FROM symphony_swarm_toggles WHERE guild_id = %s", guild_id)
  if rows and rows[1] then return coalesce(rows[1].auto_dj, false) end
  return false
end

local function set_autodj(guild_id, enabled)
  Q([[INSERT INTO symphony_swarm_toggles (guild_id, auto_dj) VALUES (%s, %s)
      ON CONFLICT (guild_id) DO UPDATE SET auto_dj = EXCLUDED.auto_dj]], guild_id, enabled)
end

local function get_home_channel_id(guild_id)
  local rows = Q("SELECT home_vc_id FROM symphony_bot_home_channels WHERE guild_id = %s AND bot_name = 'symphony'", guild_id)
  if rows and rows[1] and not is_null(rows[1].home_vc_id) then return tostring(rows[1].home_vc_id) end
  return nil
end

local function set_home_channel(guild_id, channel_id)
  Q([[INSERT INTO symphony_bot_home_channels (guild_id, bot_name, home_vc_id) VALUES (%s, 'symphony', %s)
      ON CONFLICT (guild_id, bot_name) DO UPDATE SET home_vc_id = EXCLUDED.home_vc_id]], guild_id, channel_id)
end

-- ============================================================================
-- DISCORD REST HELPERS (embeds, interaction responses -- bot:reply only does plain text)
-- ============================================================================

local function build_embed(opts)
  opts = opts or {}
  local e = {}
  if opts.title then e.title = opts.title end
  if opts.description then e.description = opts.description end
  e.color = opts.color or COLOR.blurple
  if opts.fields then e.fields = opts.fields end
  if opts.footer then e.footer = { text = opts.footer } end
  if opts.author then e.author = { name = opts.author } end
  return e
end

local function callback(interaction, itype, data)
  return bot.rest:post(("/interactions/%s/%s/callback"):format(interaction.id, interaction.token), { type = itype, data = data })
end

local function respond(interaction, opts, ephemeral)
  local data = { embeds = { build_embed(opts) } }
  if ephemeral then data.flags = 64 end
  return callback(interaction, 4, data)
end

local function respond_text(interaction, text, ephemeral)
  local data = { content = text }
  if ephemeral then data.flags = 64 end
  return callback(interaction, 4, data)
end

local function defer(interaction, ephemeral)
  local data = {}
  if ephemeral then data.flags = 64 end
  return callback(interaction, 5, data)
end

local function followup(interaction, opts, ephemeral)
  local data = { embeds = { build_embed(opts) } }
  if ephemeral then data.flags = 64 end
  return bot.rest:post(("/webhooks/%s/%s"):format(bot.application_id, interaction.token), data)
end

local function followup_text(interaction, text, ephemeral)
  local data = { content = text }
  if ephemeral then data.flags = 64 end
  return bot.rest:post(("/webhooks/%s/%s"):format(bot.application_id, interaction.token), data)
end

local function defer_update(interaction)
  return callback(interaction, 6, {})
end

local function update_message(interaction, opts, components)
  local data = { embeds = { build_embed(opts) } }
  if components then data.components = components end
  return callback(interaction, 7, data)
end

local function respond_autocomplete(interaction, choices)
  return callback(interaction, 8, { choices = choices })
end

-- Not a DJ / permission error is common enough to warrant one helper.
local function deny(interaction, text)
  respond(interaction, { description = text, color = COLOR.red }, true)
end

-- ============================================================================
-- USER DISPLAY-NAME RESOLUTION (REST + small TTL cache; no GUILD_MEMBERS intent)
-- ============================================================================

local _name_cache = {} -- key "guild:user" -> { name = , at = }
local NAME_CACHE_TTL = 600

local function resolve_display_name(guild_id, user_id)
  if not user_id then return "Unknown User" end
  local key = tostring(guild_id) .. ":" .. tostring(user_id)
  local cached = _name_cache[key]
  if cached and (os.time() - cached.at) < NAME_CACHE_TTL then return cached.name end
  local name = "User " .. tostring(user_id)
  if guild_id then
    local member, err = bot.rest:get(("/guilds/%s/members/%s"):format(guild_id, user_id))
    if member and member.user then
      name = (member.nick and member.nick ~= cjson.null and member.nick)
        or (member.user.global_name and member.user.global_name ~= cjson.null and member.user.global_name)
        or member.user.username or name
    end
  end
  if name == ("User " .. tostring(user_id)) then
    local user = bot.rest:get(("/users/%s"):format(user_id))
    if user then
      name = (user.global_name and user.global_name ~= cjson.null and user.global_name) or user.username or name
    end
  end
  _name_cache[key] = { name = name, at = os.time() }
  return name
end

local function dm_user(user_id, embed_opts)
  local chan, err = bot.rest:post("/users/@me/channels", { recipient_id = user_id })
  if not chan or not chan.id then return false, err end
  local ok, err2 = bot.rest:post(("/channels/%s/messages"):format(chan.id), { embeds = { build_embed(embed_opts) } })
  return ok ~= nil, err2
end

-- ============================================================================
-- IN-MEMORY RUNTIME STATE
-- ============================================================================

local playback = {}          -- [guild_id] = { url, title, requester_id, track_uid, channel_id,
                              --                duration_ms, position_ms, position_at, paused, speed, current_filter, active }
local processing_guild = {}  -- [guild_id] = true while process_queue is rebuilding for that guild
local vote_skip = {}         -- [guild_id] = { [user_id] = true }
local queue_votes = {}       -- ["guild:id"] = { up = {}, down = {} }
local sleep_deadline = {}    -- [guild_id] = os.time() deadline, checked by the sleep-timer coroutine
local voice_channel_of = {}  -- [guild_id] = channel_id we most recently told the gateway to join

local function with_guild_lock(guild_id, fn)
  local waited = 0
  while processing_guild[guild_id] do
    copas.sleep(0.05)
    waited = waited + 0.05
    if waited > 10 then break end
  end
  processing_guild[guild_id] = true
  local ok, err = pcall(fn)
  processing_guild[guild_id] = nil
  if not ok then
    io.stderr:write("[symphony] guarded section error: " .. tostring(err) .. "\n")
  end
end

-- ============================================================================
-- DJ / PERMISSION CHECKS
-- ============================================================================

local function is_dj(interaction, silent)
  if has_admin(interaction) then return true end
  local settings = get_settings(interaction.guild_id)
  if not settings.dj_only_mode then return true end
  if settings.dj_role_id then
    local roles = (interaction.member and interaction.member.roles) or {}
    for _, r in ipairs(roles) do
      if tostring(r) == tostring(settings.dj_role_id) then return true end
    end
  end
  if not silent then
    deny(interaction, "\xe2\x9d\x8c **Strict DJ Mode is Active.** You need the DJ Role.")
  end
  return false
end

-- ============================================================================
-- QUEUE / BACKUP DB OPERATIONS
-- ============================================================================

local function queue_count(guild_id)
  local rows = Q("SELECT COUNT(*) AS c FROM symphony_queue WHERE guild_id = %s AND bot_name = 'symphony'", guild_id)
  return rows and rows[1] and tonumber(rows[1].c) or 0
end

local function pop_next_queue_row(guild_id)
  local rows = Q([[SELECT id, video_url, title, requester_id, track_uid FROM symphony_queue
      WHERE guild_id = %s AND bot_name = 'symphony' ORDER BY id ASC LIMIT 1]], guild_id)
  if not rows or not rows[1] then return nil end
  local row = rows[1]
  Q("DELETE FROM symphony_queue WHERE id = %s", row.id)
  return row
end

local function insert_queue_end(guild_id, video_url, title, requester_id, track_uid)
  track_uid = track_uid or new_track_uid()
  Q([[INSERT INTO symphony_queue (guild_id, bot_name, video_url, title, requester_id, track_uid)
      VALUES (%s, 'symphony', %s, %s, %s, %s)]], guild_id, video_url, title, requester_id, track_uid)
  return track_uid
end

local function insert_queue_front(guild_id, video_url, title, requester_id, track_uid)
  track_uid = track_uid or new_track_uid()
  local rows = Q("SELECT COALESCE(MIN(id), 0) AS min_id FROM symphony_queue")
  local new_id = (rows and rows[1] and tonumber(rows[1].min_id) or 0) - 1
  Q([[INSERT INTO symphony_queue (id, guild_id, bot_name, video_url, title, requester_id, track_uid)
      VALUES (%s, %s, 'symphony', %s, %s, %s, %s)]], new_id, guild_id, video_url, title, requester_id, track_uid)
  return track_uid
end

local function delete_backup_track(guild_id, track_uid, video_url, title)
  if track_uid then
    Q([[DELETE FROM symphony_queue_backup WHERE ctid = (SELECT ctid FROM symphony_queue_backup
        WHERE guild_id = %s AND bot_name = 'symphony' AND track_uid = %s LIMIT 1)]], guild_id, track_uid)
  elseif video_url or title then
    Q([[DELETE FROM symphony_queue_backup WHERE ctid = (SELECT ctid FROM symphony_queue_backup
        WHERE guild_id = %s AND bot_name = 'symphony' AND video_url = %s AND title = %s LIMIT 1)]],
        guild_id, video_url, title)
  end
end

local function snapshot_queue_backup(guild_id)
  Q("DELETE FROM symphony_queue_backup WHERE guild_id = %s AND bot_name = 'symphony'", guild_id)
  local rows = Q([[SELECT video_url, title, requester_id, track_uid FROM symphony_queue
      WHERE guild_id = %s AND bot_name = 'symphony' ORDER BY id ASC]], guild_id)
  for _, row in ipairs(rows or {}) do
    Q([[INSERT INTO symphony_queue_backup (guild_id, bot_name, video_url, title, requester_id, track_uid)
        VALUES (%s, 'symphony', %s, %s, %s, %s) ON CONFLICT DO NOTHING]],
        guild_id, row.video_url, row.title, row.requester_id, row.track_uid or new_track_uid())
  end
  -- The currently-playing track is already popped from the live queue by the time this runs
  -- from most call sites -- keep its backup row too, so a crash mid-track can still recover it.
  local cur = playback[guild_id]
  if cur and cur.url then
    Q([[INSERT INTO symphony_queue_backup (guild_id, bot_name, video_url, title, requester_id, track_uid)
        VALUES (%s, 'symphony', %s, %s, %s, %s) ON CONFLICT DO NOTHING]],
        guild_id, cur.url, cur.title, cur.requester_id, cur.track_uid or new_track_uid())
  end
end

local function restore_queue_from_backup(guild_id)
  if queue_count(guild_id) > 0 then return 0 end
  local rows = Q([[SELECT video_url, title, requester_id, track_uid FROM symphony_queue_backup
      WHERE guild_id = %s AND bot_name = 'symphony' ORDER BY id ASC]], guild_id)
  local n = 0
  for _, row in ipairs(rows or {}) do
    insert_queue_end(guild_id, row.video_url, row.title, row.requester_id, row.track_uid)
    n = n + 1
  end
  return n
end



-- Fisher-Yates shuffle, optionally keeping the current head (currently-playing-adjacent
-- slot) fixed in place -- mirrors symphony.py's preserve_first behavior.
local function shuffle_queue_rows(guild_id, preserve_first)
  local rows = Q([[SELECT video_url, title, requester_id, track_uid FROM symphony_queue
      WHERE guild_id = %s AND bot_name = 'symphony' ORDER BY id ASC]], guild_id) or {}
  if #rows <= 1 then return #rows end
  local head = nil
  local body = rows
  if preserve_first then
    head = rows[1]
    body = {}
    for i = 2, #rows do body[#body + 1] = rows[i] end
  end
  for i = #body, 2, -1 do
    local j = math.random(1, i)
    body[i], body[j] = body[j], body[i]
  end
  Q("DELETE FROM symphony_queue WHERE guild_id = %s AND bot_name = 'symphony'", guild_id)
  if head then insert_queue_end(guild_id, head.video_url, head.title, head.requester_id, head.track_uid) end
  for _, row in ipairs(body) do
    insert_queue_end(guild_id, row.video_url, row.title, row.requester_id, row.track_uid)
  end
  return #rows
end

-- ===========================================================================
-- Live playlist sync: auto-add newly appeared tracks / auto-remove tracks
-- gone from a queued playlist's source. Ported from alucard.py's
-- playlist_sync_loop -- this was cut fleet-wide during the Lua rewrite
-- (symphony_active_playlists existed only as inert per-guild metadata,
-- written to by nothing but the DELETE calls scattered through /stop,
-- /clear, sleep-timer-elapsed, and queue-drained; is_playlist_url() was
-- defined but never called). Simplified vs. the Python original: no
-- YouTube Data API path (this stack only has Lavalink), so every
-- re-extraction is treated as equally trustworthy and a removal always
-- requires 2 consecutive missing cycles rather than distinguishing an
-- "API-trusted" cycle from a truncating-fallback one.
-- ===========================================================================

local PLAYLIST_SYNC_INTERVAL = tonumber(env("PLAYLIST_SYNC_INTERVAL", "30")) or 30
local PLAYLIST_SYNC_MAX_TRACKED = tonumber(env("PLAYLIST_SYNC_MAX_TRACKED", "500")) or 500
local PLAYLIST_QUEUE_MIN_TRACKS = tonumber(env("PLAYLIST_QUEUE_MIN_TRACKS", "20")) or 20
local playlist_missing_streak = {} -- guild_id -> {track_key -> consecutive cycles missing}

local function clear_active_playlist(guild_id)
  Q("DELETE FROM symphony_active_playlists WHERE guild_id = %s AND bot_name = 'symphony'", guild_id)
  Q("DELETE FROM symphony_active_playlist_tracks WHERE guild_id = %s AND bot_name = 'symphony'", guild_id)
  playlist_missing_streak[guild_id] = nil
end

-- Reuses this file's own url_key() (an 8-hex-char FNV-1a hash, same one
-- bump_track_intelligence() already uses) instead of a raw truncated
-- string -- it fits symphony_active_playlist_tracks.track_key's CHAR(40)
-- column with room to spare, unlike some other bots' playlist_track_key()
-- helpers that had to truncate a URL/title to fit (and got that truncation
-- wrong at least once, tripping the pg.lua reconnect-storm bug fixed
-- above). CHAR (not VARCHAR) still blank-pads on read, so values coming
-- back out of this column need trimming before comparing them.
local function playlist_track_key(url, title)
  return url_key(url, title)
end

local function trim(s)
  return (tostring(s or ""):gsub("%s+$", ""))
end

-- Called right after a /play (or equivalent) call resolves to a real
-- Lavalink loadType=playlist response -- registers it so playlist_sync_tick
-- starts watching it for adds/removals.
local function set_active_playlist(guild_id, playlist_url, entries, requester_id, channel_id)
  if not playlist_url or not entries or #entries == 0 then return end
  Q([[INSERT INTO symphony_active_playlists (guild_id, bot_name, playlist_url, known_track_count, requester_id, channel_id)
      VALUES (%s, 'symphony', %s, %s, %s, %s)
      ON CONFLICT (guild_id, bot_name) DO UPDATE SET playlist_url = EXCLUDED.playlist_url,
        known_track_count = EXCLUDED.known_track_count, requester_id = EXCLUDED.requester_id, channel_id = EXCLUDED.channel_id]],
    guild_id, playlist_url, math.min(#entries, PLAYLIST_SYNC_MAX_TRACKED), requester_id, channel_id)
  Q("DELETE FROM symphony_active_playlist_tracks WHERE guild_id = %s AND bot_name = 'symphony'", guild_id)
  for idx, t in ipairs(entries) do
    if idx > PLAYLIST_SYNC_MAX_TRACKED then break end
    Q([[INSERT INTO symphony_active_playlist_tracks (guild_id, bot_name, playlist_url, position_idx, track_key, track_uid, video_url, title, requester_id)
        VALUES (%s, 'symphony', %s, %s, %s, %s, %s, %s, %s)]],
      guild_id, playlist_url, idx - 1, playlist_track_key(t.uri, t.title), new_track_uid(), t.uri, t.title, requester_id)
  end
  playlist_missing_streak[guild_id] = nil
end

-- One sync cycle across every guild with a registered active playlist:
-- re-extracts each playlist via Lavalink, multiset-diffs it against the
-- last-known snapshot (symphony_active_playlist_tracks), enqueues additions,
-- and removes tracks that have been missing for 2 consecutive cycles
-- (from both the live queue and its backup mirror) -- plus a loop-mode
-- duplicate trim and a queue-health refill, matching the Python original.
local function playlist_sync_tick()
  local playlists = Q("SELECT guild_id, playlist_url, known_track_count, requester_id, channel_id FROM symphony_active_playlists WHERE bot_name = 'symphony'") or {}
  for _, prow in ipairs(playlists) do
    local gid = prow.guild_id
    local ok, err = pcall(function()
      local result, lerr = bot.lavalink:load_tracks(prow.playlist_url)
      if not result or result.loadType == "error" or result.loadType == "empty" then
        log_warn("[%s] playlist sync: re-extract failed: %s", gid, tostring((result and result.data and result.data.message) or lerr or "no response"))
        return
      end
      local raw_entries = (result.loadType == "playlist") and ((result.data and result.data.tracks) or {}) or {}
      local entries = {}
      for _, t in ipairs(raw_entries) do
        local uri = t.info and t.info.uri or t.uri
        local title = t.info and t.info.title or t.title
        if uri then entries[#entries + 1] = { uri = uri, title = title } end
      end
      if #entries == 0 then
        log_warn("[%s] playlist sync: re-extract returned no tracks", gid)
        return
      end
      if #entries > PLAYLIST_SYNC_MAX_TRACKED then
        local trimmed = {}
        for i = 1, PLAYLIST_SYNC_MAX_TRACKED do trimmed[i] = entries[i] end
        entries = trimmed
      end

      local current_rows = {}
      local current_counts = {}
      for _, t in ipairs(entries) do
        local k = playlist_track_key(t.uri, t.title)
        current_rows[#current_rows + 1] = { url = t.uri, title = t.title, key = k }
        current_counts[k] = (current_counts[k] or 0) + 1
      end

      local previous_rows = Q("SELECT track_key, track_uid, video_url, title, requester_id FROM symphony_active_playlist_tracks WHERE guild_id = %s AND bot_name = 'symphony' ORDER BY position_idx ASC", gid) or {}
      local previous_by_key = {}
      local remaining_previous = {}
      for _, p in ipairs(previous_rows) do
        local k = trim(p.track_key or "")
        previous_by_key[k] = previous_by_key[k] or {}
        table.insert(previous_by_key[k], p)
        remaining_previous[k] = (remaining_previous[k] or 0) + 1
      end

      local added_rows = {}
      for _, r in ipairs(current_rows) do
        if remaining_previous[r.key] and remaining_previous[r.key] > 0 then
          remaining_previous[r.key] = remaining_previous[r.key] - 1
        else
          added_rows[#added_rows + 1] = r
        end
      end

      local removed_counts = {}
      local carry_forward = {}
      local streaks = playlist_missing_streak[gid] or {}
      for k, missing_n in pairs(remaining_previous) do
        if missing_n > 0 then
          local streak = (streaks[k] or 0) + 1
          streaks[k] = streak
          if streak >= 2 then
            removed_counts[k] = missing_n
          else
            for i = 1, missing_n do
              local p = previous_by_key[k] and previous_by_key[k][i]
              if p then carry_forward[#carry_forward + 1] = p end
            end
          end
        end
      end
      for k in pairs(current_counts) do streaks[k] = nil end
      playlist_missing_streak[gid] = next(streaks) and streaks or nil

      local purged_live, purged_backup = 0, 0
      if next(removed_counts) ~= nil then
        local budget = {}
        for k, c in pairs(removed_counts) do budget[k] = c end
        local live_rows = Q("SELECT id, video_url, title FROM symphony_queue WHERE guild_id = %s AND bot_name = 'symphony' ORDER BY id ASC", gid) or {}
        for _, lr in ipairs(live_rows) do
          local k = playlist_track_key(lr.video_url, lr.title)
          if budget[k] and budget[k] > 0 then
            budget[k] = budget[k] - 1
            Q("DELETE FROM symphony_queue WHERE id = %s AND guild_id = %s AND bot_name = 'symphony'", lr.id, gid)
            purged_live = purged_live + 1
          end
        end
        budget = {}
        for k, c in pairs(removed_counts) do budget[k] = c end
        local backup_rows = Q("SELECT id, video_url, title FROM symphony_queue_backup WHERE guild_id = %s AND bot_name = 'symphony' ORDER BY id ASC", gid) or {}
        for _, br in ipairs(backup_rows) do
          local k = playlist_track_key(br.video_url, br.title)
          if budget[k] and budget[k] > 0 then
            budget[k] = budget[k] - 1
            Q("DELETE FROM symphony_queue_backup WHERE id = %s AND guild_id = %s AND bot_name = 'symphony'", br.id, gid)
            purged_backup = purged_backup + 1
          end
        end
      end

      if #added_rows > 0 then
        for _, r in ipairs(added_rows) do insert_queue_end(gid, r.url, r.title, prow.requester_id) end
        snapshot_queue_backup(gid)
        if #added_rows > 1 then shuffle_queue_rows(gid, true) end
      end

      -- Trim loop-mode duplicate live-queue copies down to at most 1 per track_key still in the playlist.
      local trim_count = 0
      local current_keys = {}
      for _, r in ipairs(current_rows) do current_keys[r.key] = true end
      local all_rows = Q("SELECT id, video_url, title FROM symphony_queue WHERE guild_id = %s AND bot_name = 'symphony' ORDER BY id ASC", gid) or {}
      local seen = {}
      for _, qr in ipairs(all_rows) do
        local k = playlist_track_key(qr.video_url, qr.title)
        if current_keys[k] then
          if seen[k] then
            Q("DELETE FROM symphony_queue WHERE id = %s AND guild_id = %s AND bot_name = 'symphony'", qr.id, gid)
            trim_count = trim_count + 1
          else
            seen[k] = true
          end
        end
      end

      -- Queue-health refill: independent of the diff above, top the live queue back up
      -- if it's thinner than expected relative to the tracked playlist (drained by
      -- playback/a bug/a restart without the source playlist itself changing).
      local refilled = 0
      local queued_rows = Q("SELECT video_url, title FROM symphony_queue WHERE guild_id = %s AND bot_name = 'symphony'", gid) or {}
      local queued_keys = {}
      local queued_n = 0
      for _, qr in ipairs(queued_rows) do
        local k = playlist_track_key(qr.video_url, qr.title)
        if not queued_keys[k] then queued_keys[k] = true; queued_n = queued_n + 1 end
      end
      local target = math.min(#current_rows, PLAYLIST_QUEUE_MIN_TRACKS)
      if queued_n < target then
        local refill_rows = {}
        for _, r in ipairs(current_rows) do
          if queued_n + #refill_rows >= target then break end
          if not queued_keys[r.key] then refill_rows[#refill_rows + 1] = r end
        end
        for _, r in ipairs(refill_rows) do insert_queue_end(gid, r.url, r.title, prow.requester_id) end
        if #refill_rows > 0 then
          refilled = #refill_rows
          snapshot_queue_backup(gid)
          shuffle_queue_rows(gid, true)
        end
      end

      if #added_rows > 0 or purged_live > 0 or purged_backup > 0 or trim_count > 0 or refilled > 0 then
        log_info("[%s] playlist sync: +%d -%d(live) -%d(backup) trimmed=%d refilled=%d", gid, #added_rows, purged_live, purged_backup, trim_count, refilled)
      end

      -- Persist this cycle's confirmed tracks plus anything still in its missing-streak
      -- grace window as the baseline the next cycle diffs against.
      Q("DELETE FROM symphony_active_playlist_tracks WHERE guild_id = %s AND bot_name = 'symphony'", gid)
      local idx = 0
      for _, r in ipairs(current_rows) do
        Q([[INSERT INTO symphony_active_playlist_tracks (guild_id, bot_name, playlist_url, position_idx, track_key, track_uid, video_url, title, requester_id)
            VALUES (%s, 'symphony', %s, %s, %s, %s, %s, %s, %s)]],
          gid, prow.playlist_url, idx, r.key, new_track_uid(), r.url, r.title, prow.requester_id)
        idx = idx + 1
      end
      for _, p in ipairs(carry_forward) do
        Q([[INSERT INTO symphony_active_playlist_tracks (guild_id, bot_name, playlist_url, position_idx, track_key, track_uid, video_url, title, requester_id)
            VALUES (%s, 'symphony', %s, %s, %s, %s, %s, %s, %s)]],
          gid, prow.playlist_url, idx, trim(p.track_key or ""), (trim(p.track_uid or "") ~= "" and trim(p.track_uid) or new_track_uid()), p.video_url, p.title, p.requester_id)
        idx = idx + 1
      end
      Q("UPDATE symphony_active_playlists SET known_track_count = %s WHERE guild_id = %s AND bot_name = 'symphony'", idx, gid)
    end)
    if not ok then
      log_warn("[%s] playlist sync tick error: %s", gid, tostring(err))
      report_error(gid, "runtime", "playlist sync error", tostring(err))
    end
  end
end

-- ============================================================================
-- TRACK INTELLIGENCE (simplified Smart Auto-DJ learning layer)
-- ============================================================================

local function bump_track_intelligence(guild_id, video_url, title, requester_id, field)
  local key = url_key(video_url, title)
  Q(([[INSERT INTO symphony_track_intelligence
        (guild_id, url_key, video_url, title, queued_count, play_count, finish_count, skip_count,
         like_count, dislike_count, total_listen_seconds, last_requester_id, source, first_seen, last_queued, last_played, updated_at)
      VALUES (%%s, %%s, %%s, %%s, %s, 0, 0, 0, 0, 0, 0, %%s, 'symphony', now(), now(), NULL, now())
      ON CONFLICT (guild_id, url_key) DO UPDATE SET
        %s = symphony_track_intelligence.%s + 1,
        video_url = EXCLUDED.video_url, title = EXCLUDED.title,
        last_requester_id = EXCLUDED.last_requester_id, updated_at = now()%s]])
      :format(field == "queued_count" and 1 or 0, field, field,
              field == "play_count" and ", last_played = now()" or ""),
    guild_id, key, video_url, title, requester_id)
end

local function bump_user_affinity(guild_id, user_id, video_url, title, field)
  local key = url_key(video_url, title)
  Q(([[INSERT INTO symphony_user_track_affinity
        (guild_id, user_id, url_key, video_url, title, queued_count, play_count, finish_count, skip_count,
         like_count, dislike_count, score, last_requested, updated_at)
      VALUES (%%s, %%s, %%s, %%s, %%s, %s, 0, 0, 0, 0, 0, 1.0, now(), now())
      ON CONFLICT (guild_id, user_id, url_key) DO UPDATE SET
        %s = symphony_user_track_affinity.%s + 1,
        score = symphony_user_track_affinity.score + %s,
        video_url = EXCLUDED.video_url, title = EXCLUDED.title, updated_at = now()]])
      :format(field == "queued_count" and 1 or 0, field, field,
              (field == "like_count" and "2.0") or (field == "dislike_count" and "-2.0")
                or (field == "finish_count" and "1.0") or (field == "skip_count" and "-0.5") or "0.5"),
    guild_id, user_id, key, video_url, title)
end

local function record_track_feedback(guild_id, user_id, video_url, title, liked)
  local field = liked and "like_count" or "dislike_count"
  bump_track_intelligence(guild_id, video_url, title, user_id, field)
  bump_user_affinity(guild_id, user_id, video_url, title, field)
end

local function record_history(guild_id, video_url, title, requester_id)
  Q([[INSERT INTO symphony_history (guild_id, video_url, title, played_at, requester_id)
      VALUES (%s, %s, %s, now(), %s)]], guild_id, video_url, title, requester_id)
end

-- ============================================================================
-- LAVALINK SEARCH / QUEUE ENGINE
-- ============================================================================

local function search_playables(query)
  local q = query
  if not is_explicit_query(q) then q = "ytmsearch:" .. q end
  local result, err = bot.lavalink:load_tracks(q)
  if not result then return nil, err end
  if result.loadType == "error" then
    return nil, (result.data and result.data.message) or "Lavalink load error"
  end
  if result.loadType == "empty" then return {}, nil end
  if result.loadType == "track" then return { result.data }, nil end
  if result.loadType == "playlist" then
    return (result.data and result.data.tracks) or {}, result.data and result.data.info
  end
  if result.loadType == "search" then return result.data or {}, nil end
  return {}, nil
end

local function build_filters(settings)
  local filters = {}
  local mode = settings.filter_mode or "none"
  local speed = tonumber(settings.custom_speed) or 1.0
  local eq_applied = false

  local function set_timescale(sp, pitch)
    filters.timescale = { speed = sp, pitch = pitch, rate = 1.0 }
  end
  local function set_eq(bands)
    local arr = {}
    for _, b in ipairs(bands) do arr[#arr+1] = { band = b[1], gain = b[2] } end
    filters.equalizer = arr
    eq_applied = true
  end
  local function blend(preset_bands)
    local base = {}
    for _, b in ipairs(LOUDNORM_EQ) do base[b[1]] = b[2] end
    for _, b in ipairs(preset_bands) do base[b[1]] = (base[b[1]] or 0) + b[2] end
    local out = {}
    for band, gain in pairs(base) do out[#out+1] = { band, gain } end
    table.sort(out, function(a, b) return a[1] < b[1] end)
    return out
  end

  if mode == "nightcore" then set_timescale(1.25, 1.3); speed = 1.25
  elseif mode == "vaporwave" then set_timescale(0.8, 0.8); speed = 0.8
  elseif mode == "bassboost" then set_eq(blend({{0,0.32},{1,0.24},{2,0.12}}))
  elseif mode == "8d" then
    filters.rotation = { rotationHz = 0.18 }
    filters.channelMix = { leftToLeft = 0.8, leftToRight = 0.2, rightToLeft = 0.2, rightToRight = 0.8 }
  elseif mode == "karaoke" then filters.karaoke = { level = 1.0, monoLevel = 1.0, filterBand = 220.0, filterWidth = 100.0 }
  elseif mode == "tremolo" then filters.tremolo = { frequency = 4.0, depth = 0.45 }
  elseif mode == "vibrato" then filters.vibrato = { frequency = 4.5, depth = 0.35 }
  elseif mode == "lowpass" then filters.lowPass = { smoothing = 20.0 }
  elseif mode == "lofi" then filters.lowPass = { smoothing = 35.0 }; set_timescale(0.94, 0.96); speed = 0.94
  elseif mode == "electronic" then set_eq(blend({{0,0.12},{1,0.10},{4,-0.05},{8,0.08},{10,0.14}}))
  elseif mode == "party" then set_eq(blend({{0,0.25},{1,0.18},{2,0.08},{9,0.10}}))
  elseif mode == "radio" then set_eq(blend({{0,-0.18},{1,-0.10},{4,0.12},{5,0.12},{10,-0.12}})); filters.lowPass = { smoothing = 18.0 }
  elseif mode == "cinema" then set_eq(blend({{0,0.18},{1,0.12},{8,0.08},{9,0.10}}))
  elseif mode == "soft" then filters.lowPass = { smoothing = 25.0 }
  elseif mode == "pop" then set_eq(blend({{0,-0.1},{1,0.1},{2,0.2},{3,0.25},{4,0.2},{5,0.1},{6,0.0},{7,-0.05},{8,-0.1},{9,-0.1},{10,-0.05},{11,0.0},{12,0.1},{13,0.2},{14,0.1}}))
  elseif mode == "rock" then set_eq(blend({{0,0.3},{1,0.25},{2,0.1},{3,-0.1},{4,-0.15},{5,-0.1},{6,0.0},{7,0.1},{8,0.3},{9,0.35},{10,0.3},{11,0.2},{12,0.1},{13,0.05},{14,0.0}}))
  elseif mode == "classical" then set_eq(blend({{0,0.1},{1,0.1},{2,0.05},{3,0.0},{4,-0.05},{5,-0.05},{6,-0.05},{7,-0.05},{8,-0.05},{9,-0.05},{10,0.0},{11,0.05},{12,0.1},{13,0.15},{14,0.2}}))
  elseif mode == "earrape" then filters.distortion = { sinOffset=0,sinScale=1.5,cosOffset=0,cosScale=1.5,tanOffset=0,tanScale=1.0,offset=0,scale=1.5 }
  elseif mode == "doubletime" then set_timescale(2.0, 1.0); speed = 2.0
  elseif mode == "slowmo" then set_timescale(0.5, 1.0); speed = 0.5
  elseif mode == "pitch_up" then set_timescale(1.0, 1.3)
  elseif mode == "pitch_down" then set_timescale(1.0, 0.7)
  elseif mode == "deep" then set_eq(blend({{0,0.5},{1,0.4},{2,0.3}})); set_timescale(1.0, 0.8)
  elseif mode == "anime" then set_timescale(1.4, 1.5); speed = 1.4
  end

  if not eq_applied then
    set_eq(LOUDNORM_EQ)
  end

  -- Stacked second filter, if configured (custom_speed/pitch from /modify take priority
  -- and are applied by the caller after this).
  if settings.filter_stack and settings.filter_stack ~= mode and FILTER_VALUES[settings.filter_stack] then
    local stack_mode = settings.filter_stack
    if stack_mode == "nightcore" then filters.timescale = { speed = 1.25, pitch = 1.3, rate = 1.0 }; speed = 1.25
    elseif stack_mode == "vaporwave" then filters.timescale = { speed = 0.8, pitch = 0.8, rate = 1.0 }; speed = 0.8
    elseif stack_mode == "doubletime" then filters.timescale = { speed = 2.0, pitch = 1.0, rate = 1.0 }; speed = 2.0
    elseif stack_mode == "slowmo" then filters.timescale = { speed = 0.5, pitch = 1.0, rate = 1.0 }; speed = 0.5
    end
  end

  return filters, speed
end

local function clear_playback_state(guild_id)
  Q([[UPDATE symphony_playback_state SET channel_id = NULL, video_url = NULL, title = NULL,
      position_seconds = 0, is_playing = FALSE, is_paused = FALSE, play_session_key = NULL, track_uid = NULL
      WHERE guild_id = %s AND bot_name = 'symphony']], guild_id)
  if queue_count(guild_id) == 0 then
    -- nothing left to leave a row for; still make sure a row exists so panel/settings reads don't fail
    Q([[INSERT INTO symphony_playback_state (guild_id, bot_name) VALUES (%s, 'symphony')
        ON CONFLICT (guild_id, bot_name) DO NOTHING]], guild_id)
  end
end

local function persist_playback_state(guild_id, data)
  Q([[INSERT INTO symphony_playback_state (guild_id, bot_name, channel_id, video_url, title,
        position_seconds, is_playing, is_paused, track_uid)
      VALUES (%s, 'symphony', %s, %s, %s, %s, %s, %s, %s)
      ON CONFLICT (guild_id, bot_name) DO UPDATE SET
        channel_id = EXCLUDED.channel_id, video_url = EXCLUDED.video_url, title = EXCLUDED.title,
        position_seconds = EXCLUDED.position_seconds, is_playing = EXCLUDED.is_playing,
        is_paused = EXCLUDED.is_paused, track_uid = EXCLUDED.track_uid,
        last_checkpoint_at = now()]],
    guild_id, data.channel_id, data.url, data.title,
    math.floor((data.position_ms or 0) / 1000), not data.paused, data.paused or false, data.track_uid)
end

local function current_position_seconds(guild_id)
  local d = playback[guild_id]
  if not d then return 0 end
  if d.paused then return math.floor((d.position_ms or 0) / 1000) end
  local elapsed = os.time() - (d.position_at or os.time())
  return math.floor(((d.position_ms or 0) + elapsed * 1000 * (d.speed or 1.0)) / 1000)
end

-- Persists desired/connected voice state per guild (mirrors sapphire.lua's
-- persist_voice_state, same fleet pattern). symphony_voice_state.reconnect_attempts
-- is NOT NULL with no column default (found via live \d introspection during
-- final validation) -- zero-filled here on every upsert, not just where it's
-- bumped, so the very first INSERT for a new guild doesn't throw a not-null
-- violation. Previously this table only ever had one dead UPDATE (in
-- stop_playback below) that matched zero rows because nothing ever inserted
-- one; join_and_wait_for_voice below now creates/refreshes the row so that
-- UPDATE (and Aria/SwarmPanel's cross-bot voice-state dashboard) actually see
-- real data.
local function persist_voice_state(guild_id, channel_id, desired_connected)
  Q([[INSERT INTO symphony_voice_state (guild_id, bot_name, connected_channel_id, last_channel_id, desired_connected, last_seen_at, reconnect_attempts)
      VALUES (%s, 'symphony', %s, %s, %s, now(), 0) ON CONFLICT (guild_id, bot_name) DO UPDATE SET
      connected_channel_id = EXCLUDED.connected_channel_id, last_channel_id = EXCLUDED.last_channel_id,
      desired_connected = EXCLUDED.desired_connected, last_seen_at = now()]],
    guild_id, channel_id, channel_id, desired_connected)
end

-- Wait (briefly) for the Discord voice handshake (VOICE_STATE_UPDATE + VOICE_SERVER_UPDATE)
-- to complete, which is what lets bot:_wire_voice_to_lavalink forward the session to Lavalink.
local function join_and_wait_for_voice(guild_id, channel_id)
  voice_channel_of[guild_id] = channel_id
  bot.gateway:join_voice(guild_id, channel_id, false, true)
  persist_voice_state(guild_id, channel_id, true)
  local waited = 0
  while waited < 8 do
    local vs = bot.voice_state[guild_id]
    if vs and vs.session_id then break end
    copas.sleep(0.25)
    waited = waited + 0.25
  end
  copas.sleep(0.4) -- give VOICE_SERVER_UPDATE -> Lavalink a moment to land
end

-- Port of alucard.py's update_stage_topic/clear_voice_channel_status: without
-- this, a stage channel just sits showing "waiting" in Discord's UI and the
-- member list never shows what's playing, even though audio is flowing fine
-- -- these are purely cosmetic Discord-side signals, not required for audio,
-- but users expect them. Channel type is memoized since it never changes.
local channel_kind_cache = {} -- channel_id -> "stage" | "voice"
local last_stage_topic = {}   -- guild_id -> last topic string sent (dedup)

local function get_channel_kind(channel_id)
  if not channel_id then return "voice" end
  local cached = channel_kind_cache[channel_id]
  if cached then return cached end
  local ch = bot.rest:get("/channels/" .. tostring(channel_id))
  local kind = (ch and ch.type == 13) and "stage" or "voice"
  channel_kind_cache[channel_id] = kind
  return kind
end

local function update_stage_topic(guild_id, channel_id, title)
  if not channel_id then return end
  local ok, err = pcall(function()
    local safe_title = tostring(title or "Unknown Track"):gsub("\n", " "):sub(1, 60)
    local topic = "\xF0\x9F\x8E\xB5 " .. safe_title
    if last_stage_topic[guild_id] == topic then return end
    if get_channel_kind(channel_id) == "stage" then
      local patched = bot.rest:patch("/stage-instances/" .. tostring(channel_id), { topic = topic })
      if not patched then
        bot.rest:post("/stage-instances", { channel_id = tostring(channel_id), topic = topic, privacy_level = 2 })
      end
    else
      bot.rest:patch("/channels/" .. tostring(channel_id), { status = topic })
    end
    last_stage_topic[guild_id] = topic
  end)
  if not ok then io.stderr:write(("[symphony] stage/voice topic update failed for %s: %s\n"):format(tostring(guild_id), tostring(err))) end
end

local function clear_stage_topic(guild_id, channel_id)
  last_stage_topic[guild_id] = nil
  if not channel_id then return end
  pcall(function()
    if get_channel_kind(channel_id) == "stage" then
      bot.rest:delete("/stage-instances/" .. tostring(channel_id))
    else
      bot.rest:patch("/channels/" .. tostring(channel_id), { status = cjson.null })
    end
  end)
end

local process_queue -- forward decl

-- guild_id..":"..track_uid -> consecutive resolve-failure count. A Lavalink/
-- network blip mid-drain used to permanently delete every remaining queued
-- track (each dequeued row was gone before load_tracks even ran, and a
-- failed resolve just deleted the backup copy and moved to the next row),
-- so a short outage could wipe an entire queue in seconds.
local resolve_attempts = {}
local MAX_RESOLVE_ATTEMPTS = 3

local function play_row(guild, guild_id, channel_id, row, settings, depth)
  depth = depth or 0
  if depth > 3 then
    io.stderr:write("[symphony] giving up after repeated load failures for guild " .. tostring(guild_id) .. "\n")
    return
  end

  join_and_wait_for_voice(guild_id, channel_id)

  local retry_key = guild_id .. ":" .. tostring(row.track_uid or row.video_url)
  local result, err = bot.lavalink:load_tracks(row.video_url)
  local track = result and (result.loadType == "track" and result.data
    or (result.loadType == "search" and result.data and result.data[1])
    or (result.loadType == "playlist" and result.data and result.data.tracks and result.data.tracks[1]))

  if not track then
    local attempts = (resolve_attempts[retry_key] or 0) + 1
    if attempts < MAX_RESOLVE_ATTEMPTS then
      resolve_attempts[retry_key] = attempts
      io.stderr:write(("[symphony] resolve failed for guild %s (attempt %d/%d): %s (%s) -- requeuing\n"):format(guild_id, attempts, MAX_RESOLVE_ATTEMPTS, row.title, tostring(err)))
      insert_queue_front(guild_id, row.video_url, row.title, row.requester_id, row.track_uid)
      return
    end
    resolve_attempts[retry_key] = nil
    io.stderr:write(("[symphony] giving up on '%s' for guild %s after %d failed resolves: %s\n"):format(row.title, guild_id, attempts, tostring(err)))
    report_error(guild_id, "runtime", "track resolve failed permanently", ("%s: %s"):format(row.title, tostring(err)))
    delete_backup_track(guild_id, row.track_uid, row.video_url, row.title)
    local nxt = pop_next_queue_row(guild_id)
    if nxt then return play_row(guild, guild_id, channel_id, nxt, settings, depth + 1) end
    clear_playback_state(guild_id)
    playback[guild_id] = nil
    clear_stage_topic(guild_id, channel_id)
    return
  end
  resolve_attempts[retry_key] = nil

  local filters, speed = build_filters(settings)
  if (settings.custom_modifiers_left or 0) > 0 then
    filters.timescale = { speed = settings.custom_speed or 1.0, pitch = settings.custom_pitch or 1.0, rate = 1.0 }
    speed = settings.custom_speed or 1.0
  end

  bot.lavalink:update_player(guild_id, {
    encodedTrack = track.encoded,
    volume = settings.volume or 100,
    paused = false,
    filters = filters,
  })

  local title = row.title or (track.info and track.info.title) or "Unknown Track"
  update_stage_topic(guild_id, channel_id, title)
  playback[guild_id] = {
    url = row.video_url, title = title, requester_id = row.requester_id, track_uid = row.track_uid,
    channel_id = channel_id, duration_ms = track.info and track.info.length or 0,
    position_ms = 0, position_at = os.time(), paused = false, speed = speed,
    current_filter = settings.filter_mode, active = true,
  }

  persist_playback_state(guild_id, playback[guild_id])
  record_history(guild_id, row.video_url, title, row.requester_id)
  bump_track_intelligence(guild_id, row.video_url, title, row.requester_id, "play_count")
  if row.requester_id then
    bump_user_affinity(guild_id, row.requester_id, row.video_url, title, "play_count")
  end

  -- custom modify duration counts down per track played
  if (settings.custom_modifiers_left or 0) > 0 then
    local left = settings.custom_modifiers_left - 1
    Q("UPDATE symphony_guild_settings SET custom_modifiers_left = %s WHERE guild_id = %s", left, guild_id)
    if left <= 0 then
      Q("UPDATE symphony_guild_settings SET custom_speed = 1.0, custom_pitch = 1.0 WHERE guild_id = %s", guild_id)
    end
  end
end

process_queue = function(guild_id, channel_id, opts)
  opts = opts or {}
  local existing = playback[guild_id]
  if existing and existing.active and not opts.force then return end
  if not (bot.lavalink and bot.lavalink.session_id) then
    -- Lavalink isn't connected right now -- leave the queue untouched
    -- rather than dequeuing tracks we can't resolve; the watchdog thread
    -- and the next natural trigger will retry once it's back.
    return
  end

  with_guild_lock(guild_id, function()
    local settings = get_settings(guild_id)
    local row = pop_next_queue_row(guild_id)
    if not row and settings.loop_mode == "queue" then
      if restore_queue_from_backup(guild_id) > 0 then
        shuffle_queue_rows(guild_id, true)
        row = pop_next_queue_row(guild_id)
      end
    end
    if not row then
      clear_playback_state(guild_id)
      playback[guild_id] = nil
      clear_stage_topic(guild_id, channel_id)
      return
    end
    play_row({ id = guild_id }, guild_id, channel_id, row, settings, 0)
  end)
end

local function stop_playback(guild_id)
  clear_stage_topic(guild_id, playback[guild_id] and playback[guild_id].channel_id or get_home_channel_id(guild_id))
  playback[guild_id] = nil
  clear_playback_state(guild_id)
  clear_active_playlist(guild_id)
  Q("DELETE FROM symphony_queue WHERE guild_id = %s AND bot_name = 'symphony'", guild_id)
  Q("DELETE FROM symphony_queue_backup WHERE guild_id = %s AND bot_name = 'symphony'", guild_id)
  Q([[UPDATE symphony_voice_state SET desired_connected = FALSE, connected_channel_id = NULL,
      disconnected_at = now() WHERE guild_id = %s AND bot_name = 'symphony']], guild_id)
  if bot.lavalink and bot.lavalink.session_id then
    bot.lavalink:destroy_player(guild_id)
  end
  bot.gateway:leave_voice(guild_id)
end

local function sync_pause_state(guild_id, paused)
  local d = playback[guild_id]
  if d then
    d.position_ms = current_position_seconds(guild_id) * 1000
    d.position_at = os.time()
    d.paused = paused
    persist_playback_state(guild_id, d)
  end
end

-- ============================================================================
-- LAVALINK EVENT WIRING (track lifecycle + position updates)
-- ============================================================================

bot.lavalink:on("playerUpdate", function(msg)
  local guild_id = msg.guildId
  local d = playback[guild_id]
  if d and msg.state then
    d.position_ms = msg.state.position or d.position_ms
    d.position_at = os.time()
  end
end)

bot.lavalink:on("event", function(msg)
  local guild_id = msg.guildId
  if not guild_id then return end

  if msg.type == "TrackStartEvent" then
    -- playback[] already populated synchronously in play_row(); nothing else to do.
    return
  end

  if msg.type == "TrackEndEvent" or msg.type == "TrackExceptionEvent" or msg.type == "TrackStuckEvent" then
    local reason = msg.reason or (msg.type == "TrackExceptionEvent" and "LOAD_FAILED") or (msg.type == "TrackStuckEvent" and "STUCK") or "FINISHED"
    if reason == "REPLACED" then return end

    local data = playback[guild_id] or {}
    local track_uri = data.url
    local track_title = data.title
    local requester_id = data.requester_id
    local track_uid = data.track_uid
    local channel_id = data.channel_id or voice_channel_of[guild_id]
    local finished = (reason == "FINISHED")

    if track_uri then
      if finished then
        bump_track_intelligence(guild_id, track_uri, track_title, requester_id, "finish_count")
        if requester_id then bump_user_affinity(guild_id, requester_id, track_uri, track_title, "finish_count") end
      else
        bump_track_intelligence(guild_id, track_uri, track_title, requester_id, "skip_count")
        if requester_id then bump_user_affinity(guild_id, requester_id, track_uri, track_title, "skip_count") end
      end

      local settings = get_settings(guild_id)
      if finished then
        if settings.loop_mode == "queue" then
          insert_queue_end(guild_id, track_uri, track_title, requester_id, new_track_uid())
        elseif settings.loop_mode == "song" then
          insert_queue_front(guild_id, track_uri, track_title, requester_id, track_uid)
        else
          delete_backup_track(guild_id, track_uid, track_uri, track_title)
        end
      else
        delete_backup_track(guild_id, track_uid, track_uri, track_title)
      end
    end

    playback[guild_id] = nil
    if channel_id then
      process_queue(guild_id, channel_id, { force = true })
    end
  end
end)

-- ============================================================================
-- VOICE CONNECT HELPER (used by /join and playback start)
-- ============================================================================

local function resolve_target_channel(interaction)
  local home = get_home_channel_id(interaction.guild_id)
  if home then return home end
  local vs = interaction.member and interaction.member.voice
  -- Discord doesn't include voice state on the interaction payload; caller must
  -- already have it from gateway VOICE_STATE_UPDATE tracking. We keep a light
  -- per-guild map of "last known channel per user" populated by the gateway hook below.
  return nil
end

-- Track users' current voice channel from gateway VOICE_STATE_UPDATE (used to find
-- "the channel the invoking user is currently in" for /play, /join, etc, since slash
-- command interaction payloads do not include the invoker's voice state).
local user_voice_channel = {} -- ["guild:user"] = channel_id
bot.gateway:on("VOICE_STATE_UPDATE", function(d)
  if not d.guild_id or not d.user_id then return end
  local key = d.guild_id .. ":" .. d.user_id
  if d.channel_id and d.channel_id ~= cjson.null then
    user_voice_channel[key] = d.channel_id
  else
    user_voice_channel[key] = nil
  end
end)

local function user_current_voice_channel(interaction)
  local uid = iuid(interaction)
  if not uid then return nil end
  return user_voice_channel[interaction.guild_id .. ":" .. uid]
end

-- ============================================================================
-- SMART RECOMMENDATION / TASTE (simplified Auto-DJ learning surface)
-- ============================================================================

local function pick_smart_recommendation(guild_id, listener_ids)
  -- 1) best-scoring track this listener hasn't disliked, not currently queued
  if listener_ids and listener_ids[1] then
    local rows = Q([[SELECT video_url, title, score FROM symphony_user_track_affinity
        WHERE guild_id = %s AND user_id = %s AND dislike_count = 0
        ORDER BY score DESC LIMIT 5]], guild_id, listener_ids[1])
    if rows and rows[1] then
      local pick = rows[math.random(1, #rows)]
      return pick.video_url, pick.title, "your saved taste"
    end
  end
  -- 2) best server-wide track
  local rows = Q([[SELECT video_url, title FROM symphony_track_intelligence
      WHERE guild_id = %s AND dislike_count <= like_count
      ORDER BY (play_count + like_count * 2 - dislike_count * 2) DESC LIMIT 5]], guild_id)
  if rows and rows[1] then
    local pick = rows[math.random(1, #rows)]
    return pick.video_url, pick.title, "server taste"
  end
  return nil, nil, nil
end

local function build_user_taste_summary(guild_id, user_id)
  local rows = Q([[SELECT title, score FROM symphony_user_track_affinity
      WHERE guild_id = %s AND user_id = %s ORDER BY score DESC LIMIT 8]], guild_id, user_id)
  local totals = Q([[SELECT COALESCE(SUM(play_count),0) AS played, COALESCE(SUM(finish_count),0) AS finished,
      COALESCE(SUM(like_count),0) AS liked, COALESCE(SUM(dislike_count),0) AS disliked,
      COALESCE(SUM(skip_count),0) AS skipped
      FROM symphony_user_track_affinity WHERE guild_id = %s AND user_id = %s]], guild_id, user_id)
  return rows or {}, (totals and totals[1]) or { played = 0, finished = 0, liked = 0, disliked = 0, skipped = 0 }
end

-- ============================================================================
-- LYRICS (LRCLIB, no API key -- mirrors symphony.py's fetch_lyrics)
-- ============================================================================

local function fetch_lyrics(title, duration)
  local response = {}
  local ok = https.request({
    url = "https://lrclib.net/api/search?q=" .. urlencode(title),
    method = "GET",
    headers = { ["User-Agent"] = "swarmlua-symphony/1.0" },
    sink = ltn12.sink.table(response),
  })
  if not ok then return nil end
  local raw = table.concat(response)
  local okd, results = pcall(cjson.decode, raw)
  if not okd or type(results) ~= "table" or not results[1] then return nil end
  local best = results[1]
  if duration then
    for _, r in ipairs(results) do
      if r.duration and math.abs(r.duration - duration) <= 5 then best = r; break end
    end
  end
  if not best.syncedLyrics and not best.plainLyrics then return nil end
  local synced = nil
  if best.syncedLyrics then
    synced = {}
    for ts, line in best.syncedLyrics:gmatch("%[(%d%d:%d%d%.%d%d)%](.-)\n") do
      local mm, ss, cc = ts:match("(%d%d):(%d%d)%.(%d%d)")
      local seconds = tonumber(mm) * 60 + tonumber(ss) + tonumber(cc) / 100
      synced[#synced + 1] = { seconds, line }
    end
  end
  return { synced = synced, plain = best.plainLyrics, track_name = best.trackName, artist_name = best.artistName }
end

-- ============================================================================
-- QUEUE-POSITION AUTOCOMPLETE
-- ============================================================================

local function queue_position_choices(guild_id, current)
  local rows = Q([[SELECT title FROM symphony_queue WHERE guild_id = %s AND bot_name = 'symphony'
      ORDER BY id ASC LIMIT 200]], guild_id) or {}
  local needle = (current or ""):lower()
  local choices = {}
  for i, row in ipairs(rows) do
    local title = row.title or "Unknown title"
    if needle == "" or needle == tostring(i) or title:lower():find(needle, 1, true) then
      choices[#choices + 1] = { name = trunc(("%d. %s"):format(i, title), 95), value = i }
      if #choices >= 25 then break end
    end
  end
  return choices
end

-- ============================================================================
-- COMMAND REGISTRATION
-- ============================================================================

local function cmd(name, description, options, handler)
  bot:command(CMD_PREFIX .. name, { description = description, options = options }, handler)
end

-- ---- Server configuration ---------------------------------------------------

cmd("sethome", "Save this bot's default voice or stage channel for join, autoplay, and recovery behavior.",
  { { type = OPT.CHANNEL, name = "channel", description = "Voice or stage channel", required = true, channel_types = {2, 13} } },
  function(_, interaction)
    if not has_admin(interaction) then return deny(interaction, "Administrator permission required.") end
    local channel_id = interaction.data.options[1].value
    set_home_channel(interaction.guild_id, channel_id)
    respond(interaction, { title = "\xf0\x9f\x8f\xa0 Home Set", description = "Home channel set to <#" .. channel_id .. ">.", color = COLOR.green }, true)
  end)

cmd("setfeedback", "Choose the text channel (or thread) for updates, queue actions, and recovery notices.",
  { { type = OPT.CHANNEL, name = "channel", description = "Text channel or thread", required = true, channel_types = {0, 5, 10, 11, 12} } },
  function(_, interaction)
    if not has_admin(interaction) then return deny(interaction, "Administrator permission required.") end
    local channel_id = interaction.data.options[1].value
    ensure_guild_settings(interaction.guild_id)
    Q("UPDATE symphony_guild_settings SET feedback_channel_id = %s WHERE guild_id = %s", channel_id, interaction.guild_id)
    respond(interaction, { title = "\xe2\x9c\x85 Feedback Channel Set", description = "Updates will be sent to <#" .. channel_id .. ">.", color = COLOR.green }, true)
  end)

cmd("djrole", "Set the server DJ role that can manage restricted playback, queue, and settings commands.",
  { { type = OPT.ROLE, name = "role", description = "DJ role", required = true } },
  function(_, interaction)
    if not has_admin(interaction) then return deny(interaction, "Administrator permission required.") end
    local role_id = interaction.data.options[1].value
    ensure_guild_settings(interaction.guild_id)
    Q("UPDATE symphony_guild_settings SET dj_role_id = %s WHERE guild_id = %s", role_id, interaction.guild_id)
    respond(interaction, { description = "\xf0\x9f\x8e\xa7 DJ role set to <@&" .. role_id .. ">", color = COLOR.green }, true)
  end)

cmd("removedj", "Clear the configured DJ role so only admins or open-access mode can control restricted commands.", nil,
  function(_, interaction)
    if not has_admin(interaction) then return deny(interaction, "Administrator permission required.") end
    ensure_guild_settings(interaction.guild_id)
    Q("UPDATE symphony_guild_settings SET dj_role_id = NULL WHERE guild_id = %s", interaction.guild_id)
    respond(interaction, { description = "DJ role requirements removed.", color = COLOR.green }, true)
  end)

cmd("djmode", "Enable or disable Strict DJ Mode so only admins and the DJ role can use control commands.", nil,
  function(_, interaction)
    if not has_admin(interaction) then return deny(interaction, "Administrator permission required.") end
    ensure_guild_settings(interaction.guild_id)
    local settings = get_settings(interaction.guild_id)
    local new_val = not settings.dj_only_mode
    Q("UPDATE symphony_guild_settings SET dj_only_mode = %s WHERE guild_id = %s", new_val, interaction.guild_id)
    respond(interaction, { description = "\xf0\x9f\x8e\xa7 Strict DJ Mode is now **" .. (new_val and "ENABLED" or "DISABLED") .. "**.", color = COLOR.green }, true)
  end)

cmd("247", "Keep the bot connected and ready in voice channels even after playback ends until you disable it.", nil,
  function(_, interaction)
    if not has_admin(interaction) then return deny(interaction, "Administrator permission required.") end
    ensure_guild_settings(interaction.guild_id)
    local settings = get_settings(interaction.guild_id)
    local new_val = not settings.stay_in_vc
    Q("UPDATE symphony_guild_settings SET stay_in_vc = %s WHERE guild_id = %s", new_val, interaction.guild_id)
    respond(interaction, { description = "\xf0\x9f\x95\xb0\xef\xb8\x8f 24/7 Mode is now **" .. (new_val and "ENABLED" or "DISABLED") .. "**.", color = COLOR.green }, true)
  end)

cmd("restart", "Restart this bot instance immediately for maintenance or recovery. Administrator only.", nil,
  function(_, interaction)
    if not has_admin(interaction) then return deny(interaction, "Administrator permission required.") end
    respond_text(interaction, "Restarting...", true)
    os.exit(0) -- expect the container/process supervisor to restart the process
  end)

-- ---- Playback & transport ----------------------------------------------------

cmd("play", "Queue a track, URL, livestream, search result, or playlist and start playback if idle.",
  {
    { type = OPT.STRING, name = "search", description = "Track name, URL, or search text", required = true, autocomplete = true },
    { type = OPT.STRING, name = "source", description = "Where to search (defaults to YouTube for plain text)", required = false,
      choices = { { name = "YouTube", value = "ytmsearch" }, { name = "SoundCloud", value = "scsearch" } } },
  },
  function(_, interaction)
    local opts = interaction.data.options or {}
    local search, source = nil, "ytmsearch"
    for _, o in ipairs(opts) do
      if o.name == "search" then search = o.value end
      if o.name == "source" then source = o.value end
    end
    if (source == "ytmsearch" or source == "scsearch") and not is_explicit_query(search) then
      search = source .. ":" .. search
    end
    defer(interaction, false)

    local channel_id = get_home_channel_id(interaction.guild_id) or user_current_voice_channel(interaction)
    if not channel_id then
      return followup(interaction, { title = "\xe2\x9d\x8c Error", description = "Join a channel first or set a home channel.", color = COLOR.red })
    end

    local entries, playlist_info = search_playables(search)
    if not entries or #entries == 0 then
      return followup(interaction, { title = "\xe2\x9d\x8c Source Error", description = "Could not load that source: " .. tostring(playlist_info or "nothing playable came back"), color = COLOR.red })
    end

    local requester_id = iuid(interaction)
    ensure_guild_settings(interaction.guild_id)
    for _, track in ipairs(entries) do
      local uri = track.info and track.info.uri or track.uri
      local title = track.info and track.info.title or track.title or "Unknown Track"
      local uid = insert_queue_end(interaction.guild_id, uri, title, requester_id)
      bump_track_intelligence(interaction.guild_id, uri, title, requester_id, "queued_count")
      bump_user_affinity(interaction.guild_id, requester_id, uri, title, "queued_count")
    end
    if #entries > 1 then shuffle_queue_rows(interaction.guild_id, true) end
    if playlist_info and is_playlist_url(search) then
      local norm = {}
      for _, track in ipairs(entries) do
        local uri = track.info and track.info.uri or track.uri
        local title = track.info and track.info.title or track.title
        if uri then norm[#norm + 1] = { uri = uri, title = title } end
      end
      set_active_playlist(interaction.guild_id, search, norm, requester_id, channel_id)
    end
    snapshot_queue_backup(interaction.guild_id)
    local q_len = queue_count(interaction.guild_id)

    local already_playing = playback[interaction.guild_id] and playback[interaction.guild_id].active
    if not already_playing then
      followup(interaction, { title = "\xf0\x9f\x8e\xb6 Queued & Starting", description = ("Added **%d** track(s). Starting Lavalink Engine!"):format(#entries), color = COLOR.green })
      process_queue(interaction.guild_id, channel_id)
    else
      followup(interaction, { title = "\xf0\x9f\x93\xa5 Added to Queue", description = ("Added **%d** track(s). (Queue size: %d)"):format(#entries, q_len), color = COLOR.blue })
    end
  end)

cmd("playnext", "Queue one track to play next, ahead of the existing queue, without clearing current playback.",
  { { type = OPT.STRING, name = "search", description = "Track name or URL", required = true } },
  function(_, interaction)
    if not is_dj(interaction) then return end
    local search = interaction.data.options[1].value
    defer(interaction, true)
    local entries, err = search_playables(search)
    if not entries or #entries == 0 then
      return followup(interaction, { description = "Could not resolve that source: " .. tostring(err), color = COLOR.red }, true)
    end
    local track = entries[1]
    local uri = track.info and track.info.uri or track.uri
    local title = track.info and track.info.title or track.title or "Unknown Track"
    local requester_id = iuid(interaction)
    insert_queue_front(interaction.guild_id, uri, title, requester_id)
    bump_track_intelligence(interaction.guild_id, uri, title, requester_id, "queued_count")
    snapshot_queue_backup(interaction.guild_id)

    local active = playback[interaction.guild_id] and playback[interaction.guild_id].active
    if not active then
      local channel_id = get_home_channel_id(interaction.guild_id) or user_current_voice_channel(interaction)
      if channel_id then process_queue(interaction.guild_id, channel_id) end
    end
    followup(interaction, { description = "**Playing next:** " .. title, color = COLOR.green }, true)
  end)

cmd("skip", "Skip the current track and move playback to the next queued item or Auto-DJ recommendation.", nil,
  function(_, interaction)
    if not is_dj(interaction) then return end
    local d = playback[interaction.guild_id]
    if d and d.active and bot.lavalink then
      bot.lavalink:stop(interaction.guild_id)
      respond(interaction, { description = "\xe2\x8f\xad\xef\xb8\x8f Skipped", color = COLOR.blurple }, true)
    else
      respond(interaction, { description = "\xe2\x9d\x8c Nothing is playing.", color = COLOR.red }, true)
    end
  end)

cmd("stop", "Stop playback, clear the queue, remove recovery state, and reset the bot for this server.", nil,
  function(_, interaction)
    if not is_dj(interaction) then return end
    stop_playback(interaction.guild_id)
    respond(interaction, { title = "\xe2\x8f\xb9\xef\xb8\x8f Stopped", description = "Music stopped and cleared.", color = COLOR.red }, true)
  end)

cmd("sleep", "Stop playback and disconnect after N minutes. Use 0 to cancel an active timer.",
  { { type = OPT.INTEGER, name = "minutes", description = "Minutes until playback stops automatically (0 cancels)", required = true, min_value = 0, max_value = 240 } },
  function(_, interaction)
    if not is_dj(interaction) then return end
    local minutes = interaction.data.options[1].value
    local guild_id = interaction.guild_id
    if minutes <= 0 then
      sleep_deadline[guild_id] = nil
      return respond(interaction, { description = "\xe2\x8f\xb0 Sleep timer cancelled.", color = COLOR.orange }, true)
    end
    sleep_deadline[guild_id] = os.time() + minutes * 60
    copas.addthread(function()
      local my_deadline = sleep_deadline[guild_id]
      while sleep_deadline[guild_id] == my_deadline and os.time() < my_deadline do
        copas.sleep(2)
      end
      if sleep_deadline[guild_id] == my_deadline then
        sleep_deadline[guild_id] = nil
        stop_playback(guild_id)
      end
    end)
    respond(interaction, { description = ("\xe2\x9d\x84\xef\xb8\x8f\xf0\x9f\x98\xb4 Sleep timer set for **%d** minute(s). Playback will stop automatically."):format(minutes), color = COLOR.blue }, true)
  end)

cmd("pause", "Pause the current track without clearing the queue or playback position.", nil,
  function(_, interaction)
    if not is_dj(interaction) then return end
    local d = playback[interaction.guild_id]
    if d and d.active and not d.paused then
      bot.lavalink:set_paused(interaction.guild_id, true)
      sync_pause_state(interaction.guild_id, true)
      respond(interaction, { description = "\xe2\x8f\xb8\xef\xb8\x8f Paused", color = COLOR.blue }, true)
    else
      respond(interaction, { description = "\xe2\x9d\x8c Nothing is currently playing.", color = COLOR.red }, true)
    end
  end)

cmd("resume", "Resume the paused track from its current playback position.", nil,
  function(_, interaction)
    if not is_dj(interaction) then return end
    local d = playback[interaction.guild_id]
    if d and d.active and d.paused then
      bot.lavalink:set_paused(interaction.guild_id, false)
      sync_pause_state(interaction.guild_id, false)
      respond(interaction, { description = "\xe2\x96\xb6\xef\xb8\x8f Resumed", color = COLOR.green }, true)
    else
      respond(interaction, { description = "\xe2\x9d\x8c Nothing is currently paused.", color = COLOR.red }, true)
    end
  end)

cmd("replay", "Restart the current track from the beginning without changing the queue.", nil,
  function(_, interaction)
    if not is_dj(interaction) then return end
    local guild_id = interaction.guild_id
    if not playback[guild_id] then return respond(interaction, { description = "Nothing playing.", color = COLOR.red }, true) end
    bot.lavalink:seek(guild_id, 0)
    playback[guild_id].position_ms = 0
    playback[guild_id].position_at = os.time()
    respond(interaction, { description = "Replaying song.", color = COLOR.green }, true)
  end)

local function do_seek(interaction, compute_target, response_text)
  if not is_dj(interaction) then return end
  local guild_id = interaction.guild_id
  if not playback[guild_id] then return respond(interaction, { description = "Nothing playing.", color = COLOR.red }, true) end
  local target = math.max(0, compute_target())
  bot.lavalink:seek(guild_id, target * 1000)
  playback[guild_id].position_ms = target * 1000
  playback[guild_id].position_at = os.time()
  respond(interaction, { description = response_text, color = COLOR.green }, true)
end

cmd("seek", "Jump to an exact time in the current track using seconds from the start.",
  { { type = OPT.INTEGER, name = "seconds", description = "Seconds from the start", required = true } },
  function(_, interaction)
    local seconds = interaction.data.options[1].value
    do_seek(interaction, function() return seconds end, ("Seeked to %ds"):format(seconds))
  end)

cmd("forward", "Jump forward within the current track by the number of seconds you provide.",
  { { type = OPT.INTEGER, name = "seconds", description = "Seconds to jump forward", required = true } },
  function(_, interaction)
    local seconds = interaction.data.options[1].value
    do_seek(interaction, function() return current_position_seconds(interaction.guild_id) + seconds end, ("Skipped forward %ds"):format(seconds))
  end)

cmd("rewind", "Jump backward within the current track by the number of seconds you provide.",
  { { type = OPT.INTEGER, name = "seconds", description = "Seconds to jump backward", required = true } },
  function(_, interaction)
    local seconds = interaction.data.options[1].value
    do_seek(interaction, function() return current_position_seconds(interaction.guild_id) - seconds end, ("Rewound %ds"):format(seconds))
  end)

cmd("join", "Force the bot to join your current voice channel, or its configured home channel if one is saved.", nil,
  function(_, interaction)
    defer(interaction, true)
    local channel_id = get_home_channel_id(interaction.guild_id) or user_current_voice_channel(interaction)
    if not channel_id then
      return followup_text(interaction, "Join a channel first, or set a home channel.", true)
    end
    join_and_wait_for_voice(interaction.guild_id, channel_id)
    followup(interaction, { description = "Joined <#" .. channel_id .. ">.", color = COLOR.green })
  end)

cmd("leave", "Disconnect the bot from voice and clear any pending recovery handoff for this server.", nil,
  function(_, interaction)
    if not is_dj(interaction) then return end
    stop_playback(interaction.guild_id)
    respond(interaction, { description = "Left the channel.", color = COLOR.orange }, true)
  end)

cmd("nowplaying", "Show the current track, progress bar, requester, and live playback status.", nil,
  function(_, interaction)
    local d = playback[interaction.guild_id]
    if not d then return respond(interaction, { description = "Nothing playing.", color = COLOR.red }, true) end
    local bar = make_progress_bar(current_position_seconds(interaction.guild_id), math.floor((d.duration_ms or 0) / 1000))
    local fields = { { name = "Filter", value = tostring(d.current_filter or "none"), inline = true } }
    if d.requester_id then
      table.insert(fields, 1, { name = "Requested by", value = resolve_display_name(interaction.guild_id, d.requester_id), inline = true })
    end
    respond(interaction, {
      title = "\xf0\x9f\x8e\xb5 Now Playing",
      description = ("**[%s](%s)**\n\n`%s`"):format(d.title or "Playing", d.url, bar),
      color = COLOR.blue, fields = fields,
    }, true)
  end)

-- ---- Queue management ---------------------------------------------------

cmd("queue", "Show the current queue with paging, requester names, and track positions", {
  { type = OPT.INTEGER, name = "page", description = "Page number", required = false } },
  function(_, interaction)
    defer(interaction, true)
    local page = 1
    if interaction.data.options and interaction.data.options[1] then page = math.max(1, interaction.data.options[1].value) end
    local per_page = 10
    local offset = (page - 1) * per_page
    local total = queue_count(interaction.guild_id)
    local rows = Q([[SELECT title, requester_id FROM symphony_queue WHERE guild_id = %s AND bot_name = 'symphony'
        ORDER BY id ASC LIMIT %s OFFSET %s]], interaction.guild_id, per_page, offset) or {}
    if #rows == 0 then return followup(interaction, { description = "Queue empty.", color = COLOR.red }, true) end
    local lines = {}
    for i, row in ipairs(rows) do
      lines[#lines + 1] = ("**%d.** %s \xe2\x80\x94 *%s*"):format(offset + i, row.title, resolve_display_name(interaction.guild_id, row.requester_id))
    end
    local pages = math.max(1, math.ceil(total / per_page))
    followup(interaction, { title = "\xf0\x9f\x93\x9c Queue", description = table.concat(lines, "\n"), color = COLOR.blurple,
      footer = ("Page %d/%d \xe2\x80\xa2 %d queued track(s)"):format(page, pages, total) }, true)
  end)

cmd("shuffle", "Smart-shuffle the upcoming queue while keeping duplicate tracks separated when possible.", nil,
  function(_, interaction)
    if not is_dj(interaction) then return end
    local n = shuffle_queue_rows(interaction.guild_id, false)
    if n <= 0 then return respond_text(interaction, "Queue empty.", true) end
    snapshot_queue_backup(interaction.guild_id)
    respond(interaction, { description = ("\xf0\x9f\x94\x80 Shuffled %d queued track(s)."):format(n), color = COLOR.green }, true)
  end)

cmd("remove", "Remove a queued track by its queue number so it will not play later.",
  { { type = OPT.INTEGER, name = "index", description = "Queue position", required = true, autocomplete = true } },
  function(_, interaction)
    if not is_dj(interaction) then return end
    local index = interaction.data.options[1].value
    if index < 1 then return respond_text(interaction, "Invalid index.", true) end
    local rows = Q([[SELECT id, video_url, title, track_uid FROM symphony_queue WHERE guild_id = %s AND bot_name = 'symphony'
        ORDER BY id ASC LIMIT 1 OFFSET %s]], interaction.guild_id, index - 1)
    if not rows or not rows[1] then return respond_text(interaction, "Invalid index.", true) end
    local row = rows[1]
    Q("DELETE FROM symphony_queue WHERE id = %s", row.id)
    delete_backup_track(interaction.guild_id, row.track_uid, row.video_url, row.title)
    respond(interaction, { description = ("Removed item #%d"):format(index), color = COLOR.green }, true)
  end)

cmd("skipto", "Drop everything before a chosen queue position and jump playback forward to that track.",
  { { type = OPT.INTEGER, name = "index", description = "Queue position", required = true, autocomplete = true } },
  function(_, interaction)
    if not is_dj(interaction) then return end
    local index = interaction.data.options[1].value
    if index < 1 then return respond_text(interaction, "Invalid index.", true) end
    defer(interaction, true)
    local skip_n = index - 1
    if skip_n > 0 then
      local rows = Q([[SELECT id, video_url, title, track_uid FROM symphony_queue WHERE guild_id = %s AND bot_name = 'symphony'
          ORDER BY id ASC LIMIT %s]], interaction.guild_id, skip_n) or {}
      for _, row in ipairs(rows) do
        Q("DELETE FROM symphony_queue WHERE id = %s", row.id)
        delete_backup_track(interaction.guild_id, row.track_uid, row.video_url, row.title)
      end
    end
    if playback[interaction.guild_id] and bot.lavalink then bot.lavalink:stop(interaction.guild_id) end
    followup(interaction, { description = ("Skipped to #%d"):format(index), color = COLOR.green }, true)
  end)

cmd("move", "Move a queued track from one queue slot to another without rebuilding the entire session manually.",
  { { type = OPT.INTEGER, name = "frm", description = "From position", required = true, autocomplete = true },
    { type = OPT.INTEGER, name = "to", description = "To position", required = true, autocomplete = true } },
  function(_, interaction)
    if not is_dj(interaction) then return end
    local frm, to
    for _, o in ipairs(interaction.data.options) do
      if o.name == "frm" then frm = o.value end
      if o.name == "to" then to = o.value end
    end
    defer(interaction, true)
    local rows = Q([[SELECT video_url, title, requester_id, track_uid FROM symphony_queue
        WHERE guild_id = %s AND bot_name = 'symphony' ORDER BY id ASC]], interaction.guild_id) or {}
    if frm > #rows or to > #rows or frm < 1 or to < 1 then
      return followup_text(interaction, "Invalid index", true)
    end
    local item = table.remove(rows, frm)
    table.insert(rows, math.max(1, math.min(to, #rows + 1)), item)
    Q("DELETE FROM symphony_queue WHERE guild_id = %s AND bot_name = 'symphony'", interaction.guild_id)
    for _, row in ipairs(rows) do
      insert_queue_end(interaction.guild_id, row.video_url, row.title, row.requester_id, row.track_uid)
    end
    snapshot_queue_backup(interaction.guild_id)
    followup(interaction, { description = ("Moved item from %d to %d"):format(frm, to), color = COLOR.green }, true)
  end)

cmd("bump", "Move a queued track to the front so it plays next", {
  { type = OPT.INTEGER, name = "index", description = "Queue position", required = true, autocomplete = true } },
  function(_, interaction)
    if not is_dj(interaction) then return end
    local index = interaction.data.options[1].value
    if index < 1 then return respond_text(interaction, "Invalid index.", true) end
    local rows = Q([[SELECT id, video_url, title, requester_id, track_uid FROM symphony_queue
        WHERE guild_id = %s AND bot_name = 'symphony' ORDER BY id ASC LIMIT 1 OFFSET %s]], interaction.guild_id, index - 1)
    if not rows or not rows[1] then return respond_text(interaction, "Invalid index.", true) end
    local row = rows[1]
    Q("DELETE FROM symphony_queue WHERE id = %s", row.id)
    insert_queue_front(interaction.guild_id, row.video_url, row.title, row.requester_id, row.track_uid)
    snapshot_queue_backup(interaction.guild_id)
    respond(interaction, { description = "\xe2\xac\x86\xef\xb8\x8f Moved **" .. row.title .. "** to play next.", color = COLOR.green }, true)
  end)

cmd("clearmine", "Remove your own queued songs without touching other listeners' tracks", nil,
  function(_, interaction)
    defer(interaction, true)
    local uid = iuid(interaction)
    local rows = Q([[SELECT COUNT(*) AS c FROM symphony_queue WHERE guild_id = %s AND bot_name = 'symphony' AND requester_id = %s]], interaction.guild_id, uid)
    local removed = rows and rows[1] and tonumber(rows[1].c) or 0
    if removed > 0 then
      Q("DELETE FROM symphony_queue WHERE guild_id = %s AND bot_name = 'symphony' AND requester_id = %s", interaction.guild_id, uid)
      Q("DELETE FROM symphony_queue_backup WHERE guild_id = %s AND bot_name = 'symphony' AND requester_id = %s", interaction.guild_id, uid)
    end
    followup(interaction, { description = ("\xf0\x9f\xa7\xb9 Removed **%d** of your queued track(s)."):format(removed), color = COLOR.green }, true)
  end)

cmd("clear", "Clear the upcoming queue, stop playback, and reset stored playback state for this server.", nil,
  function(_, interaction)
    if not is_dj(interaction) then return end
    stop_playback(interaction.guild_id)
    respond(interaction, { description = "\xf0\x9f\x97\x91\xef\xb8\x8f Playback stopped and queue cleared.", color = COLOR.red }, true)
  end)

cmd("voteskip", "Start or join a vote skip when no DJ is around to skip directly", nil,
  function(_, interaction)
    local guild_id = interaction.guild_id
    local d = playback[guild_id]
    if not d or not d.active then return respond_text(interaction, "Nothing is playing right now.", true) end
    if is_dj(interaction, true) then
      bot.lavalink:stop(guild_id)
      return respond(interaction, { description = "\xe2\x8f\xad\xef\xb8\x8f DJ override used: skipped the current track.", color = COLOR.green }, true)
    end
    local uid = iuid(interaction)
    vote_skip[guild_id] = vote_skip[guild_id] or {}
    vote_skip[guild_id][uid] = true
    local count = 0
    for _ in pairs(vote_skip[guild_id]) do count = count + 1 end
    local required = 2 -- listener count isn't visible without full member cache; 2-vote floor mirrors the Python minimum.
    if count >= required then
      vote_skip[guild_id] = nil
      bot.lavalink:stop(guild_id)
      return respond(interaction, { description = ("\xe2\x8f\xad\xef\xb8\x8f Vote skip passed with **%d** vote(s)."):format(count), color = COLOR.green })
    end
    respond(interaction, { description = ("\xf0\x9f\x97\xb3\xef\xb8\x8f Vote recorded: **%d/%d** votes to skip."):format(count, required), color = COLOR.blurple }, true)
  end)

cmd("upvote", "Upvote a queued track; enough listener upvotes bump it to play next.",
  { { type = OPT.INTEGER, name = "index", description = "Queue position", required = true } },
  function(_, interaction)
    local guild_id = interaction.guild_id
    local index = interaction.data.options[1].value
    if index < 1 then return respond_text(interaction, "Invalid index.", true) end
    local rows = Q([[SELECT id, video_url, title, requester_id, track_uid FROM symphony_queue
        WHERE guild_id = %s AND bot_name = 'symphony' ORDER BY id ASC LIMIT 1 OFFSET %s]], guild_id, index - 1)
    if not rows or not rows[1] then return respond_text(interaction, "Invalid index.", true) end
    local row = rows[1]
    local key = guild_id .. ":" .. row.id
    queue_votes[key] = queue_votes[key] or { up = {}, down = {} }
    local uid = iuid(interaction)
    queue_votes[key].down[uid] = nil
    queue_votes[key].up[uid] = true
    local up_count = 0
    for _ in pairs(queue_votes[key].up) do up_count = up_count + 1 end
    local required = 2
    if up_count >= required then
      queue_votes[key] = nil
      Q("DELETE FROM symphony_queue WHERE id = %s", row.id)
      insert_queue_front(guild_id, row.video_url, row.title, row.requester_id, row.track_uid)
      snapshot_queue_backup(guild_id)
      return respond(interaction, { description = "\xe2\xac\x86\xef\xb8\x8f **" .. row.title .. "** got enough upvotes and will play next!", color = COLOR.green })
    end
    respond(interaction, { description = ("\xf0\x9f\x94\xba Upvoted **#%d** (%d/%d) to play next."):format(index, up_count, required), color = COLOR.blurple }, true)
  end)

cmd("downvote", "Downvote a queued track; enough listener downvotes removes it from the queue.",
  { { type = OPT.INTEGER, name = "index", description = "Queue position", required = true } },
  function(_, interaction)
    local guild_id = interaction.guild_id
    local index = interaction.data.options[1].value
    if index < 1 then return respond_text(interaction, "Invalid index.", true) end
    local rows = Q([[SELECT id, video_url, title, track_uid FROM symphony_queue
        WHERE guild_id = %s AND bot_name = 'symphony' ORDER BY id ASC LIMIT 1 OFFSET %s]], guild_id, index - 1)
    if not rows or not rows[1] then return respond_text(interaction, "Invalid index.", true) end
    local row = rows[1]
    local key = guild_id .. ":" .. row.id
    queue_votes[key] = queue_votes[key] or { up = {}, down = {} }
    local uid = iuid(interaction)
    queue_votes[key].up[uid] = nil
    queue_votes[key].down[uid] = true
    local down_count = 0
    for _ in pairs(queue_votes[key].down) do down_count = down_count + 1 end
    local required = 2
    if down_count >= required then
      queue_votes[key] = nil
      Q("DELETE FROM symphony_queue WHERE id = %s", row.id)
      delete_backup_track(guild_id, row.track_uid, row.video_url, row.title)
      return respond(interaction, { description = "\xe2\xac\x87\xef\xb8\x8f **" .. row.title .. "** got enough downvotes and was removed from the queue.", color = COLOR.orange })
    end
    respond(interaction, { description = ("\xf0\x9f\x94\xbb Downvoted **#%d** (%d/%d)."):format(index, down_count, required), color = COLOR.blurple }, true)
  end)

cmd("loop", "Choose whether playback loops nothing, the current song, or the full queue.",
  { { type = OPT.STRING, name = "mode", description = "Loop mode", required = true,
      choices = { { name = "Off", value = "off" }, { name = "Song", value = "song" }, { name = "Queue", value = "queue" } } } },
  function(_, interaction)
    if not is_dj(interaction) then return end
    local mode = interaction.data.options[1].value
    ensure_guild_settings(interaction.guild_id)
    Q("UPDATE symphony_guild_settings SET loop_mode = %s WHERE guild_id = %s", mode, interaction.guild_id)
    respond(interaction, { description = "\xf0\x9f\x94\x81 Looping set to: " .. mode, color = COLOR.green }, true)
  end)

cmd("radio", "Queue several tracks matching a mood or vibe, e.g. 'chill lofi' or 'workout hype'.",
  { { type = OPT.STRING, name = "mood", description = "Mood or vibe", required = true },
    { type = OPT.INTEGER, name = "count", description = "How many tracks (1-10)", required = false, min_value = 1, max_value = 10 } },
  function(_, interaction)
    local mood, count = nil, 5
    for _, o in ipairs(interaction.data.options) do
      if o.name == "mood" then mood = o.value end
      if o.name == "count" then count = o.value end
    end
    defer(interaction, false)
    local channel_id = user_current_voice_channel(interaction)
    if not channel_id then return followup(interaction, { description = "Join a voice channel first.", color = COLOR.red }) end
    local entries, err = search_playables(trunc(mood, 100) .. " mix")
    if not entries or #entries == 0 then
      return followup(interaction, { description = ("Couldn't find tracks for **%s**."):format(mood), color = COLOR.red })
    end
    local n = math.min(#entries, count)
    local requester_id = iuid(interaction)
    ensure_guild_settings(interaction.guild_id)
    for i = 1, n do
      local track = entries[i]
      local uri = track.info and track.info.uri or track.uri
      local title = track.info and track.info.title or track.title or "Unknown Track"
      insert_queue_end(interaction.guild_id, uri, title, requester_id)
    end
    snapshot_queue_backup(interaction.guild_id)
    if not (playback[interaction.guild_id] and playback[interaction.guild_id].active) then
      process_queue(interaction.guild_id, channel_id)
    end
    followup(interaction, { title = "\xf0\x9f\x93\xbb Mood Radio", description = ("Queued **%d** track(s) for the vibe: *%s*."):format(n, mood), color = COLOR.green })
  end)

-- ---- Audio & filters ---------------------------------------------------

cmd("volume", "Set the playback volume for this server from 1 to 200 percent.",
  { { type = OPT.INTEGER, name = "vol", description = "Volume percent", required = true } },
  function(_, interaction)
    if not is_dj(interaction) then return end
    local vol = math.max(1, math.min(200, interaction.data.options[1].value))
    ensure_guild_settings(interaction.guild_id)
    Q("UPDATE symphony_guild_settings SET volume = %s WHERE guild_id = %s", vol, interaction.guild_id)
    if playback[interaction.guild_id] and bot.lavalink then bot.lavalink:set_volume(interaction.guild_id, vol) end
    respond(interaction, { description = ("\xf0\x9f\x94\x8a Volume set to %d%%"):format(vol), color = COLOR.green }, true)
  end)

cmd("filter", "Apply an audio filter such as nightcore, vaporwave, or bass boost to upcoming playback.",
  { { type = OPT.STRING, name = "mode", description = "Choose an audio filter to apply", required = true, choices = FILTER_CHOICES },
    { type = OPT.STRING, name = "stack", description = "Optional second filter to layer on top", required = false, choices = FILTER_CHOICES } },
  function(_, interaction)
    if not is_dj(interaction) then return end
    local mode, stack = nil, nil
    for _, o in ipairs(interaction.data.options) do
      if o.name == "mode" then mode = o.value end
      if o.name == "stack" then stack = o.value end
    end
    if not FILTER_VALUES[mode] then return respond_text(interaction, "Invalid filter.", true) end
    if stack and not FILTER_VALUES[stack] then return respond_text(interaction, "Invalid stacked filter.", true) end
    ensure_guild_settings(interaction.guild_id)
    local stack_value = (stack == nil or stack == "none") and nil or stack
    Q([[UPDATE symphony_guild_settings SET filter_mode = %s, filter_stack = %s,
        custom_speed = 1.0, custom_pitch = 1.0, custom_modifiers_left = 0 WHERE guild_id = %s]],
      mode, stack_value, interaction.guild_id)
    if playback[interaction.guild_id] and bot.lavalink then
      local settings = get_settings(interaction.guild_id)
      local filters, speed = build_filters(settings)
      bot.lavalink:update_player(interaction.guild_id, { filters = filters })
      playback[interaction.guild_id].speed = speed
      playback[interaction.guild_id].current_filter = mode
    end
    local label = FILTER_LABELS[mode] or mode
    if stack_value and stack_value ~= mode then
      respond(interaction, { description = ("\xf0\x9f\x8e\x9b\xef\xb8\x8f Filter set to: **%s** + **%s**."):format(label, FILTER_LABELS[stack_value] or stack_value), color = COLOR.blurple }, true)
    else
      respond(interaction, { description = "\xf0\x9f\x8e\x9b\xef\xb8\x8f Filter set to: **" .. label .. "**.", color = COLOR.blurple }, true)
    end
  end)

cmd("fade", "Customize track fade transitions, let the bot pick smart fade timing, or enable BPM-matched mixing.",
  { { type = OPT.STRING, name = "mode", description = "Fade mode", required = true,
      choices = { { name = "Smart Adaptive Fades", value = "smart" }, { name = "Custom Fades", value = "fade" },
                  { name = "Beat-Matched Mixing", value = "mix" }, { name = "Disable Fades", value = "off" } } },
    { type = OPT.NUMBER, name = "seconds", description = "Fade length in seconds (0.5-20)", required = false },
    { type = OPT.STRING, name = "curve", description = "Volume curve", required = false,
      choices = { { name = "Linear", value = "linear" }, { name = "Smooth", value = "smooth" },
                  { name = "Slow Start", value = "ease_in" }, { name = "Soft Land", value = "ease_out" } } } },
  function(_, interaction)
    if not is_dj(interaction) then return end
    local mode, seconds, curve = nil, 3.0, "smooth"
    for _, o in ipairs(interaction.data.options) do
      if o.name == "mode" then mode = o.value end
      if o.name == "seconds" then seconds = o.value end
      if o.name == "curve" then curve = o.value end
    end
    seconds = math.max(0.25, math.min(12.0, seconds or 3.0))
    ensure_guild_settings(interaction.guild_id)
    Q("UPDATE symphony_guild_settings SET transition_mode = %s, fade_seconds = %s, fade_curve = %s WHERE guild_id = %s",
      mode, seconds, curve, interaction.guild_id)
    local message, color
    if mode == "smart" then
      message, color = "\xf0\x9f\x8c\x8a Smart fades enabled. Short, smooth ramps that adapt to track length and active filters.", COLOR.green
    elseif mode == "fade" then
      message, color = ("\xf0\x9f\x8c\x8a Custom fades enabled: %gs using %s."):format(seconds, curve:gsub("_", " ")), COLOR.green
    elseif mode == "mix" then
      message, color = "\xf0\x9f\x8e\x9a\xef\xb8\x8f Beat-matched mixing enabled. Compatible-BPM transitions blend; mismatched tempos cut cleanly instead.", COLOR.green
    else
      message, color = "\xe2\x8f\xb9\xef\xb8\x8f Smooth fades disabled.", COLOR.red
    end
    respond(interaction, { description = message, color = color }, true)
  end)

cmd("modify", "Apply temporary custom speed and pitch modifiers to the next few tracks in the queue.",
  { { type = OPT.NUMBER, name = "speed", description = "Speed multiplier (0.5-2.0)", required = false },
    { type = OPT.NUMBER, name = "pitch", description = "Pitch multiplier (0.5-2.0)", required = false },
    { type = OPT.INTEGER, name = "duration", description = "How many tracks this lasts (default 1)", required = false } },
  function(_, interaction)
    if not is_dj(interaction) then return end
    local speed, pitch, duration = 1.0, 1.0, 1
    for _, o in ipairs(interaction.data.options or {}) do
      if o.name == "speed" then speed = o.value end
      if o.name == "pitch" then pitch = o.value end
      if o.name == "duration" then duration = o.value end
    end
    speed = math.max(0.5, math.min(2.0, speed))
    pitch = math.max(0.5, math.min(2.0, pitch))
    ensure_guild_settings(interaction.guild_id)
    local settings = get_settings(interaction.guild_id)
    if settings.filter_mode ~= "none" then
      return respond(interaction, { description = "\xe2\x9d\x8c **Conflict:** Disable standard Filters via `/" .. CMD_PREFIX .. "filter none` first.", color = COLOR.red }, true)
    end
    Q("UPDATE symphony_guild_settings SET custom_speed = %s, custom_pitch = %s, custom_modifiers_left = %s WHERE guild_id = %s",
      speed, pitch, duration, interaction.guild_id)
    if playback[interaction.guild_id] and bot.lavalink then
      bot.lavalink:update_player(interaction.guild_id, { filters = { timescale = { speed = speed, pitch = pitch, rate = 1.0 } } })
      playback[interaction.guild_id].speed = speed
    end
    respond(interaction, { title = "\xf0\x9f\x8e\x9b\xef\xb8\x8f Audio Modifiers Set",
      description = ("**Speed:** %gx\n**Pitch:** %gx\n*Active for the next %d track(s).*"):format(speed, pitch, duration),
      color = COLOR.gold }, true)
  end)

-- ---- Smart Auto-DJ & discovery -------------------------------------------

cmd("autodj", "Enable or disable smarter Auto-DJ recommendations when the queue runs dry",
  { { type = OPT.BOOLEAN, name = "enabled", description = "Enable Auto-DJ", required = true } },
  function(_, interaction)
    if not is_dj(interaction) then return end
    local enabled = interaction.data.options[1].value
    set_autodj(interaction.guild_id, enabled)
    respond(interaction, { description = "\xf0\x9f\x93\xbb Auto-DJ is now **" .. (enabled and "enabled" or "disabled") .. "**.", color = COLOR.green }, true)
  end)

cmd("recommend", "Queue a smart recommendation learned from server taste, playlists, and listener feedback.",
  { { type = OPT.USER, name = "member", description = "Base the recommendation on this member's taste", required = false } },
  function(_, interaction)
    defer(interaction, false)
    local target_id = iuid(interaction)
    if interaction.data.options and interaction.data.options[1] then target_id = interaction.data.options[1].value end
    local channel_id = (playback[interaction.guild_id] and playback[interaction.guild_id].channel_id)
      or get_home_channel_id(interaction.guild_id) or user_current_voice_channel(interaction)
    if not channel_id then
      return followup(interaction, { title = "Source Error", description = "Join a channel first or set a home channel.", color = COLOR.red })
    end
    local url, title, reason = pick_smart_recommendation(interaction.guild_id, { target_id })
    if not url then
      return followup(interaction, { description = "I could not find a recommendation yet. Play a few tracks or save a playlist first.", color = COLOR.red })
    end
    local requester_id = iuid(interaction)
    insert_queue_end(interaction.guild_id, url, title, requester_id)
    bump_track_intelligence(interaction.guild_id, url, title, requester_id, "queued_count")
    Q([[INSERT INTO symphony_smart_recommendations (guild_id, requester_id, chosen_url, chosen_title, reason, accepted, created_at)
        VALUES (%s, %s, %s, %s, %s, true, now())]], interaction.guild_id, requester_id, url, title, reason)
    local q_len = queue_count(interaction.guild_id)
    local active = playback[interaction.guild_id] and playback[interaction.guild_id].active
    local heading
    if not active then process_queue(interaction.guild_id, channel_id); heading = "Smart Recommendation Starting"
    else heading = "Smart Recommendation Queued" end
    followup(interaction, { title = heading, description = ("Added **%s** based on **%s**. Queue size: %d"):format(title, reason, q_len), color = COLOR.green })
  end)

cmd("taste", "Show the saved taste profile Auto-DJ has learned for you or another member.",
  { { type = OPT.USER, name = "member", description = "Member to inspect", required = false } },
  function(_, interaction)
    local target_id = iuid(interaction)
    if interaction.data.options and interaction.data.options[1] then target_id = interaction.data.options[1].value end
    local rows, totals = build_user_taste_summary(interaction.guild_id, target_id)
    if #rows == 0 then
      return respond_text(interaction, "No saved taste profile for " .. resolve_display_name(interaction.guild_id, target_id) .. " yet.", true)
    end
    local lines = {}
    for i, row in ipairs(rows) do
      lines[#lines + 1] = ("**%d.** %s *(score %.1f)*"):format(i, row.title or "Unknown Track", tonumber(row.score) or 0)
    end
    respond(interaction, {
      title = "SYMPHONY Taste Profile: " .. resolve_display_name(interaction.guild_id, target_id),
      description = table.concat(lines, "\n"), color = COLOR.blurple,
      fields = { { name = "Signals", value = ("plays %d | finishes %d | likes %d | dislikes %d | skips %d"):format(
        tonumber(totals.played) or 0, tonumber(totals.finished) or 0, tonumber(totals.liked) or 0,
        tonumber(totals.disliked) or 0, tonumber(totals.skipped) or 0), inline = false } },
    }, true)
  end)

cmd("like", "Teach Auto-DJ to play more tracks like the current one for you.", nil,
  function(_, interaction)
    local d = playback[interaction.guild_id]
    if not d then return respond_text(interaction, "Nothing is playing right now.", true) end
    record_track_feedback(interaction.guild_id, iuid(interaction), d.url, d.title, true)
    respond(interaction, { description = "Saved your like for **" .. (d.title or "this track") .. "**. Auto-DJ will lean toward similar picks.", color = COLOR.green }, true)
  end)

cmd("dislike", "Teach Auto-DJ to avoid the current track for your future recommendations.", nil,
  function(_, interaction)
    local d = playback[interaction.guild_id]
    if not d then return respond_text(interaction, "Nothing is playing right now.", true) end
    record_track_feedback(interaction.guild_id, iuid(interaction), d.url, d.title, false)
    respond(interaction, { description = "Saved your dislike for **" .. (d.title or "this track") .. "**. Auto-DJ will avoid it for you.", color = COLOR.orange }, true)
  end)

-- ---- Personal playlists & history -------------------------------------------

cmd("playlists", "List your saved personal playlists and how many tracks each one contains", nil,
  function(_, interaction)
    local uid = iuid(interaction)
    local rows = Q([[SELECT playlist_name, COUNT(*) AS c FROM symphony_user_playlists WHERE user_id = %s
        GROUP BY playlist_name ORDER BY playlist_name ASC]], uid) or {}
    if #rows == 0 then return respond_text(interaction, "You do not have any saved playlists yet.", true) end
    local lines = {}
    for i = 1, math.min(20, #rows) do
      lines[#lines + 1] = ("\xe2\x80\xa2 **%s** \xe2\x80\x94 %s track(s)"):format(rows[i].playlist_name, rows[i].c)
    end
    respond(interaction, { title = "\xf0\x9f\x8e\xbc Your Saved Playlists", description = table.concat(lines, "\n"), color = COLOR.blurple }, true)
  end)

cmd("deleteplaylist", "Delete one of your saved personal playlists by name",
  { { type = OPT.STRING, name = "name", description = "Playlist name", required = true } },
  function(_, interaction)
    local uid = iuid(interaction)
    local name = interaction.data.options[1].value
    local rows = Q("SELECT COUNT(*) AS c FROM symphony_user_playlists WHERE user_id = %s AND playlist_name = %s", uid, name)
    local count = rows and rows[1] and tonumber(rows[1].c) or 0
    if count == 0 then return respond_text(interaction, "That playlist was not found.", true) end
    Q("DELETE FROM symphony_user_playlists WHERE user_id = %s AND playlist_name = %s", uid, name)
    respond(interaction, { description = ("\xf0\x9f\x97\x91\xef\xb8\x8f Deleted **%s** (%d track(s))."):format(name, count), color = COLOR.green }, true)
  end)

cmd("savequeue", "Save the current queue to one of your personal playlists so you can load it again later.",
  { { type = OPT.STRING, name = "name", description = "Playlist name", required = true } },
  function(_, interaction)
    local uid = iuid(interaction)
    local name = interaction.data.options[1].value
    defer(interaction, true)
    local rows = Q([[SELECT video_url, title FROM symphony_queue WHERE guild_id = %s AND bot_name = 'symphony' ORDER BY id ASC]], interaction.guild_id) or {}
    if #rows == 0 then return followup_text(interaction, "Queue is empty!", true) end
    for _, row in ipairs(rows) do
      Q("INSERT INTO symphony_user_playlists (user_id, playlist_name, video_url, title) VALUES (%s, %s, %s, %s)", uid, name, row.video_url, row.title)
    end
    followup(interaction, { description = ("\xf0\x9f\x92\xbe Saved **%d** tracks to your personal playlist: **%s**"):format(#rows, name), color = COLOR.green }, true)
  end)

cmd("loadqueue", "Load one of your saved personal playlists into the active queue and start playback if needed.",
  { { type = OPT.STRING, name = "name", description = "Playlist name", required = true } },
  function(_, interaction)
    local uid = iuid(interaction)
    local name = interaction.data.options[1].value
    defer(interaction, true)
    local rows = Q("SELECT video_url, title FROM symphony_user_playlists WHERE user_id = %s AND playlist_name = %s", uid, name) or {}
    if #rows == 0 then return followup_text(interaction, "Playlist not found or empty.", true) end
    for _, row in ipairs(rows) do
      insert_queue_end(interaction.guild_id, row.video_url, row.title, uid)
      bump_track_intelligence(interaction.guild_id, row.video_url, row.title, uid, "queued_count")
    end
    snapshot_queue_backup(interaction.guild_id)
    followup(interaction, { description = ("\xf0\x9f\x93\x82 Loaded **%d** tracks from **%s** into the queue!"):format(#rows, name), color = COLOR.green }, true)
    if not (playback[interaction.guild_id] and playback[interaction.guild_id].active) then
      local channel_id = user_current_voice_channel(interaction)
      if channel_id then process_queue(interaction.guild_id, channel_id) end
    end
  end)

cmd("leaderboard", "Show the most played tracks from this server based on stored playback history.", nil,
  function(_, interaction)
    defer(interaction, true)
    local rows = Q([[SELECT title, COUNT(*) AS plays FROM symphony_history WHERE guild_id = %s
        GROUP BY title ORDER BY plays DESC LIMIT 10]], interaction.guild_id) or {}
    if #rows == 0 then return followup_text(interaction, "No play history yet.", true) end
    local lines = {}
    for i, row in ipairs(rows) do lines[#lines + 1] = ("**%d.** %s *(Played %s times)*"):format(i, row.title, row.plays) end
    followup(interaction, { title = "\xf0\x9f\x8f\x86 Server Top Tracks", description = table.concat(lines, "\n"), color = COLOR.gold }, true)
  end)

cmd("history", "Show the most recent tracks played in this server from playback history.", nil,
  function(_, interaction)
    defer(interaction, true)
    local rows = Q("SELECT title FROM symphony_history WHERE guild_id = %s ORDER BY played_at DESC LIMIT 5", interaction.guild_id) or {}
    if #rows == 0 then return followup_text(interaction, "No history.", true) end
    local lines = {}
    for _, row in ipairs(rows) do lines[#lines + 1] = "- " .. row.title end
    followup(interaction, { title = "\xf0\x9f\x93\x9c History", description = table.concat(lines, "\n"), color = COLOR.blurple }, true)
  end)

cmd("userhistory", "Show the most recent tracks requested by a specific user in this server.",
  { { type = OPT.USER, name = "member", description = "Member to inspect", required = true } },
  function(_, interaction)
    local member_id = interaction.data.options[1].value
    local rows = Q([[SELECT id, title, video_url FROM symphony_history WHERE guild_id = %s AND requester_id = %s
        ORDER BY played_at DESC LIMIT 10]], interaction.guild_id, member_id) or {}
    if #rows == 0 then
      return respond(interaction, { description = "\xf0\x9f\x93\xad " .. resolve_display_name(interaction.guild_id, member_id) .. " hasn't queued any songs yet.", color = COLOR.red }, true)
    end
    local lines = {}
    for i, row in ipairs(rows) do lines[#lines + 1] = ("**%d.** [%s](%s)"):format(i, row.title, row.video_url) end
    respond(interaction, {
      title = "\xf0\x9f\x8e\xa7 " .. resolve_display_name(interaction.guild_id, member_id) .. "'s Play History",
      description = table.concat(lines, "\n"), color = COLOR.blue,
      footer = ("Use /%ssteal <user> <number> to add one to the queue!"):format(CMD_PREFIX),
    }, true)
  end)

cmd("steal", "Copy a track from a member's request history and add it back into the queue.",
  { { type = OPT.USER, name = "member", description = "Member to steal from", required = true },
    { type = OPT.INTEGER, name = "track_number", description = "Track number from their history", required = true } },
  function(_, interaction)
    local member_id, track_number
    for _, o in ipairs(interaction.data.options) do
      if o.name == "member" then member_id = o.value end
      if o.name == "track_number" then track_number = o.value end
    end
    if track_number < 1 then return respond(interaction, { description = "\xe2\x9d\x8c Track number must be 1 or greater.", color = COLOR.red }, true) end
    local rows = Q([[SELECT video_url, title FROM symphony_history WHERE guild_id = %s AND requester_id = %s
        ORDER BY played_at DESC LIMIT 1 OFFSET %s]], interaction.guild_id, member_id, track_number - 1)
    if not rows or not rows[1] then return respond(interaction, { description = ("\xe2\x9d\x8c Could not find track #%d in their history."):format(track_number), color = COLOR.red }, true) end
    local row = rows[1]
    local uid = iuid(interaction)
    insert_queue_end(interaction.guild_id, row.video_url, row.title, uid)
    bump_track_intelligence(interaction.guild_id, row.video_url, row.title, uid, "queued_count")
    snapshot_queue_backup(interaction.guild_id)
    respond(interaction, { title = "\xf0\x9f\xa5\xb7 Song Stolen!", description = ("Added **%s** to the queue from %s's history."):format(row.title, resolve_display_name(interaction.guild_id, member_id)), color = COLOR.green })
    if not (playback[interaction.guild_id] and playback[interaction.guild_id].active) then
      local channel_id = user_current_voice_channel(interaction)
      if channel_id then process_queue(interaction.guild_id, channel_id) end
    end
  end)

cmd("grab", "Send yourself the currently playing track in a direct message for easy saving or sharing.", nil,
  function(_, interaction)
    defer(interaction, true)
    local d = playback[interaction.guild_id]
    if not d then return followup(interaction, { description = "Nothing is currently playing.", color = COLOR.red }, true) end
    local uid = iuid(interaction)
    local ok = dm_user(uid, {
      title = "\xf0\x9f\x8e\xb5 Track Saved!",
      description = ("Hey **%s**!\nHere is the track you wanted to save:\n\n**[%s](%s)**"):format(idisplay(interaction), d.title or "Unknown Title", d.url),
      color = 0x5865F2,
    })
    if ok then
      followup(interaction, { description = "\xf0\x9f\x93\xac Check your DMs!", color = COLOR.green }, true)
    else
      followup(interaction, { description = "\xe2\x9d\x8c I can't DM you! Please check your privacy settings.", color = COLOR.red }, true)
    end
  end)

-- ---- Utility -------------------------------------------------------------

cmd("settings", "Show the saved playback, DJ, queue, and recovery settings for this server", nil,
  function(_, interaction)
    defer(interaction, true)
    local s = get_settings(interaction.guild_id)
    local autodj_state = get_autodj(interaction.guild_id)
    followup(interaction, {
      title = "\xe2\x9a\x99\xef\xb8\x8f Server Music Settings", color = COLOR.blurple,
      fields = {
        { name = "Home Channel", value = s.home_vc_id and ("<#" .. s.home_vc_id .. ">") or "Not set", inline = true },
        { name = "Feedback Channel", value = s.feedback_channel_id and ("<#" .. s.feedback_channel_id .. ">") or "Not set", inline = true },
        { name = "DJ Role", value = s.dj_role_id and ("<@&" .. s.dj_role_id .. ">") or "Not set", inline = true },
        { name = "Volume", value = tostring(s.volume), inline = true },
        { name = "Loop Mode", value = tostring(s.loop_mode), inline = true },
        { name = "Filter", value = tostring(s.filter_mode), inline = true },
        { name = "Transitions", value = tostring(s.transition_mode), inline = true },
        { name = "Custom Speed/Pitch", value = ("%sx / %sx (%s left)"):format(s.custom_speed, s.custom_pitch, s.custom_modifiers_left), inline = true },
        { name = "Strict DJ", value = s.dj_only_mode and "Enabled" or "Disabled", inline = true },
        { name = "24/7 Mode", value = s.stay_in_vc and "Enabled" or "Disabled", inline = true },
        { name = "Auto-DJ", value = autodj_state and "Enabled" or "Disabled", inline = true },
      },
    }, true)
  end)

local function build_panel_embed(guild_id)
  local s = get_settings(guild_id)
  local d = playback[guild_id]
  local fields = { { name = "Volume", value = s.volume .. "%", inline = true }, { name = "Loop", value = s.loop_mode, inline = true } }
  local description = nil
  if d then
    local bar = make_progress_bar(current_position_seconds(guild_id), math.floor((d.duration_ms or 0) / 1000))
    local now_playing = d.url and ("**[%s](%s)**"):format(d.title or "Playing", d.url) or ("**" .. (d.title or "Playing") .. "**")
    table.insert(fields, 1, { name = "Now Playing", value = now_playing .. "\n`" .. bar .. "`", inline = false })
    if d.requester_id then
      table.insert(fields, 2, { name = "Requested by", value = resolve_display_name(guild_id, d.requester_id), inline = true })
    end
    table.insert(fields, { name = "Filter", value = tostring(d.current_filter or "none"), inline = true })
  else
    description = "Nothing is playing right now."
  end
  return build_embed({ title = "\xf0\x9f\x8e\x9b\xef\xb8\x8f SYMPHONY Music Control Panel", description = description, color = COLOR.panel, fields = fields, footer = "SYMPHONY Main Music System" })
end

local PANEL_COMPONENTS = {
  { type = 1, components = {
    { type = 2, style = 1, custom_id = "symphony_panel_playpause", label = "\xe2\x8f\xaf\xef\xb8\x8f Play/Pause" },
    { type = 2, style = 4, custom_id = "symphony_panel_stop", label = "\xe2\x8f\xb9\xef\xb8\x8f Stop" },
    { type = 2, style = 2, custom_id = "symphony_panel_skip", label = "\xe2\x8f\xad\xef\xb8\x8f Skip" },
  } },
  { type = 1, components = {
    { type = 2, style = 2, custom_id = "symphony_panel_rw", label = "\xe2\x8f\xaa -10s" },
    { type = 2, style = 2, custom_id = "symphony_panel_fw", label = "\xe2\x8f\xa9 +10s" },
    { type = 2, style = 2, custom_id = "symphony_panel_voldown", label = "\xf0\x9f\x94\x89 Vol-" },
    { type = 2, style = 2, custom_id = "symphony_panel_volup", label = "\xf0\x9f\x94\x8a Vol+" },
  } },
  { type = 1, components = {
    { type = 2, style = 2, custom_id = "symphony_panel_loop", label = "\xf0\x9f\x94\x81 Loop" },
    { type = 2, style = 3, custom_id = "symphony_panel_shuffle", label = "\xf0\x9f\x94\x80 Shuffle" },
    { type = 2, style = 2, custom_id = "symphony_panel_queue", label = "\xf0\x9f\x93\x9c View Queue" },
  } },
}

cmd("panel", "Post an interactive control panel with playback, queue, and transport buttons.", nil,
  function(_, interaction)
    callback(interaction, 4, { embeds = { build_panel_embed(interaction.guild_id) }, components = PANEL_COMPONENTS })
  end)

cmd("lyrics", "Show lyrics for the currently playing track, synced to the current position when available.", nil,
  function(_, interaction)
    local d = playback[interaction.guild_id]
    if not d then return respond(interaction, { description = "Nothing is playing.", color = COLOR.red }, true) end
    defer(interaction, false)
    local title = d.title or "Unknown"
    local result = fetch_lyrics(title, math.floor((d.duration_ms or 0) / 1000))
    if not result then return followup(interaction, { description = ("No lyrics found for **%s**."):format(title), color = COLOR.red }) end
    if result.synced then
      local pos = current_position_seconds(interaction.guild_id)
      local current_idx = 1
      for idx, line in ipairs(result.synced) do
        if line[1] <= pos then current_idx = idx else break end
      end
      local body = {}
      for idx = math.max(1, current_idx - 2), math.min(#result.synced, current_idx + 4) do
        local text = result.synced[idx][2]
        body[#body + 1] = (idx == current_idx) and ("**" .. text .. "**") or text
      end
      followup(interaction, { title = "\xf0\x9f\x93\x9c " .. (result.track_name or title), description = trunc(table.concat(body, "\n"), 4000), color = COLOR.blue, footer = "Synced to current playback position \xe2\x80\x94 rerun to refresh." })
    else
      followup(interaction, { title = "\xf0\x9f\x93\x9c " .. (result.track_name or title), description = trunc(result.plain or "No lyrics text available.", 4000), color = COLOR.blue })
    end
  end)

cmd("ping", "Show the bot websocket latency so you can quickly check responsiveness.", nil,
  function(_, interaction)
    respond(interaction, { description = "\xf0\x9f\x8f\x93 Pong!", color = COLOR.green }, true)
  end)

cmd("uptime", "Show how long this bot process has been running since its last startup.", nil,
  function(_, interaction)
    local secs = os.time() - START_TIME
    local h = math.floor(secs / 3600)
    local m = math.floor((secs % 3600) / 60)
    local s = secs % 60
    respond(interaction, { description = ("\xe2\x8f\xb1\xef\xb8\x8f Uptime: %d:%02d:%02d"):format(h, m, s), color = COLOR.green }, true)
  end)

cmd("stats", "Show quick bot statistics such as guild count and active player count.", nil,
  function(_, interaction)
    local active = 0
    for _ in pairs(playback) do active = active + 1 end
    respond(interaction, { description = ("\xf0\x9f\x93\x8a Active Players: %d"):format(active), color = COLOR.green }, true)
  end)

cmd("help", "Show a categorized help menu for all SYMPHONY music commands and utilities", nil,
  function(_, interaction)
    local names = {}
    for name in pairs(bot.commands) do names[#names + 1] = name end
    table.sort(names)
    respond(interaction, { title = "\xf0\x9f\x93\x9a Command List", description = table.concat(names, ", "), color = COLOR.blue }, true)
  end)

-- ============================================================================
-- AUTOCOMPLETE + PANEL BUTTON HANDLING (registered alongside Bot:run()'s own
-- INTERACTION_CREATE handler -- gateway.lua supports multiple handlers per event)
-- ============================================================================

local function focused_option(options)
  for _, o in ipairs(options or {}) do
    if o.focused then return o end
    if o.options then
      local nested = focused_option(o.options)
      if nested then return nested end
    end
  end
  return nil
end

bot.gateway:on("INTERACTION_CREATE", function(interaction)
  if interaction.type == 4 then -- APPLICATION_COMMAND_AUTOCOMPLETE
    local focused = focused_option(interaction.data.options)
    if not focused then return respond_autocomplete(interaction, {}) end
    if focused.name == "search" then
      local current = focused.value or ""
      if #current < 2 then return respond_autocomplete(interaction, {}) end
      local response = {}
      local ok = https.request({
        url = "https://suggestqueries.google.com/complete/search?client=firefox&ds=yt&q=" .. urlencode(current),
        method = "GET",
        sink = ltn12.sink.table(response),
      })
      local choices = {}
      if ok then
        local okd, data = pcall(cjson.decode, table.concat(response))
        if okd and type(data) == "table" and data[2] then
          for _, s in ipairs(data[2]) do
            if #choices >= 10 then break end
            choices[#choices + 1] = { name = trunc(s, 100), value = trunc(s, 100) }
          end
        end
      end
      if #choices == 0 and current ~= "" then choices = { { name = trunc(current, 100), value = trunc(current, 100) } } end
      respond_autocomplete(interaction, choices)
    elseif focused.name == "index" or focused.name == "frm" or focused.name == "to" then
      respond_autocomplete(interaction, queue_position_choices(interaction.guild_id, focused.value))
    else
      respond_autocomplete(interaction, {})
    end
    return
  end

  if interaction.type == 3 then -- MESSAGE_COMPONENT (panel buttons)
    local id = interaction.data.custom_id
    local guild_id = interaction.guild_id
    if not id or not id:match("^symphony_panel_") then return end

    if id == "symphony_panel_playpause" then
      if not is_dj(interaction) then return end
      local d = playback[guild_id]
      if d and d.active then
        local new_paused = not d.paused
        bot.lavalink:set_paused(guild_id, new_paused)
        sync_pause_state(guild_id, new_paused)
      end
      update_message(interaction, build_panel_embed(guild_id), PANEL_COMPONENTS)
    elseif id == "symphony_panel_stop" then
      if not is_dj(interaction) then return end
      stop_playback(guild_id)
      update_message(interaction, build_panel_embed(guild_id), PANEL_COMPONENTS)
    elseif id == "symphony_panel_skip" then
      if not is_dj(interaction) then return end
      if playback[guild_id] and bot.lavalink then bot.lavalink:stop(guild_id) end
      respond_text(interaction, "\xe2\x8f\xad\xef\xb8\x8f Skipped to next track", true)
    elseif id == "symphony_panel_rw" or id == "symphony_panel_fw" then
      if not is_dj(interaction) then return end
      local d = playback[guild_id]
      if not d then return respond_text(interaction, "Nothing playing.", true) end
      local delta = (id == "symphony_panel_fw") and 10 or -10
      local new_pos = math.max(0, current_position_seconds(guild_id) + delta)
      bot.lavalink:seek(guild_id, new_pos * 1000)
      d.position_ms = new_pos * 1000
      d.position_at = os.time()
      update_message(interaction, build_panel_embed(guild_id), PANEL_COMPONENTS)
    elseif id == "symphony_panel_voldown" or id == "symphony_panel_volup" then
      if not is_dj(interaction) then return end
      local s = get_settings(guild_id)
      local delta = (id == "symphony_panel_volup") and 10 or -10
      local vol = math.max(1, math.min(200, s.volume + delta))
      Q("UPDATE symphony_guild_settings SET volume = %s WHERE guild_id = %s", vol, guild_id)
      if playback[guild_id] and bot.lavalink then bot.lavalink:set_volume(guild_id, vol) end
      update_message(interaction, build_panel_embed(guild_id), PANEL_COMPONENTS)
    elseif id == "symphony_panel_loop" then
      if not is_dj(interaction) then return end
      local s = get_settings(guild_id)
      local next_mode = ({ off = "queue", queue = "song", song = "off" })[s.loop_mode] or "queue"
      Q("UPDATE symphony_guild_settings SET loop_mode = %s WHERE guild_id = %s", next_mode, guild_id)
      update_message(interaction, build_panel_embed(guild_id), PANEL_COMPONENTS)
    elseif id == "symphony_panel_shuffle" then
      if not is_dj(interaction) then return end
      local n = shuffle_queue_rows(guild_id, false)
      if n > 0 then snapshot_queue_backup(guild_id) end
      respond_text(interaction, n > 0 and "\xf0\x9f\x94\x80 Queue successfully shuffled!" or "Queue empty.", true)
    elseif id == "symphony_panel_queue" then
      defer(interaction, true)
      local rows = Q([[SELECT title FROM symphony_queue WHERE guild_id = %s AND bot_name = 'symphony' ORDER BY id ASC LIMIT 10]], guild_id) or {}
      if #rows == 0 then return followup_text(interaction, "Queue is empty.", true) end
      local lines = {}
      for i, row in ipairs(rows) do lines[#lines + 1] = i .. ". " .. row.title end
      followup_text(interaction, "**Current Queue:**\n" .. table.concat(lines, "\n"), true)
    end
  end
end)

-- ============================================================================
-- SWARM HEARTBEAT (liveness signal for Aria / SwarmPanel)
-- ============================================================================

local function heartbeat_tick()
  Q([[INSERT INTO swarm_health (bot_name, last_pulse, status) VALUES ('symphony', now(), 'online')
      ON CONFLICT (bot_name) DO UPDATE SET last_pulse = EXCLUDED.last_pulse, status = EXCLUDED.status]])
  for guild_id, d in pairs(playback) do
    Q([[INSERT INTO symphony_metrics (guild_id, bot_name, voice_connected, player_connected, player_playing,
          player_paused, queue_count, is_playing_db, is_paused_db, position_seconds, duration_seconds, updated_at)
        VALUES (%s, 'symphony', true, true, %s, %s, %s, %s, %s, %s, %s, now())
        ON CONFLICT (guild_id, bot_name) DO UPDATE SET
          voice_connected = EXCLUDED.voice_connected, player_connected = EXCLUDED.player_connected,
          player_playing = EXCLUDED.player_playing, player_paused = EXCLUDED.player_paused,
          queue_count = EXCLUDED.queue_count, is_playing_db = EXCLUDED.is_playing_db,
          is_paused_db = EXCLUDED.is_paused_db, position_seconds = EXCLUDED.position_seconds,
          duration_seconds = EXCLUDED.duration_seconds, updated_at = now()]],
      guild_id, not d.paused, d.paused or false, queue_count(guild_id), not d.paused, d.paused or false,
      current_position_seconds(guild_id), math.floor((d.duration_ms or 0) / 1000))
  end
end

copas.addthread(function()
  while true do
    local ok = pcall(heartbeat_tick)
    if not ok then
      io.stderr:write("[symphony] heartbeat cycle failed\n")
      report_error(nil, "runtime", "heartbeat cycle failed", "swarm_health/symphony_metrics upsert failed; see bot logs for the pcall traceback.")
    end
    copas.sleep(20)
  end
end)

-- ============================================================================
-- ERROR-EVENT RETENTION JANITOR
-- Mirrors symphony.py's "Database Janitor" retention pass for error_events
-- (30-day retention); the history/smart_recommendations/track_intelligence/
-- user_track_affinity pruning that same job did in Python is not ported --
-- those tables are small per-guild and the fleet plans a shared cross-bot
-- retention job at the Postgres level instead of per-bot copies of the same
-- DELETE statements (see final report).
-- ============================================================================
copas.addthread(function()
  while true do
    copas.sleep(6 * 3600)
    local ok, err = pcall(Q, "DELETE FROM symphony_error_events WHERE created_at < NOW() - INTERVAL '30 days'")
    if not ok then io.stderr:write("[symphony] error_events cleanup failed: " .. tostring(err) .. "\n") end
  end
end)

-- ============================================================================
-- ENTRY POINT
-- ============================================================================

print("[symphony] starting up (command namespace: /" .. CMD_PREFIX .. "*)")

if env("SYMPHONY_DRY_RUN") == "test" then
  -- Exposes internals for the DB/logic smoke test (db_smoke_test.lua); not
  -- reached in normal operation (that script sets this env var itself).
  -- Mirrors the same NEXUS_DRY_RUN="test" / SAPPHIRE_DRY_RUN="test" pattern
  -- so the gateway/lavalink websocket connections (bot:run()) are never
  -- opened during a pure-DB smoke test.
  return {
    Q = Q, url_key = url_key, new_track_uid = new_track_uid,
    ensure_guild_settings = ensure_guild_settings, get_settings = get_settings,
    get_autodj = get_autodj, set_autodj = set_autodj,
    get_home_channel_id = get_home_channel_id, set_home_channel = set_home_channel,
    queue_count = queue_count, pop_next_queue_row = pop_next_queue_row,
    insert_queue_end = insert_queue_end, insert_queue_front = insert_queue_front,
    snapshot_queue_backup = snapshot_queue_backup, restore_queue_from_backup = restore_queue_from_backup,
    shuffle_queue_rows = shuffle_queue_rows,
    bump_track_intelligence = bump_track_intelligence, bump_user_affinity = bump_user_affinity,
    record_track_feedback = record_track_feedback, record_history = record_history,
    persist_playback_state = persist_playback_state, clear_playback_state = clear_playback_state,
    persist_voice_state = persist_voice_state,
    pick_smart_recommendation = pick_smart_recommendation, build_user_taste_summary = build_user_taste_summary,
    persist_error_event = persist_error_event, report_error = report_error,
    heartbeat_tick = heartbeat_tick,
    playback = playback, bot = bot,
  }
elseif env("SYMPHONY_DRY_RUN") then
  print(("[symphony] dry run OK: %d commands registered, db/lavalink/gateway objects constructed."):format((function()
    local n = 0; for _ in pairs(bot.commands) do n = n + 1 end; return n
  end)()))
  os.exit(0)
end

-- Rejoin and resume for any guild that was playing (or had a non-empty
-- queue) when this process last stopped -- container recreates/restarts
-- otherwise leave the bot sitting disconnected with a full queue and no
-- way back in short of a manual /play.
-- BUGFIX: the Discord gateway sends a fresh READY (not RESUMED) on every
-- invalidated session -- routine reconnects, not just process restarts --
-- and this handler used to redo its full boot-resume/background-loop-spawn
-- pass every single time, which both force-restarted/discarded queued
-- tracks on reconnect and piled up duplicate background loops. Gate on
-- did_initial_ready so it runs exactly once per process (matching what it was
-- actually written for).
local did_initial_ready = false

-- SwarmPanel's control.lua also writes uppercase PLAY/RECOVER/LEAVE/SEEK
-- rows to symphony_swarm_direct_orders for source_url plays, fleet-
-- supervisor recovery, forced disconnects, and seek-to-position -- none of
-- the simpler symphony_swarm_overrides poller above covers these.
local function handle_direct_order(order)
  local guild_id = tostring(order.guild_id)
  local cmd = order.command
  local ok, err = pcall(function()
    if cmd == "PLAY" and order.data and order.vc_id and order.vc_id ~= "0" then
      local entries, playlist_info = search_playables(order.data)
      if not entries or #entries == 0 then error("could not resolve source: " .. tostring(playlist_info), 0) end
      for _, track in ipairs(entries) do
        local uri = track.info and track.info.uri or track.uri
        local title = track.info and track.info.title or track.title or "Unknown Track"
        insert_queue_end(guild_id, uri, title, nil)
      end
      if #entries > 1 then shuffle_queue_rows(guild_id, true) end
      -- Mirrors /play's own playlist-tracking condition -- without this, a
      -- playlist queued via SwarmPanel (rather than /play) never registered
      -- for auto-add/auto-remove sync at all.
      if playlist_info and is_playlist_url(order.data) then
        local norm = {}
        for _, track in ipairs(entries) do
          local uri = track.info and track.info.uri or track.uri
          local title = track.info and track.info.title or track.title
          if uri then norm[#norm + 1] = { uri = uri, title = title } end
        end
        set_active_playlist(guild_id, order.data, norm, nil, order.vc_id)
      end
      snapshot_queue_backup(guild_id)
      local d = playback[guild_id]
      if not (d and d.active) then process_queue(guild_id, order.vc_id) end
    elseif cmd == "RECOVER" then
      local channel_id = (order.vc_id and order.vc_id ~= "0" and order.vc_id) or get_home_channel_id(guild_id)
      local d = playback[guild_id]
      if channel_id and not (d and d.active) then process_queue(guild_id, channel_id) end
    elseif cmd == "LEAVE" then
      stop_playback(guild_id)
    elseif cmd == "SEEK" and order.data then
      local target_seconds = math.max(0, tonumber(order.data) or 0)
      local d = playback[guild_id]
      if d and d.active then
        bot.lavalink:seek(guild_id, target_seconds * 1000)
      end
    end
  end)
  if not ok then
    Q("UPDATE symphony_swarm_direct_orders SET attempts = attempts + 1, last_error = %s WHERE id = %s", tostring(err):sub(1, 500), order.id)
  else
    Q("DELETE FROM symphony_swarm_direct_orders WHERE id = %s", order.id)
    print(("[symphony] Aria executed direct order %s in guild %s."):format(cmd, guild_id))
  end
end

local function recovery_watchdog()
  local stalled = Q("SELECT DISTINCT guild_id FROM symphony_queue WHERE bot_name = 'symphony'") or {}
  for _, row in ipairs(stalled) do
    local guild_id = tostring(row.guild_id)
    if not (playback[guild_id] and playback[guild_id].active) then
      local vrows = Q("SELECT connected_channel_id FROM symphony_voice_state WHERE guild_id = %s AND bot_name = 'symphony'", guild_id)
      local channel_id = (vrows and vrows[1] and vrows[1].connected_channel_id) or get_home_channel_id(guild_id)
      if channel_id then process_queue(guild_id, channel_id) end
    end
  end
end

local function poll_direct_orders()
  local rows = Q("SELECT id, guild_id, vc_id, text_channel_id, command, data FROM symphony_swarm_direct_orders WHERE bot_name = 'symphony' AND claimed_at IS NULL ORDER BY id ASC LIMIT 5") or {}
  for _, order in ipairs(rows) do
    Q("UPDATE symphony_swarm_direct_orders SET claimed_at = NOW() WHERE id = %s", order.id)
    handle_direct_order(order)
  end
end

bot.gateway:on("READY", function()
  if did_initial_ready then return end
  did_initial_ready = true
  copas.addthread(function()
    -- Wait for the Lavalink websocket to actually be connected before
    -- touching the queue -- process_queue deletes each row before resolving
    -- it, so racing this against Lavalink's own (often slow, cold-start)
    -- connect meant every "no response from Lavalink" failure permanently
    -- destroyed that queued track instead of just skipping playback.
    local waited = 0
    while not (bot.lavalink and bot.lavalink.session_id) and waited < 60 do
      copas.sleep(1)
      waited = waited + 1
    end
    local rows = Q("SELECT DISTINCT guild_id FROM symphony_bot_home_channels WHERE bot_name = 'symphony'") or {}
    for _, row in ipairs(rows) do
      local gid = row.guild_id
      local channel_id = get_home_channel_id(gid)
      if channel_id then
        local has_queue = queue_count(gid) > 0
        local wp_rows = Q("SELECT is_playing FROM symphony_playback_state WHERE guild_id = %s AND bot_name = 'symphony'", gid)
        local was_playing = wp_rows and wp_rows[1]
        if has_queue or (was_playing and was_playing.is_playing) then
          print(("[symphony] [%s] resuming on boot (queue=%s, was_playing=%s)"):format(tostring(gid), tostring(has_queue), tostring(was_playing and was_playing.is_playing)))
          process_queue(gid, channel_id)
        end
      end
    end
  end)

  -- SwarmPanel/Aria remote-control bridge -- was entirely missing, so the
  -- panel's PAUSE/RESUME/SKIP/STOP/RESTART buttons silently did nothing for
  -- this bot. Same poll-and-execute-then-delete pattern as alucard.lua/
  -- gws.lua/sapphire.lua, adapted to symphony's Q()/sync_pause_state().
  copas.addthread(function()
    while true do
      copas.sleep(10)
      local ok, err = pcall(function()
        local rows = Q("SELECT guild_id, command FROM symphony_swarm_overrides WHERE bot_name = 'symphony'") or {}
        for _, row in ipairs(rows) do
          local guild_id = tostring(row.guild_id)
          local cmd_name = (row.command or ""):upper()
          local d = playback[guild_id]
          local executed = false
          if cmd_name == "RESTART" then
            Q("DELETE FROM symphony_swarm_overrides WHERE guild_id = %s AND bot_name = 'symphony'", guild_id)
            print("[symphony] Aria requested a restart.")
            os.exit(0)
          elseif cmd_name == "PAUSE" and d and d.active and not d.paused then
            bot.lavalink:set_paused(guild_id, true)
            sync_pause_state(guild_id, true)
            executed = true
          elseif cmd_name == "RESUME" and d and d.active and d.paused then
            bot.lavalink:set_paused(guild_id, false)
            sync_pause_state(guild_id, false)
            executed = true
          elseif cmd_name == "SKIP" and d and d.active then
            bot.lavalink:stop(guild_id)
            executed = true
          elseif cmd_name == "STOP" then
            stop_playback(guild_id)
            executed = true
          end
          if executed then
            print(("[symphony] Aria executed %s in guild %s."):format(cmd_name, guild_id))
          end
          Q("DELETE FROM symphony_swarm_overrides WHERE guild_id = %s AND bot_name = 'symphony' AND command = %s", guild_id, row.command)
        end
      end)
      if not ok then
        print("[symphony] swarm override poll error: " .. tostring(err))
        report_error(nil, "runtime", "swarm override poll error", tostring(err))
      end
    end
  end)

  copas.addthread(function()
    while true do
      copas.sleep(5)
      local ok, err = pcall(poll_direct_orders)
      if not ok then
        print("[symphony] direct order poll error: " .. tostring(err))
        report_error(nil, "runtime", "direct order poll error", tostring(err))
      end
    end
  end)
  copas.addthread(function()
    while true do
      copas.sleep(30)
      local ok, err = pcall(recovery_watchdog)
      if not ok then
        print("[symphony] recovery_watchdog error: " .. tostring(err))
        report_error(nil, "runtime", "recovery_watchdog error", tostring(err))
      end
    end
  end)

  copas.addthread(function()
    while true do
      copas.sleep(PLAYLIST_SYNC_INTERVAL)
      local ok, err = pcall(playlist_sync_tick)
      if not ok then
        print("[symphony] playlist sync tick error: " .. tostring(err))
        report_error(nil, "runtime", "playlist sync tick error", tostring(err))
      end
    end
  end)
end)

bot:run()

--[[
  PORT NOTES (simplifications vs. symphony.py -- full command-surface parity
  is otherwise kept; see README.md's command list, all 59 symphony_main_*
  slash commands are registered):
  - yt-dlp/cookies playlist expansion, local audio disk cache, ffmpeg loudness
    normalization/BPM analysis, and the Gemini-embedding similarity engine are
    not ported; Lavalink's native search/playlist loading covers /play,
    /playnext, /radio, and Auto-DJ instead. The Smart Auto-DJ learning layer
    (bump_track_intelligence/bump_user_affinity/pick_smart_recommendation) is
    consequently a simplified score/count heuristic over
    symphony_track_intelligence/symphony_user_track_affinity rather than
    symphony.py's weighted decay + embedding-similarity model.
    symphony_track_cooccurrence and symphony_track_embeddings are
    consequently never written or read by this port (schema tables stay
    empty; not a bug, just unused -- unlike sapphire.lua, this port's
    recommendation surface doesn't use a cooccurrence join).
  - Redis cross-process locking is not used (single LuaJIT process per bot;
    copas coroutines are cooperative so the in-process command handlers
    already serialize per-guild queue mutations well enough for this scale;
    see with_guild_lock).
  - The Aria direct-order swarm bridge (symphony_swarm_direct_orders) is
    ported (see poll_direct_orders/handle_direct_order); symphony_swarm_overrides
    is the separate simpler claim-token job queue also handled above.
  - The deep resilience state machine (persistent-voice-restore-on-startup
    background loop, watchdog stall/revival counters, voice reconnect backoff
    tuning, periodic scheduled restarts, zombie reaper, continuous
    background playlist-resync loop) is replaced with: reconnect-on-drop via
    bot.lua's own gateway/session resume and Lavalink event-driven queue
    advance. symphony_voice_state is still written (see persist_voice_state,
    called from join_and_wait_for_voice) so Aria/SwarmPanel's cross-bot voice
    dashboard has real data, it just isn't read back to auto-rejoin on
    process restart.
  - The Database Janitor's history/smart_recommendations/track_intelligence/
    user_track_affinity retention pruning is not ported; only the
    symphony_error_events 30-day retention pass is (see the janitor thread
    above) -- those other tables are small per-guild and the fleet plans a
    shared cross-bot retention job at the Postgres level instead of 13 copies
    of the same DELETE statements.
  - Operational Discord webhook logging: SYMPHONY_ERROR_WEBHOOK_URL /
    SWARM_ERROR_WEBHOOK_URL error reporting IS now ported
    (persist_error_event/send_error_webhook/report_error, wired via
    bot.on_error -- see lib/swarmlua/bot.lua's additive hook -- and the
    heartbeat loop above). This was previously not ported at all despite
    ERROR_WEBHOOK_URL being read from env; found and fixed during the final
    validation pass, same pattern gws/harmonic/nexus/sapphire used.
    SYMPHONY_WEBHOOK_URL (non-error operational announcements: sleep-timer
    elapsed, playlist-sync summaries, Aria override notices) is NOT ported --
    those all belong to features already dropped above (Aria bridge,
    continuous playlist resync) except the /symphony_main_sleep timer, which
    stops playback and replies in-channel already, just without the extra
    webhook echo.
  - Also fixed during the final validation pass (live-schema/live-DB
    verified via db_smoke_test.lua, not just read-through): the vendored
    lib/swarmlua/pg.lua was missing both the `table.unpack or unpack` LuaJIT
    shim AND nil-parameter -> SQL NULL escaping (a nil query parameter would
    previously either crash pg.lua's string.format or serialize as the Lua
    string "nil"); lib/swarmlua/bot.lua was missing the additive self.on_error
    hook entirely; symphony_voice_state had no INSERT path at all (only a
    dead UPDATE in stop_playback that matched zero rows, since nothing ever
    created the row) despite reconnect_attempts being NOT NULL with no
    column default; and symphony_error_events was never written to at all.
    symphony_track_intelligence / symphony_user_track_affinity's NOT-NULL-
    no-default counter columns were already correctly zero-filled on INSERT
    in this port (bump_track_intelligence/bump_user_affinity) -- verified,
    not a bug found here, unlike the equivalent gotcha in some sibling bots.
    No direct `table.unpack` calls were found in this file itself (some
    sibling bots had that bug independently of pg.lua's).
]]
