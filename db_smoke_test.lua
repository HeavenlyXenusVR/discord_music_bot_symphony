-- Live-database smoke test for symphony.lua. Mirrors the same
-- SYMPHONY_DRY_RUN="test" export pattern nexus.lua/sapphire.lua use so
-- bot:run() (gateway/lavalink websockets) is never reached -- this only
-- exercises the DB write paths against the real Postgres database.
--
-- Run with:
--   SYMPHONY_DISCORD_TOKEN=x SYMPHONY_LAVALINK_PASSWORD=x \
--   SYMPHONY_DB_HOST=127.0.0.1 SYMPHONY_DB_PORT=5432 \
--   SYMPHONY_DB_USER=botuser SYMPHONY_DB_PASSWORD=bot_logins \
--   SYMPHONY_DB_NAME=discord_music_symphony SYMPHONY_DRY_RUN=test \
--   luajit db_smoke_test.lua

package.path = "./lib/?.lua;./lib/?/init.lua;" .. package.path

local S = dofile("symphony.lua")

local TEST_GUILD = "999900001"
local TEST_USER = "999900002"
local TEST_USER2 = "999900003"

local pass, fail = 0, 0
local function check(label, ok, err)
  if ok then
    pass = pass + 1
    print("OK   " .. label)
  else
    fail = fail + 1
    print("FAIL " .. label .. " -> " .. tostring(err))
  end
end

-- guild settings upsert/read
S.ensure_guild_settings(TEST_GUILD)
local settings = S.get_settings(TEST_GUILD)
check("get_settings returns row", settings ~= nil)
S.Q("UPDATE symphony_guild_settings SET volume = %s, loop_mode = %s WHERE guild_id = %s", 77, "song", TEST_GUILD)
settings = S.get_settings(TEST_GUILD)
check("volume persisted", tonumber(settings.volume) == 77, tostring(settings.volume))
check("loop_mode persisted", settings.loop_mode == "song")

-- queue lifecycle
local uid1 = S.insert_queue_end(TEST_GUILD, "https://example.com/a", "Track A", TEST_USER)
local uid2 = S.insert_queue_end(TEST_GUILD, "https://example.com/b", "Track B", TEST_USER)
S.insert_queue_front(TEST_GUILD, "https://example.com/z", "Track Z (front)", TEST_USER)
check("queue_count after 3 inserts", S.queue_count(TEST_GUILD) == 3, S.queue_count(TEST_GUILD))
local rows = S.Q("SELECT video_url, title FROM symphony_queue WHERE guild_id = %s AND bot_name = 'symphony' ORDER BY id ASC", TEST_GUILD)
check("insert_queue_front puts Z first", rows and rows[1] and rows[1].title == "Track Z (front)")

S.snapshot_queue_backup(TEST_GUILD)
local backup_row = S.Q("SELECT COUNT(*) AS n FROM symphony_queue_backup WHERE guild_id = %s AND bot_name = 'symphony'", TEST_GUILD)
backup_row = backup_row and backup_row[1]
check("backup_count matches after snapshot", backup_row and tonumber(backup_row.n) == 3, backup_row and backup_row.n)

local n = S.shuffle_queue_rows(TEST_GUILD, true)
check("shuffle_queue_rows returns count", n == 3, n)

-- clear queue, then restore from backup
S.Q("DELETE FROM symphony_queue WHERE guild_id = %s AND bot_name = 'symphony'", TEST_GUILD)
check("queue emptied", S.queue_count(TEST_GUILD) == 0)
local restored = S.restore_queue_from_backup(TEST_GUILD)
check("restore_queue_from_backup restores 3", restored == 3 and S.queue_count(TEST_GUILD) == 3, restored)

-- track intelligence / affinity (the NOT-NULL counter-column gotcha -- these
-- calls each throw a Postgres not-null-violation on the FIRST insert for a
-- new row if any counter column isn't explicitly zero-filled)
local ok1, err1 = pcall(S.bump_track_intelligence, TEST_GUILD, "https://example.com/a", "Track A", TEST_USER, "queued_count")
check("bump_track_intelligence queued", ok1, err1)
local ok2, err2 = pcall(S.bump_track_intelligence, TEST_GUILD, "https://example.com/a", "Track A", TEST_USER, "play_count")
check("bump_track_intelligence play", ok2, err2)
local ok3, err3 = pcall(S.bump_track_intelligence, TEST_GUILD, "https://example.com/a", "Track A", TEST_USER, "finish_count")
check("bump_track_intelligence finish", ok3, err3)
local ok4, err4 = pcall(S.bump_track_intelligence, TEST_GUILD, "https://example.com/b", "Track B", TEST_USER, "skip_count")
check("bump_track_intelligence skip (new row)", ok4, err4)
local ok5, err5 = pcall(S.record_track_feedback, TEST_GUILD, TEST_USER, "https://example.com/a", "Track A", true)
check("record_track_feedback like", ok5, err5)
local ok6, err6 = pcall(S.record_track_feedback, TEST_GUILD, TEST_USER2, "https://example.com/b", "Track B", false)
check("record_track_feedback dislike (new affinity row)", ok6, err6)

local key_a = S.url_key("https://example.com/a", "Track A")
local ti = S.Q("SELECT queued_count, play_count, finish_count, like_count, dislike_count, skip_count, total_listen_seconds, first_seen, last_queued, last_played FROM symphony_track_intelligence WHERE guild_id = %s AND url_key = %s", TEST_GUILD, key_a)
ti = ti and ti[1]
check("track_intelligence row has all counters non-null",
  ti and ti.queued_count ~= nil and ti.play_count ~= nil and ti.finish_count ~= nil
    and ti.like_count ~= nil and ti.dislike_count ~= nil and ti.skip_count ~= nil and ti.total_listen_seconds ~= nil,
  tostring(ti))
check("track_intelligence play_count == 1 after one play", ti and tonumber(ti.play_count) == 1, ti and tostring(ti.play_count))
check("track_intelligence finish_count == 1 after one finish", ti and tonumber(ti.finish_count) == 1, ti and tostring(ti.finish_count))
check("track_intelligence like_count == 1 after one like", ti and tonumber(ti.like_count) == 1, ti and tostring(ti.like_count))
check("track_intelligence first_seen/last_queued are populated", ti and ti.first_seen ~= nil and ti.last_queued ~= nil, tostring(ti))

local aff = S.Q("SELECT queued_count, play_count, finish_count, skip_count, like_count, dislike_count, score FROM symphony_user_track_affinity WHERE guild_id = %s AND user_id = %s AND url_key = %s", TEST_GUILD, TEST_USER, key_a)
aff = aff and aff[1]
check("user_track_affinity row has all counters non-null",
  aff and aff.queued_count ~= nil and aff.play_count ~= nil and aff.finish_count ~= nil
    and aff.skip_count ~= nil and aff.like_count ~= nil and aff.dislike_count ~= nil and aff.score ~= nil, tostring(aff))

local key_b = S.url_key("https://example.com/b", "Track B")
local aff_b = S.Q("SELECT queued_count, play_count, finish_count, skip_count, like_count, dislike_count, score FROM symphony_user_track_affinity WHERE guild_id = %s AND user_id = %s AND url_key = %s", TEST_GUILD, TEST_USER2, key_b)
aff_b = aff_b and aff_b[1]
check("user_track_affinity dislike-only row has all counters non-null (new row, only dislike bumped)",
  aff_b and aff_b.queued_count ~= nil and aff_b.play_count ~= nil and aff_b.finish_count ~= nil
    and aff_b.skip_count ~= nil and aff_b.like_count ~= nil and aff_b.dislike_count ~= nil and aff_b.score ~= nil, tostring(aff_b))

-- pick_smart_recommendation / build_user_taste_summary (exercise the read side)
local ok7, url7 = pcall(S.pick_smart_recommendation, TEST_GUILD, { TEST_USER })
check("pick_smart_recommendation runs", ok7, url7)
local ok8, rows8 = pcall(S.build_user_taste_summary, TEST_GUILD, TEST_USER)
check("build_user_taste_summary runs", ok8, rows8)

-- history
local ok9, err9 = pcall(S.record_history, TEST_GUILD, "https://example.com/a", "Track A", TEST_USER)
check("record_history", ok9, err9)

-- playback_state upsert/clear
local ok10, err10 = pcall(S.persist_playback_state, TEST_GUILD, {
  channel_id = "111", url = "https://example.com/a", title = "Track A",
  position_ms = 42000, paused = false, track_uid = uid1,
})
check("persist_playback_state upsert", ok10, err10)
local ps = S.Q("SELECT position_seconds, is_playing FROM symphony_playback_state WHERE guild_id = %s AND bot_name = 'symphony'", TEST_GUILD)
ps = ps and ps[1]
check("playback_state position persisted", ps and tonumber(ps.position_seconds) == 42, ps and tostring(ps.position_seconds))
S.clear_playback_state(TEST_GUILD)
ps = S.Q("SELECT video_url, is_playing FROM symphony_playback_state WHERE guild_id = %s AND bot_name = 'symphony'", TEST_GUILD)
ps = ps and ps[1]
check("clear_playback_state clears", ps and ps.video_url == nil and ps.is_playing == false, tostring(ps))

-- voice_state upsert (the reconnect_attempts NOT-NULL gotcha; this table
-- previously had no INSERT path at all -- see symphony.lua's persist_voice_state)
local ok11, err11 = pcall(S.persist_voice_state, TEST_GUILD, "222", true)
check("persist_voice_state connect (was: table never written at all before fix)", ok11, err11)
local vs = S.Q("SELECT connected_channel_id::text AS connected_channel_id, desired_connected, reconnect_attempts FROM symphony_voice_state WHERE guild_id = %s AND bot_name = 'symphony'", TEST_GUILD)
vs = vs and vs[1]
check("voice_state connected row correct", vs and vs.connected_channel_id == "222" and vs.desired_connected == true and tonumber(vs.reconnect_attempts) == 0, tostring(vs))
S.Q("UPDATE symphony_voice_state SET desired_connected = FALSE, connected_channel_id = NULL, disconnected_at = NOW() WHERE guild_id = %s AND bot_name = 'symphony'", TEST_GUILD)
vs = S.Q("SELECT connected_channel_id, desired_connected FROM symphony_voice_state WHERE guild_id = %s AND bot_name = 'symphony'", TEST_GUILD)
vs = vs and vs[1]
check("voice_state disconnected row correct", vs and vs.connected_channel_id == nil and vs.desired_connected == false, tostring(vs))

-- heartbeat_tick (argument-order/count correctness against a live in-memory
-- playback entry, mirroring the gotcha class already found in sibling bots)
S.playback[TEST_GUILD] = {
  url = "https://example.com/a", title = "Track A", requester_id = TEST_USER,
  duration_ms = 200000, position_ms = 55000, position_at = os.time(), paused = false, speed = 1.0, active = true,
}
local ok12, err12 = pcall(S.heartbeat_tick)
check("heartbeat_tick runs with an active player", ok12, err12)
local metrics = S.Q("SELECT queue_count, is_paused_db, position_seconds, duration_seconds FROM symphony_metrics WHERE guild_id = %s AND bot_name = 'symphony'", TEST_GUILD)
metrics = metrics and metrics[1]
check("symphony_metrics row written", metrics ~= nil, tostring(metrics))
check("symphony_metrics is_paused_db is boolean false, not a position/duration value", metrics and metrics.is_paused_db == false, metrics and tostring(metrics.is_paused_db))
check("symphony_metrics position_seconds is a plausible position (not the duration/title)", metrics and tonumber(metrics.position_seconds) ~= nil and tonumber(metrics.position_seconds) < 300, metrics and tostring(metrics.position_seconds))
check("symphony_metrics duration_seconds == 200 (not swapped with position)", metrics and tonumber(metrics.duration_seconds) == 200, metrics and tostring(metrics.duration_seconds))
S.playback[TEST_GUILD] = nil

-- error_events (report_error / persist_error_event) -- previously not ported at all
local ok13, err13 = pcall(S.report_error, TEST_GUILD, "smoke_test", "smoke test error", "this is a test error event, safe to ignore")
check("report_error / persist_error_event (was: not ported at all before fix)", ok13, err13)
local ee = S.Q("SELECT title, error_type FROM symphony_error_events WHERE guild_id = %s AND error_type = 'smoke_test' ORDER BY created_at DESC LIMIT 1", TEST_GUILD)
ee = ee and ee[1]
check("error_events row persisted", ee and ee.title == "smoke test error", tostring(ee))

-- bot.on_error wiring sanity (does not go through the gateway; just confirms
-- the hook is callable with the (name, interaction, err) signature this bot's
-- vendored lib/swarmlua/bot.lua invokes it with)
local ok14, err14 = pcall(S.bot.on_error, "symphony_main_smoketest", { guild_id = TEST_GUILD }, "synthetic handler error")
check("bot.on_error hook callable", ok14, err14)

-- cleanup test rows
S.Q("DELETE FROM symphony_queue WHERE guild_id = %s AND bot_name = 'symphony'", TEST_GUILD)
S.Q("DELETE FROM symphony_queue_backup WHERE guild_id = %s AND bot_name = 'symphony'", TEST_GUILD)
S.Q("DELETE FROM symphony_playback_state WHERE guild_id = %s AND bot_name = 'symphony'", TEST_GUILD)
S.Q("DELETE FROM symphony_guild_settings WHERE guild_id = %s", TEST_GUILD)
S.Q("DELETE FROM symphony_swarm_toggles WHERE guild_id = %s", TEST_GUILD)
S.Q("DELETE FROM symphony_track_intelligence WHERE guild_id = %s", TEST_GUILD)
S.Q("DELETE FROM symphony_user_track_affinity WHERE guild_id = %s", TEST_GUILD)
S.Q("DELETE FROM symphony_voice_state WHERE guild_id = %s AND bot_name = 'symphony'", TEST_GUILD)
S.Q("DELETE FROM symphony_error_events WHERE guild_id = %s", TEST_GUILD)
S.Q("DELETE FROM symphony_metrics WHERE guild_id = %s AND bot_name = 'symphony'", TEST_GUILD)
S.Q("DELETE FROM symphony_history WHERE guild_id = %s", TEST_GUILD)
print("Cleanup complete.")

print(("\n%d passed, %d failed"):format(pass, fail))
os.exit(fail == 0 and 0 or 1)
