/**
 * @file inception_parse.cc
 * @brief Parse inception magic comments.
 *
 * Comment format:
 *   / *--user=root;--password=xxx;--host=10.0.0.1;--port=3306;
 *     --execute=1;inception_magic_start;* /
 */

#include "sql/inception/inception_parse.h"

#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "sql/inception/inception_context.h"
#include "sql/inception/inception_sysvars.h"
#include "include/my_aes.h"
#include "include/base64.h"

namespace inception {

/* Helper: skip leading whitespace */
static const char *skip_whitespace(const char *p, const char *end) {
  while (p < end && (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n'))
    ++p;
  return p;
}

/* Helper: find substring case-insensitive */
static const char *find_ci(const char *haystack, size_t haystack_len,
                           const char *needle) {
  size_t needle_len = strlen(needle);
  if (needle_len > haystack_len) return nullptr;
  for (size_t i = 0; i <= haystack_len - needle_len; ++i) {
    if (strncasecmp(haystack + i, needle, needle_len) == 0)
      return haystack + i;
  }
  return nullptr;
}

/**
 * Find the length of the first C-style comment starting at p.
 * p must point to the '/' of '/ *'. Returns length including '* /' or
 * 0 if no closing found.
 */
static size_t first_comment_len(const char *p, const char *end) {
  if (p + 2 > end || p[0] != '/' || p[1] != '*') return 0;
  const char *close = find_ci(p + 2, static_cast<size_t>(end - p - 2), "*/");
  if (!close) return 0;
  return static_cast<size_t>(close + 2 - p);
}

bool is_inception_start(const char *query, size_t length) {
  if (!query || length < 24) return false;
  const char *p = skip_whitespace(query, query + length);
  size_t clen = first_comment_len(p, query + length);
  if (clen == 0) return false;
  /* Only search within the first comment */
  return find_ci(p, clen, "inception_magic_start") != nullptr;
}

bool is_inception_commit(const char *query, size_t length) {
  if (!query || length < 24) return false;
  const char *p = skip_whitespace(query, query + length);
  size_t clen = first_comment_len(p, query + length);
  if (clen == 0) return false;
  /* Only search within the first comment */
  return find_ci(p, clen, "inception_magic_commit") != nullptr;
}

/**
 * Parse a single --key=value token and populate ctx.
 */
static void parse_option(const char *key, size_t key_len, const char *val,
                         size_t val_len, InceptionContext *ctx) {
  auto match = [&](const char *name) -> bool {
    return key_len == strlen(name) && strncasecmp(key, name, key_len) == 0;
  };

  if (match("host")) {
    ctx->host.assign(val, val_len);
    ctx->explicit_host = true;
  } else if (match("user")) {
    ctx->user.assign(val, val_len);
    ctx->explicit_user = true;
  } else if (match("password")) {
    ctx->password.assign(val, val_len);
  } else if (match("port")) {
    ctx->port = static_cast<uint>(strtoul(val, nullptr, 10));
    ctx->explicit_port = true;
  } else if (match("enable-execute")) {
    if (val_len > 0 && val[0] == '1') ctx->mode = OpMode::EXECUTE;
  } else if (match("enable-check")) {
    if (val_len > 0 && val[0] == '1') ctx->mode = OpMode::CHECK;
  } else if (match("enable-split")) {
    if (val_len > 0 && val[0] == '1') ctx->mode = OpMode::SPLIT;
  } else if (match("enable-query-tree")) {
    if (val_len > 0 && val[0] == '1') ctx->mode = OpMode::QUERY_TREE;
  } else if (match("enable-force")) {
    ctx->force = (val_len > 0 && val[0] == '1');
  } else if (match("enable-remote-backup")) {
    ctx->backup = (val_len > 0 && val[0] == '1');
  } else if (match("enable-ignore-warnings")) {
    ctx->ignore_warnings = (val_len > 0 && val[0] == '1');
  } else if (match("sleep")) {
    ctx->sleep_ms = strtoull(val, nullptr, 10);
  } else if (match("slave-hosts") || match("slave_hosts")) {
    /* Parse "ip1:port1,ip2:port2" format */
    std::string v(val, val_len);
    ctx->slave_hosts.clear();
    size_t pos = 0;
    while (pos < v.size()) {
      size_t comma = v.find(',', pos);
      if (comma == std::string::npos) comma = v.size();
      std::string entry = v.substr(pos, comma - pos);
      auto colon = entry.rfind(':');
      if (colon != std::string::npos) {
        std::string h = entry.substr(0, colon);
        uint p = static_cast<uint>(strtoul(entry.c_str() + colon + 1, nullptr, 10));
        if (!h.empty() && p > 0)
          ctx->slave_hosts.emplace_back(h, p);
      }
      pos = comma + 1;
    }
  }
}

/**
 * Decrypt a password if it has the "AES:" prefix.
 * Uses AES-128-ECB (same as MySQL AES_ENCRYPT/AES_DECRYPT default).
 * Returns the original string if no prefix or decryption fails.
 */
static std::string decrypt_password(const std::string &encrypted) {
  if (encrypted.size() <= 4 || strncmp(encrypted.c_str(), "AES:", 4) != 0)
    return encrypted;
  if (!opt_inception_password_encrypt_key ||
      opt_inception_password_encrypt_key[0] == '\0')
    return encrypted;

  const char *b64 = encrypted.c_str() + 4;
  size_t b64_len = encrypted.size() - 4;

  /* base64 decode */
  uint64 decoded_alloc = base64_needed_decoded_length(b64_len);
  std::vector<unsigned char> decoded(decoded_alloc);
  int64 decoded_len =
      base64_decode(b64, b64_len, decoded.data(), nullptr, 0);
  if (decoded_len <= 0) return encrypted;

  /* AES-128-ECB decrypt */
  uint32 key_len =
      static_cast<uint32>(strlen(opt_inception_password_encrypt_key));
  std::vector<unsigned char> plain(decoded_len + MY_AES_BLOCK_SIZE);
  int plain_len = my_aes_decrypt(
      decoded.data(), static_cast<uint32>(decoded_len), plain.data(),
      reinterpret_cast<const unsigned char *>(opt_inception_password_encrypt_key),
      key_len, my_aes_128_ecb, nullptr, true);
  if (plain_len <= 0) return encrypted;

  return std::string(reinterpret_cast<char *>(plain.data()), plain_len);
}

bool parse_inception_start(const char *query, size_t length,
                           InceptionContext *ctx) {
  ctx->reset();

  /* Find comment body: skip leading whitespace, "/*" prefix */
  const char *p = skip_whitespace(query, query + length);
  const char *end = query + length;
  if (p + 2 >= end || p[0] != '/' || p[1] != '*') return true;
  p += 2;

  /* Find end of comment */
  const char *comment_end = find_ci(p, static_cast<size_t>(end - p), "*/");
  if (!comment_end) comment_end = end;

  /* Tokenize by ';' */
  while (p < comment_end) {
    p = skip_whitespace(p, comment_end);
    if (p >= comment_end) break;

    /* Find next ';' */
    const char *semi = static_cast<const char *>(
        memchr(p, ';', static_cast<size_t>(comment_end - p)));
    if (!semi) semi = comment_end;

    const char *token = p;
    size_t token_len = static_cast<size_t>(semi - token);
    p = semi + 1;

    /* Skip leading "--" */
    if (token_len >= 2 && token[0] == '-' && token[1] == '-') {
      token += 2;
      token_len -= 2;
    }

    /* Skip "inception_magic_start" token itself */
    if (token_len >= 20 &&
        strncasecmp(token, "inception_magic_start", 21) == 0)
      continue;

    /* Split key=value, or treat as flag if no '=' */
    const char *eq =
        static_cast<const char *>(memchr(token, '=', token_len));
    if (eq) {
      const char *key = token;
      size_t key_len = static_cast<size_t>(eq - token);
      const char *val = eq + 1;
      size_t val_len = token_len - key_len - 1;
      parse_option(key, key_len, val, val_len, ctx);
    }
  }

  /* Fall back to global inception_user/inception_password if not specified */
  if (ctx->user.empty() && opt_inception_user && opt_inception_user[0] != '\0')
    ctx->user = opt_inception_user;
  if (ctx->password.empty() && opt_inception_password && opt_inception_password[0] != '\0')
    ctx->password = opt_inception_password;

  /* Decrypt password if it has "AES:" prefix */
  if (!ctx->password.empty())
    ctx->password = decrypt_password(ctx->password);

  if (ctx->explicit_port && (ctx->port == 0 || ctx->port > 65535)) {
    return true;
  }

  ctx->active = true;
  return false;  // success
}

}  // namespace inception
