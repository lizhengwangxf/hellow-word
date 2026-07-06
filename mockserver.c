/*
 * mockserver.c - A minimal mock of a Sybase Open Server.
 *
 * 环境: 没有 Sybase Open Server SDK (libsrv), 所以这里直接用原生 C
 * 实现一个最小可用的 TDS 5.0 服务端, 让 isql / tsql 这类 TDS 客户端
 * 能够登录、并执行 SQL 返回结果集。
 *
 * 行为:
 *   1. 监听一个 TCP 端口 (默认 4500)
 *   2. 接收 TDS 5.0 login 包 (0x02), 回 CAPABILITY + LOGINACK + DONE
 *   3. 接收 language 批处理包 (0x0F), 内含 TDS_LANGUAGE_TOKEN(0x21)
 *      - 若 SQL 含 "select ... from TB_AppConfig" -> 返回 mock 结果集
 *      - 其它 SQL -> 回一个空 DONE (成功, 无结果)
 *
 * 编译: gcc -O2 -Wall -o mockserver mockserver.c
 * 运行: ./mockserver [port]
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <ctype.h>
#include <errno.h>
#include <unistd.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/wait.h>

/* ---------------- TDS 协议常量 (来自 FreeTDS proto.h) --------------- */
#define TDS_PKT_LOGIN   0x02
#define TDS_PKT_REPLY   0x04
#define TDS_PKT_CANCEL  0x06
#define TDS_PKT_NORMAL  0x0F   /* TDS5 language batch */

#define TDS_LANG_TOKEN      0x21   /* TDS5 only */
#define TDS_CAPABILITY_TOK  0xE2
#define TDS_LOGINACK_TOK    0xAD
#define TDS_RESULT_TOK      0xEE   /* TDS5 column metadata */
#define TDS_ROW_TOK         0xD1
#define TDS_DONE_TOK        0xFD
#define TDS_ERROR_TOK       0xAA
#define TDS_INFO_TOK        0xAB
#define TDS5_DYNAMIC_TOK    0xE7   /* TDS5 dynamic SQL (prepare/exec/dealloc) */

/* TDS5 dynamic operations */
#define TDS_DYN_PREPARE     0x01
#define TDS_DYN_EXEC        0x02
#define TDS_DYN_DEALLOC     0x04

/* TDS 列类型 */
#define SYBCHAR  47   /* 变长, 行内 1 字节长度前缀 */
#define SYBINT4  56   /* 定长, 4 字节 */
#define SYBINT1  48   /* 定长, 1 字节 */
#define SYBBIT   50   /* 定长, 1 字节 */

/* DONE 标志位 */
#define TDS_DONE_FINAL  0x0000
#define TDS_DONE_MORE   0x0001
#define TDS_DONE_ERROR  0x0002
#define TDS_DONE_COUNT  0x0010

/* ---------------- 动态缓冲区, 用来拼 token 流 ---------------- */
typedef struct {
    unsigned char *data;
    size_t len;
    size_t cap;
} buf_t;

static void buf_init(buf_t *b) { b->data = NULL; b->len = 0; b->cap = 0; }
static void buf_free(buf_t *b) { free(b->data); b->data = NULL; b->len = b->cap = 0; }

static void buf_reserve(buf_t *b, size_t extra) {
    if (b->len + extra <= b->cap) return;
    size_t ncap = b->cap ? b->cap * 2 : 256;
    while (ncap < b->len + extra) ncap *= 2;
    unsigned char *nd = realloc(b->data, ncap);
    if (!nd) { perror("realloc"); exit(1); }
    b->data = nd; b->cap = ncap;
}
static void buf_put(buf_t *b, const void *p, size_t n) {
    buf_reserve(b, n);
    memcpy(b->data + b->len, p, n);
    b->len += n;
}
static void buf_byte(buf_t *b, unsigned char v) { buf_put(b, &v, 1); }
static void buf_le16(buf_t *b, uint16_t v) {
    unsigned char t[2] = { (unsigned char)(v & 0xff), (unsigned char)((v >> 8) & 0xff) };
    buf_put(b, t, 2);
}
static void buf_le32(buf_t *b, uint32_t v) {
    unsigned char t[4] = { (unsigned char)(v & 0xff), (unsigned char)((v >> 8) & 0xff),
                           (unsigned char)((v >> 16) & 0xff), (unsigned char)((v >> 24) & 0xff) };
    buf_put(b, t, 4);
}
static void buf_be16(buf_t *b, uint16_t v) {
    unsigned char t[2] = { (unsigned char)((v >> 8) & 0xff), (unsigned char)(v & 0xff) };
    buf_put(b, t, 2);
}
/* 在 offset 处回填一个 le16 (用于先占位再写长度) */
static void buf_patch_le16(buf_t *b, size_t off, uint16_t v) {
    b->data[off]   = (unsigned char)(v & 0xff);
    b->data[off+1] = (unsigned char)((v >> 8) & 0xff);
}

/* ---------------- 基础 IO ---------------- */
/* 读取恰好 n 字节 */
static int read_n(int fd, void *p, size_t n) {
    unsigned char *bp = p;
    size_t got = 0;
    while (got < n) {
        ssize_t r = read(fd, bp + got, n - got);
        if (r == 0) return 0;          /* 对端关闭 */
        if (r < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        got += (size_t)r;
    }
    return 1;
}

/* 读一个完整的 TDS 包 (会自动跨多个分片重组).
 * 返回: 1=成功, 0=对端关闭, -1=错误.
 * *type = 第一个分片的 type; 数据写入 out 缓冲区 (不含 8 字节头). */
static int read_tds_packet(int fd, unsigned char *type, buf_t *out) {
    out->len = 0;
    for (;;) {
        unsigned char hdr[8];
        int rc = read_n(fd, hdr, 8);
        if (rc != 1) return rc;
        uint16_t plen = ((uint16_t)hdr[2] << 8) | hdr[3];   /* big-endian */
        if (plen < 8) return -1;
        size_t body = plen - 8;
        if (out->len == 0) *type = hdr[0];   /* 取首个分片的 type */
        buf_reserve(out, body);
        rc = read_n(fd, out->data + out->len, body);
        if (rc != 1) return rc;
        out->len += body;
        if (hdr[1] & 0x01) return 1;         /* end of message */
        /* 否则继续读下一个分片 */
    }
}

/* 发送一个 TDS 包 (单分片, status=0x01). */
static int send_tds_packet(int fd, unsigned char type, const unsigned char *data, size_t len) {
    size_t total = 8 + len;
    if (total > 0xffff) {
        /* 简单分片: 每片最大 0xffff, status 0x00 / 末片 0x01 */
        size_t off = 0;
        unsigned char id = 1;
        while (off < len) {
            size_t chunk = len - off;
            if (chunk > 0xffff - 8) chunk = 0xffff - 8;
            unsigned char hdr[8];
            hdr[0] = type;
            hdr[1] = (off + chunk >= len) ? 0x01 : 0x00;
            uint16_t plen = (uint16_t)(8 + chunk);
            hdr[2] = (unsigned char)(plen >> 8);
            hdr[3] = (unsigned char)(plen & 0xff);
            hdr[4] = 0; hdr[5] = 0;
            hdr[6] = id++; hdr[7] = 0;
            if (write(fd, hdr, 8) != 8) return -1;
            if (chunk && write(fd, data + off, chunk) != (ssize_t)chunk) return -1;
            off += chunk;
        }
        return 0;
    }
    unsigned char hdr[8];
    hdr[0] = type;
    hdr[1] = 0x01;
    uint16_t plen = (uint16_t)total;
    hdr[2] = (unsigned char)(plen >> 8);
    hdr[3] = (unsigned char)(plen & 0xff);
    hdr[4] = 0; hdr[5] = 0;
    hdr[6] = 1; hdr[7] = 0;
    if (write(fd, hdr, 8) != 8) return -1;
    if (len && write(fd, data, len) != (ssize_t)len) return -1;
    return 0;
}

/* ---------------- token 构造 ---------------- */

/* 登录应答: CAPABILITY + LOGINACK + DONE */
static void build_login_reply(buf_t *b) {
    /* --- CAPABILITY (0xE2) --- */
    buf_byte(b, TDS_CAPABILITY_TOK);
    buf_le16(b, 18);
    static const unsigned char caps[18] = {
        1,7,7,97,65,207,255,255,230,2,7,0,0,2,0,0,0,0
    };
    buf_put(b, caps, 18);

    /* --- LOGINACK (0xAD) ---
     * 帧体: ack(1) + version(4 BE) + progname_len(1) + progname + product_version(4 BE)
     * length 字段 = 10 + strlen(progname) */
    const char *prog = "SQL Server";   /* Sybase 经典 product name */
    size_t plen = strlen(prog);
    buf_byte(b, TDS_LOGINACK_TOK);
    buf_le16(b, (uint16_t)(10 + plen));
    buf_byte(b, 5);                    /* ack=5 -> TDS5 登录成功 */
    /* version, big-endian: 5.0.0.0 */
    buf_byte(b, 0x05); buf_byte(b, 0x00); buf_byte(b, 0x00); buf_byte(b, 0x00);
    buf_byte(b, (unsigned char)plen);
    buf_put(b, prog, plen);
    /* product version, big-endian: 5.0.0.0 */
    buf_byte(b, 0x05); buf_byte(b, 0x00); buf_byte(b, 0x00); buf_byte(b, 0x00);

    /* --- DONE (0xFD) --- 登录结束, flags=0, rows=0 */
    buf_byte(b, TDS_DONE_TOK);
    buf_le16(b, TDS_DONE_FINAL);
    buf_le16(b, 0x0002);                /* 事务状态, 与真实服务器一致 */
    buf_le32(b, 0);
}

/* DONE token (用于非结果集命令的成功应答) */
static void build_done(buf_t *b, uint16_t flags, uint32_t rows) {
    buf_byte(b, TDS_DONE_TOK);
    buf_le16(b, flags);
    buf_le16(b, 0x0002);
    buf_le32(b, rows);
}

/* ---------------- mock 表 TB_AppConfig ---------------- */
typedef struct {
    const char *name;
    unsigned char type;     /* SYB* 类型 */
    int size;               /* 变长: 最大长度; 定长: 字节数 */
    int variable;           /* 1=变长(元数据里有 size 字节, 行里有 len 前缀) */
} col_def_t;

static const col_def_t g_cols[] = {
    { "AppConfigID", SYBINT4, 4,   0 },
    { "AppName",     SYBCHAR, 50,  1 },
    { "ConfigKey",   SYBCHAR, 50,  1 },
    { "ConfigValue", SYBCHAR, 100, 1 },
    { "Remark",      SYBCHAR, 100, 1 },
    { "Status",      SYBINT4, 4,   0 },
};
#define NCOLS (int)(sizeof(g_cols)/sizeof(g_cols[0]))

/* mock 数据行 (全部用字符串表达, INT 列会转成 4 字节 LE) */
static const char *g_rows[][NCOLS] = {
    { "1001", "OrderService",   "DB_TIMEOUT",  "30",     "order db timeout seconds",     "1" },
    { "1002", "OrderService",   "MAX_RETRY",   "3",      "max retry count",              "1" },
    { "1003", "PaymentService", "CURRENCY",    "CNY",    "default currency",             "1" },
    { "1004", "PaymentService", "RATE_LIMIT",  "1000",   "qps limit",                    "0" },
    { "1005", "UserService",    "SESSION_TTL", "3600",   "session ttl seconds",          "1" },
    { "1006", "UserService",    "PWD_POLICY",  "STRONG", "password policy",              "1" },
    { "1007", "Gateway",        "PORT",        "8080",   "listen port",                  "1" },
    { "1008", "Gateway",        "TLS_ENABLED", "1",      "enable tls",                   "1" },
};
#define NROWS (int)(sizeof(g_rows)/sizeof(g_rows[0]))

/* 构造结果集: RESULT(列元数据) + N x ROW + DONE(COUNT) */
static void build_result_set(buf_t *b) {
    size_t start = b->len;
    buf_byte(b, TDS_RESULT_TOK);
    size_t totlen_off = b->len;
    buf_le16(b, 0);                 /* 占位, 稍后回填 */

    /* num_cols */
    buf_le16(b, (uint16_t)NCOLS);

    /* 每列元数据:
     *   name_len(1) + name + flags(1,='0'=0x30) + usertype(4 LE) + type(1)
     *   + [变长: size(1)] + locale_len(1,=0) */
    for (int i = 0; i < NCOLS; i++) {
        const col_def_t *c = &g_cols[i];
        size_t nlen = strlen(c->name);
        buf_byte(b, (unsigned char)nlen);
        buf_put(b, c->name, nlen);
        buf_byte(b, 0x30);                       /* flags: nullable|writeable */
        buf_le32(b, 0);                          /* usertype */
        buf_byte(b, c->type);
        if (c->variable) buf_byte(b, (unsigned char)c->size);
        buf_byte(b, 0);                          /* locale len = 0 */
    }

    /* 回填 totlen = 当前长度 - (start+1)  (即 0xEE 之后所有字节数) */
    uint16_t totlen = (uint16_t)(b->len - (start + 1));
    buf_patch_le16(b, totlen_off, totlen);

    /* 行 */
    for (int r = 0; r < NROWS; r++) {
        buf_byte(b, TDS_ROW_TOK);
        for (int c = 0; c < NCOLS; c++) {
            if (g_cols[c].variable) {
                /* SYBCHAR: 1 字节长度 + 数据 */
                size_t l = strlen(g_rows[r][c]);
                if (l > 255) l = 255;
                buf_byte(b, (unsigned char)l);
                buf_put(b, g_rows[r][c], l);
            } else if (g_cols[c].type == SYBINT4) {
                /* 4 字节 LE int */
                long v = strtol(g_rows[r][c], NULL, 10);
                buf_le32(b, (uint32_t)v);
            } else if (g_cols[c].type == SYBINT1) {
                long v = strtol(g_rows[r][c], NULL, 10);
                buf_byte(b, (unsigned char)(v & 0xff));
            } else if (g_cols[c].type == SYBBIT) {
                buf_byte(b, g_rows[r][c][0] == '0' ? 0 : 1);
            }
        }
    }

    /* DONE + COUNT */
    build_done(b, TDS_DONE_FINAL | TDS_DONE_COUNT, (uint32_t)NROWS);
}

/* 构造一个简单的 ERROR token (用于不支持的语句时可选) */
static void build_info(buf_t *b, const char *msg) {
    /* INFO token: 0xAB, len(2 LE), severity(4) + state(1) + line(2) + ... 简化 */
    buf_byte(b, TDS_INFO_TOK);
    size_t off = b->len;
    buf_le16(b, 0);
    size_t mstart = b->len;
    buf_le32(b, 0);        /* severity */
    buf_le32(b, 0);        /* error */
    buf_le32(b, 0);        /* state? 简化处理, 客户端只跳过 */
    buf_byte(b, 0);        /* state */
    buf_le16(b, 0);        /* line */
    buf_byte(b, 0);        /* status */
    buf_le16(b, 0);        /* msg len (简写为0) */
    buf_le16(b, 0);        /* server name len */
    buf_le16(b, 0);        /* proc name len */
    buf_le32(b, 0);        /* line */
    (void)msg; (void)mstart;
    buf_patch_le16(b, off, (uint16_t)(b->len - off - 2));
}

/* ---------------- 工具 ---------------- */
static void str_lower(char *s) {
    for (; *s; s++) *s = (char)tolower((unsigned char)*s);
}

static int sql_mentions(const char *sql, const char *kw) {
    char buf[4096];
    size_t n = strlen(sql);
    if (n >= sizeof(buf)) n = sizeof(buf) - 1;
    memcpy(buf, sql, n);
    buf[n] = 0;
    str_lower(buf);
    return strstr(buf, kw) != NULL;
}

/* 从 TDS5 language 包数据中提取 SQL 文本.
 * 包数据格式: [0x21][len:4 LE][params_flag:1][sql: len-1]
 * 返回 malloc 的字符串 (调用方 free). */
static char *extract_sql(const buf_t *pkt) {
    if (pkt->len < 6) return NULL;
    if (pkt->data[0] != TDS_LANG_TOKEN) return NULL;
    uint32_t L = (uint32_t)pkt->data[1]
               | ((uint32_t)pkt->data[2] << 8)
               | ((uint32_t)pkt->data[3] << 16)
               | ((uint32_t)pkt->data[4] << 24);
    if (L < 1 || 5 + L > pkt->len) return NULL;
    size_t sql_len = L - 1;                 /* 去掉 params_flag */
    char *sql = malloc(sql_len + 1);
    if (!sql) return NULL;
    memcpy(sql, pkt->data + 6, sql_len);    /* 跳过 0x21 + 4长度 + 1 flag */
    sql[sql_len] = 0;
    return sql;
}

/* ---------------- TDS5 动态 SQL (prepared statement) ---------------- */
/* FreeTDS ODBC 的 isql 走 SQLPrepare/SQLExecute, 在 TDS5 下体现为
 * dynamic token (0xE7): prepare(1) / execute(2) / dealloc(4).
 * 这里维护一个 id -> 已 prepare 的语句 的小表, 仅用于 mock. */
#define MAX_DYN 32
typedef struct { int used; char id[32]; char *stmt; int is_appconfig; } dyn_entry_t;

static dyn_entry_t *dyn_find(dyn_entry_t *d, const char *id) {
    for (int i = 0; i < MAX_DYN; i++)
        if (d[i].used && strcmp(d[i].id, id) == 0) return &d[i];
    return NULL;
}
static dyn_entry_t *dyn_put(dyn_entry_t *d, const char *id) {
    dyn_entry_t *e = dyn_find(d, id);
    if (!e) {
        for (int i = 0; i < MAX_DYN; i++) {
            if (!d[i].used) { e = &d[i]; memset(e, 0, sizeof(*e)); e->used = 1; strncpy(e->id, id, sizeof(e->id)-1); break; }
        }
    }
    return e;
}
static void dyn_del(dyn_entry_t *d, const char *id) {
    dyn_entry_t *e = dyn_find(d, id);
    if (!e) return;
    free(e->stmt);
    memset(e, 0, sizeof(*e));
}

/* 处理 0xE7 dynamic 包. 返回:
 *   0  -> 已构造应答写入 out
 *   -1 -> 解析失败/忽略 */
static int handle_dynamic(const buf_t *pkt, buf_t *out, dyn_entry_t *dyns) {
    if (pkt->len < 8 || pkt->data[0] != TDS5_DYNAMIC_TOK) return -1;
    const unsigned char *p = pkt->data + 1;
    /* outer_len(2 LE) */ p += 2;
    unsigned char op = *p++;
    unsigned char status = *p++;   (void)status;
    unsigned char id_len = *p++;
    if (id_len == 0 || id_len > 31 || p + id_len > pkt->data + pkt->len) return -1;
    char id[32] = {0};
    memcpy(id, p, id_len); p += id_len;

    if (op == TDS_DYN_PREPARE) {
        /* stmt_len(2 LE) + stmt */
        if (p + 2 > pkt->data + pkt->len) return -1;
        uint16_t slen = (uint16_t)p[0] | ((uint16_t)p[1] << 8); p += 2;
        if (p + slen > pkt->data + pkt->len) return -1;
        char *stmt = malloc(slen + 1);
        if (!stmt) return -1;
        memcpy(stmt, p, slen); stmt[slen] = 0;

        dyn_entry_t *e = dyn_put(dyns, id);
        if (e) { free(e->stmt); e->stmt = stmt; e->is_appconfig = sql_mentions(stmt, "tb_appconfig"); }

        /* 去掉换行方便日志 */
        char logbuf[256]; size_t ln = slen < sizeof(logbuf)-1 ? slen : sizeof(logbuf)-1;
        memcpy(logbuf, stmt, ln); logbuf[ln] = 0;
        for (char *q = logbuf; *q; q++) if (*q=='\n'||*q=='\r') *q=' ';
        fprintf(stderr, "[mock] DYN PREPARE id=%s -> %s\n", id, logbuf);

        /* 应答: 一个空 DONE 即可被客户端接受 */
        out->len = 0;
        build_done(out, TDS_DONE_FINAL, 0);
        return 0;
    }

    if (op == TDS_DYN_EXEC) {
        /* 后面跟 smallint 0 (无参数时) */
        fprintf(stderr, "[mock] DYN EXEC id=%s\n", id);
        out->len = 0;
        dyn_entry_t *e = dyn_find(dyns, id);
        if (e && e->is_appconfig) {
            build_result_set(out);
            fprintf(stderr, "[mock] -> returning TB_AppConfig (%d rows)\n", NROWS);
        } else {
            build_done(out, TDS_DONE_FINAL, 0);
        }
        return 0;
    }

    if (op == TDS_DYN_DEALLOC) {
        fprintf(stderr, "[mock] DYN DEALLOC id=%s\n", id);
        dyn_del(dyns, id);
        out->len = 0;
        build_done(out, TDS_DONE_FINAL, 0);
        return 0;
    }

    /* 未知 op */
    out->len = 0;
    build_done(out, TDS_DONE_FINAL, 0);
    return 0;
}

/* ---------------- 连接处理 ---------------- */
static void handle_client(int cfd, struct sockaddr_in *cli) {
    char ip[64] = {0};
    inet_ntop(AF_INET, &cli->sin_addr, ip, sizeof(ip));
    fprintf(stderr, "[mock] client connected: %s:%d\n", ip, ntohs(cli->sin_port));

    buf_t pkt; buf_init(&pkt);
    buf_t out; buf_init(&out);
    int logged_in = 0;
    dyn_entry_t dyns[MAX_DYN];
    memset(dyns, 0, sizeof(dyns));

    for (;;) {
        unsigned char type = 0;
        int rc = read_tds_packet(cfd, &type, &pkt);
        if (rc != 1) {
            fprintf(stderr, "[mock] read_tds_packet end (rc=%d)\n", rc);
            break;
        }

        if (type == TDS_PKT_LOGIN) {
            /* 接收 login, 不校验用户名密码, 直接回成功 */
            fprintf(stderr, "[mock] LOGIN packet (%zu bytes), accepting\n", pkt.len);
            out.len = 0;
            build_login_reply(&out);
            if (send_tds_packet(cfd, TDS_PKT_REPLY, out.data, out.len) < 0) break;
            logged_in = 1;
            fprintf(stderr, "[mock] login reply sent\n");
            continue;
        }

        if (type == TDS_PKT_CANCEL) {
            fprintf(stderr, "[mock] CANCEL received\n");
            continue;
        }

        if (type == TDS_PKT_NORMAL) {
            unsigned char tok = pkt.len ? pkt.data[0] : 0;

            /* 1) language 批处理 (tsql 直接发 SQL 走这里) */
            if (tok == TDS_LANG_TOKEN) {
                char *sql = extract_sql(&pkt);
                if (!sql) {
                    out.len = 0;
                    build_done(&out, TDS_DONE_FINAL, 0);
                    if (send_tds_packet(cfd, TDS_PKT_REPLY, out.data, out.len) < 0) break;
                    continue;
                }
                for (char *p = sql; *p; p++)
                    if (*p == '\n' || *p == '\r') *p = ' ';
                fprintf(stderr, "[mock] SQL: %s\n", sql);

                out.len = 0;
                if (sql_mentions(sql, "tb_appconfig")) {
                    build_result_set(&out);
                    fprintf(stderr, "[mock] -> returning TB_AppConfig (%d rows)\n", NROWS);
                } else {
                    build_done(&out, TDS_DONE_FINAL, 0);
                }
                if (send_tds_packet(cfd, TDS_PKT_REPLY, out.data, out.len) < 0) {
                    free(sql);
                    break;
                }
                free(sql);
                continue;
            }

            /* 2) dynamic SQL (isql 的 SQLPrepare/SQLExecute 走这里) */
            if (tok == TDS5_DYNAMIC_TOK) {
                if (handle_dynamic(&pkt, &out, dyns) == 0) {
                    if (send_tds_packet(cfd, TDS_PKT_REPLY, out.data, out.len) < 0) break;
                    continue;
                }
                /* 解析失败, 兜底 */
                out.len = 0;
                build_done(&out, TDS_DONE_FINAL, 0);
                if (send_tds_packet(cfd, TDS_PKT_REPLY, out.data, out.len) < 0) break;
                continue;
            }

            /* 3) 其它 TDS5 token (option/logout 等): 回空 DONE */
            fprintf(stderr, "[mock] normal packet tok=0x%02x (%zu bytes), reply DONE\n",
                    tok, pkt.len);
            out.len = 0;
            build_done(&out, TDS_DONE_FINAL, 0);
            if (send_tds_packet(cfd, TDS_PKT_REPLY, out.data, out.len) < 0) break;
            continue;
        }

        /* 未知包类型: 回一个空 DONE 兜底 */
        fprintf(stderr, "[mock] unknown packet type 0x%02x (%zu bytes)\n", type, pkt.len);
        out.len = 0;
        build_done(&out, TDS_DONE_FINAL, 0);
        if (send_tds_packet(cfd, TDS_PKT_REPLY, out.data, out.len) < 0) break;
    }

    /* 释放动态语句表 */
    for (int i = 0; i < MAX_DYN; i++) free(dyns[i].stmt);
    (void)logged_in;
    buf_free(&pkt);
    buf_free(&out);
    close(cfd);
    fprintf(stderr, "[mock] client %s:%d done\n", ip, ntohs(cli->sin_port));
}

static void sigchld_handler(int sig) {
    (void)sig;
    while (waitpid(-1, NULL, WNOHANG) > 0) {}
}

int main(int argc, char **argv) {
    int port = (argc > 1) ? atoi(argv[1]) : 4500;

    signal(SIGPIPE, SIG_IGN);
    signal(SIGCHLD, sigchld_handler);

    int sfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sfd < 0) { perror("socket"); return 1; }
    int opt = 1;
    setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons((uint16_t)port);

    if (bind(sfd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind"); return 1;
    }
    if (listen(sfd, 16) < 0) { perror("listen"); return 1; }

    fprintf(stderr, "[mock] Sybase Open Server mock listening on 0.0.0.0:%d (TDS 5.0)\n", port);
    fprintf(stderr, "[mock] try:  tsql -S mocksyb -U sa -P ''   (then: select * from TB_AppConfig\\ngo)\n");

    for (;;) {
        struct sockaddr_in cli;
        socklen_t clen = sizeof(cli);
        int cfd = accept(sfd, (struct sockaddr *)&cli, &clen);
        if (cfd < 0) {
            if (errno == EINTR) continue;
            perror("accept");
            continue;
        }
        pid_t pid = fork();
        if (pid == 0) {
            close(sfd);
            handle_client(cfd, &cli);
            _exit(0);
        }
        close(cfd);   /* 父进程不处理这个连接 */
    }
    close(sfd);
    return 0;
}
