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
 *      - 若 SQL 含 "from TB_AppConfig" -> 加载 TB_AppConfig.csv 返回结果集
 *        支持简单 WHERE col='val' 单条件过滤
 *      - 其它 SQL -> 回一个空 DONE (成功, 无结果)
 *   4. TDS5 动态 SQL (0xE7, prepare/exec/dealloc): isql 的 SQLPrepare/SQLExecute
 *
 * 数据文件: 默认从工作目录读取 <TableName>.csv (第一行表头, 逗号分隔)
 *   可用环境变量 MOCK_DATA_DIR 指定数据目录
 *
 * 编译: gcc -O2 -Wall -o mockserver mockserver.c
 * 运行: ./mockserver [port]
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
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

/* mock 数据行: 从 CSV 文件加载, 每行 NCOLS 个字符串字段 */
typedef struct {
    char **cells;     /* NCOLS 个 char* */
} row_t;

/* 简单 WHERE 条件: col='val' (单条件). col_idx=-1 表示无 WHERE / 列未识别 */
typedef struct {
    int col_idx;
    char value[256];
} where_t;

/* 构造结果集: RESULT(列元数据) + N x ROW + DONE(COUNT)
 * rows 中只输出满足 where 条件的行. */
static void build_result_set(buf_t *b, row_t *rows, int nrows, const where_t *w) {
    /* 先统计匹配行数, 用于 DONE COUNT */
    int matched = 0;
    for (int r = 0; r < nrows; r++) {
        if (w->col_idx < 0 || strcmp(rows[r].cells[w->col_idx], w->value) == 0)
            matched++;
    }

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

    /* 行 (只输出匹配 where 的行) */
    for (int r = 0; r < nrows; r++) {
        if (w->col_idx >= 0 && strcmp(rows[r].cells[w->col_idx], w->value) != 0)
            continue;
        buf_byte(b, TDS_ROW_TOK);
        for (int c = 0; c < NCOLS; c++) {
            const char *val = rows[r].cells[c];
            if (g_cols[c].variable) {
                /* SYBCHAR: 1 字节长度 + 数据 */
                size_t l = strlen(val);
                if (l > 255) l = 255;
                buf_byte(b, (unsigned char)l);
                buf_put(b, val, l);
            } else if (g_cols[c].type == SYBINT4) {
                /* 4 字节 LE int */
                long v = strtol(val, NULL, 10);
                buf_le32(b, (uint32_t)v);
            } else if (g_cols[c].type == SYBINT1) {
                long v = strtol(val, NULL, 10);
                buf_byte(b, (unsigned char)(v & 0xff));
            } else if (g_cols[c].type == SYBBIT) {
                buf_byte(b, val[0] == '0' ? 0 : 1);
            }
        }
    }

    /* DONE + COUNT (用匹配行数, 而非总行数) */
    build_done(b, TDS_DONE_FINAL | TDS_DONE_COUNT, (uint32_t)matched);
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

/* ---------------- CSV 数据加载 ---------------- */
/* 数据目录: 默认工作目录, 可用环境变量 MOCK_DATA_DIR 覆盖 */
static const char *data_dir(void) {
    const char *d = getenv("MOCK_DATA_DIR");
    return (d && *d) ? d : ".";
}

/* 加载 CSV 文件 (第一行表头跳过, 逗号分隔, 不处理引号).
 * 返回 malloc 的 row_t 数组, *out_nrows = 行数; 失败返回 NULL. */
static row_t *load_csv(const char *table_name, int *out_nrows) {
    char path[512];
    snprintf(path, sizeof(path), "%s/%s.csv", data_dir(), table_name);

    FILE *f = fopen(path, "r");
    if (!f) {
        fprintf(stderr, "[mock] load_csv: cannot open %s: %s\n", path, strerror(errno));
        return NULL;
    }
    row_t *rows = NULL;
    int cap = 0, n = 0;
    char line[4096];
    int line_no = 0;
    while (fgets(line, sizeof(line), f)) {
        line_no++;
        if (line_no == 1) continue;            /* 跳过表头 */
        size_t l = strlen(line);
        while (l > 0 && (line[l-1] == '\n' || line[l-1] == '\r')) line[--l] = 0;
        if (l == 0) continue;

        /* 按逗号分割, 最多 NCOLS-1 个逗号 -> NCOLS 个字段 (最后一个含剩余字符) */
        char *fields[NCOLS];
        int nf = 0;
        char *start = line;
        char *p = line;
        while (*p && nf < NCOLS - 1) {
            if (*p == ',') {
                *p = 0;
                fields[nf++] = start;
                start = p + 1;
            }
            p++;
        }
        fields[nf++] = start;                  /* 最后一个字段 */
        while (nf < NCOLS) fields[nf++] = "";  /* 字段不足补空串 */

        if (n >= cap) {
            cap = cap ? cap * 2 : 16;
            rows = realloc(rows, cap * sizeof(row_t));
            if (!rows) { fclose(f); return NULL; }
        }
        rows[n].cells = malloc(NCOLS * sizeof(char*));
        for (int i = 0; i < NCOLS; i++)
            rows[n].cells[i] = strdup(fields[i]);
        n++;
    }
    fclose(f);
    *out_nrows = n;
    fprintf(stderr, "[mock] loaded %d rows from %s\n", n, path);
    return rows;
}

static void free_rows(row_t *rows, int nrows) {
    if (!rows) return;
    for (int i = 0; i < nrows; i++) {
        if (rows[i].cells) {
            for (int j = 0; j < NCOLS; j++) free(rows[i].cells[j]);
            free(rows[i].cells);
        }
    }
    free(rows);
}

/* 从 SQL 中提取 'from <table>' 后的表名. 返回 malloc 字符串 (原样大小写).
 * 找不到返回 NULL. */
static char *extract_table_name(const char *sql) {
    char buf[4096];
    size_t n = strlen(sql);
    if (n >= sizeof(buf)) n = sizeof(buf) - 1;
    memcpy(buf, sql, n);
    buf[n] = 0;
    str_lower(buf);
    char *p = strstr(buf, "from ");
    if (!p) return NULL;
    p += 5;                                     /* skip "from " */
    while (*p == ' ' || *p == '\t') p++;
    char *end = p;
    while (*end && *end != ' ' && *end != '\t' && *end != ';' && *end != '\n' && *end != '\r')
        end++;
    size_t tlen = end - p;
    if (tlen == 0) return NULL;
    char *name = malloc(tlen + 1);
    memcpy(name, p, tlen);
    name[tlen] = 0;
    return name;                                /* 小写形式 */
}

/* 解析简单 WHERE col='val' (单条件, 不区分大小写列名).
 * col_idx = 列在 g_cols 中的下标; -1 = 无 WHERE 或列未识别.
 * value 保留原始大小写 (从小写副本定位, 但从原 SQL 提取值). */
static where_t parse_where(const char *sql) {
    where_t w;
    w.col_idx = -1;
    w.value[0] = 0;

    char buf[4096];
    size_t n = strlen(sql);
    if (n >= sizeof(buf)) n = sizeof(buf) - 1;
    memcpy(buf, sql, n);
    buf[n] = 0;
    str_lower(buf);

    char *p = strstr(buf, "where ");
    if (!p) return w;
    size_t pos = (size_t)(p - buf) + 6;         /* skip "where " */
    while (pos < n && (buf[pos] == ' ' || buf[pos] == '\t')) pos++;

    /* 列名: 到 空格/= 制表符 */
    size_t cstart = pos;
    while (pos < n && buf[pos] != ' ' && buf[pos] != '\t' && buf[pos] != '=') pos++;
    size_t clen = pos - cstart;
    if (clen == 0 || clen >= 64) return w;
    char colname[64];
    memcpy(colname, buf + cstart, clen);
    colname[clen] = 0;

    /* 跳过空格找 '=' */
    while (pos < n && (buf[pos] == ' ' || buf[pos] == '\t')) pos++;
    if (pos >= n || buf[pos] != '=') return w;
    pos++;
    while (pos < n && (buf[pos] == ' ' || buf[pos] == '\t')) pos++;

    /* 期望 ' 或 " 开头; 也可以是裸值 */
    char quote = 0;
    if (pos < n && (buf[pos] == '\'' || buf[pos] == '"')) { quote = buf[pos]; pos++; }
    size_t vstart = pos;
    if (quote) {
        while (pos < n && buf[pos] != quote) pos++;
    } else {
        while (pos < n && buf[pos] != ' ' && buf[pos] != ';') pos++;
    }
    size_t vlen = pos - vstart;
    if (vlen == 0) return w;
    if (vlen >= sizeof(w.value)) vlen = sizeof(w.value) - 1;
    /* 从原始 sql 提取 value, 保留原大小写 */
    memcpy(w.value, sql + vstart, vlen);
    w.value[vlen] = 0;

    /* 找列索引 (列名匹配不区分大小写) */
    for (int i = 0; i < NCOLS; i++) {
        if (strcasecmp(g_cols[i].name, colname) == 0) {
            w.col_idx = i;
            break;
        }
    }
    return w;
}

/* 判断 SQL 是否针对 TB_AppConfig 表 (不区分大小写) */
static int is_appconfig_sql(const char *sql) {
    char *t = extract_table_name(sql);
    int yes = (t && strcasecmp(t, "TB_AppConfig") == 0);
    free(t);
    return yes;
}

/* ---------------- TDS5 动态 SQL (prepared statement) ---------------- */
/* FreeTDS ODBC 的 isql 走 SQLPrepare/SQLExecute, 在 TDS5 下体现为
 * dynamic token (0xE7): prepare(1) / execute(2) / dealloc(4).
 * 这里维护一个 id -> 已 prepare 的语句 的小表, 仅用于 mock. */
#define MAX_DYN 32
typedef struct {
    int used;
    char id[32];
    char *stmt;
    int is_appconfig;     /* SQL 是否针对 TB_AppConfig */
    where_t where;        /* prepare 时解析的 WHERE 条件, exec 时复用 */
} dyn_entry_t;

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
        if (e) {
            free(e->stmt);
            e->stmt = stmt;
            e->is_appconfig = is_appconfig_sql(stmt);
            e->where = parse_where(stmt);
        }

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
            int nrows = 0;
            row_t *rows = load_csv("TB_AppConfig", &nrows);
            if (rows) {
                build_result_set(out, rows, nrows, &e->where);
                if (e->where.col_idx >= 0)
                    fprintf(stderr, "[mock] -> TB_AppConfig WHERE %s='%s'\n",
                            g_cols[e->where.col_idx].name, e->where.value);
                else
                    fprintf(stderr, "[mock] -> returning TB_AppConfig (%d rows)\n", nrows);
                free_rows(rows, nrows);
            } else {
                build_done(out, TDS_DONE_FINAL | TDS_DONE_ERROR, 0);
            }
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
                if (is_appconfig_sql(sql)) {
                    where_t w = parse_where(sql);
                    int nrows = 0;
                    row_t *rows = load_csv("TB_AppConfig", &nrows);
                    if (rows) {
                        build_result_set(&out, rows, nrows, &w);
                        if (w.col_idx >= 0)
                            fprintf(stderr, "[mock] -> TB_AppConfig WHERE %s='%s'\n",
                                    g_cols[w.col_idx].name, w.value);
                        else
                            fprintf(stderr, "[mock] -> returning TB_AppConfig (%d rows)\n", nrows);
                        free_rows(rows, nrows);
                    } else {
                        build_done(&out, TDS_DONE_FINAL | TDS_DONE_ERROR, 0);
                    }
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
    fprintf(stderr, "[mock] data dir: %s  (override with MOCK_DATA_DIR env)\n", data_dir());
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
