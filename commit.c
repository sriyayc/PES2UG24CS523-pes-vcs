// commit.c — Commit creation and history traversal
//
// Commit object format (stored as text, one field per line):
//
// tree <64-char-hex-hash>
// parent <64-char-hex-hash>    ← omitted for the very first commit
// author <name> <unix-timestamp>
// committer <name> <unix-timestamp>
//
// <commit message>
//
// Note: there is a blank line between the headers and the message.
//
// PROVIDED functions: commit_parse, commit_serialize, commit_walk, head_read, head_update
// TODO functions: commit_create

#include "commit.h"
#include "index.h"
#include "tree.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>

// Forward declarations (implemented in object.c)
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out);
int object_read(const ObjectID *id, ObjectType *type_out, void **data_out, size_t *len_out);

// ─── PROVIDED ────────────────────────────────────────────────────────────────

int commit_parse(const void *data, size_t len, Commit *commit_out) {
    (void)len;
    const char *p = (const char *)data;
    char hex[HASH_HEX_SIZE + 1];

    if (sscanf(p, "tree %64s\n", hex) != 1) return -1;
    if (hex_to_hash(hex, &commit_out->tree) != 0) return -1;
    p = strchr(p, '\n') + 1;

    if (strncmp(p, "parent ", 7) == 0) {
        if (sscanf(p, "parent %64s\n", hex) != 1) return -1;
        if (hex_to_hash(hex, &commit_out->parent) != 0) return -1;
        commit_out->has_parent = 1;
        p = strchr(p, '\n') + 1;
    } else {
        commit_out->has_parent = 0;
    }

    char author_buf[256];
    uint64_t ts;
    if (sscanf(p, "author %255[^\n]\n", author_buf) != 1) return -1;
    char *last_space = strrchr(author_buf, ' ');
    if (!last_space) return -1;
    ts = (uint64_t)strtoull(last_space + 1, NULL, 10);
    *last_space = '\0';
    strncpy(commit_out->author, author_buf, sizeof(commit_out->author) - 1);
    commit_out->timestamp = ts;

    p = strchr(p, '\n') + 1;  // skip author line
    p = strchr(p, '\n') + 1;  // skip committer line
    p = strchr(p, '\n') + 1;  // skip blank line

    strncpy(commit_out->message, p, sizeof(commit_out->message) - 1);
    return 0;
}

int commit_serialize(const Commit *commit, void **data_out, size_t *len_out) {
    char tree_hex[HASH_HEX_SIZE + 1];
    char parent_hex[HASH_HEX_SIZE + 1];
    hash_to_hex(&commit->tree, tree_hex);

    char buf[8192];
    int n = 0;
    n += snprintf(buf + n, sizeof(buf) - n, "tree %s\n", tree_hex);
    if (commit->has_parent) {
        hash_to_hex(&commit->parent, parent_hex);
        n += snprintf(buf + n, sizeof(buf) - n, "parent %s\n", parent_hex);
    }
    n += snprintf(buf + n, sizeof(buf) - n,
                  "author %s %" PRIu64 "\n"
                  "committer %s %" PRIu64 "\n"
                  "\n"
                  "%s",
                  commit->author, commit->timestamp,
                  commit->author, commit->timestamp,
                  commit->message);

    *data_out = malloc(n + 1);
    if (!*data_out) return -1;
    memcpy(*data_out, buf, n + 1);
    *len_out = (size_t)n;
    return 0;
}

int commit_walk(commit_walk_fn callback, void *ctx) {
    ObjectID id;
    if (head_read(&id) != 0) return -1;

    while (1) {
        ObjectType type;
        void *raw;
        size_t raw_len;
        if (object_read(&id, &type, &raw, &raw_len) != 0) return -1;

        Commit c;
        int rc = commit_parse(raw, raw_len, &c);
        free(raw);
        if (rc != 0) return -1;

        callback(&id, &c, ctx);
        if (!c.has_parent) break;
        id = c.parent;
    }
    return 0;
}

int head_read(ObjectID *id_out) {
    FILE *f = fopen(HEAD_FILE, "r");
    if (!f) return -1;

    char line[512];
    if (!fgets(line, sizeof(line), f)) { fclose(f); return -1; }
    fclose(f);
    line[strcspn(line, "\r\n")] = '\0';

    char ref_path[512];
    if (strncmp(line, "ref: ", 5) == 0) {
        snprintf(ref_path, sizeof(ref_path), "%s/%s", PES_DIR, line + 5);
        f = fopen(ref_path, "r");
        if (!f) return -1;
        if (!fgets(line, sizeof(line), f)) { fclose(f); return -1; }
        fclose(f);
        line[strcspn(line, "\r\n")] = '\0';
    }

    return hex_to_hash(line, id_out);
}

int head_update(const ObjectID *new_commit) {
    FILE *f = fopen(HEAD_FILE, "r");
    if (!f) return -1;

    char line[512];
    if (!fgets(line, sizeof(line), f)) { fclose(f); return -1; }
    fclose(f);
    line[strcspn(line, "\r\n")] = '\0';

    char target_path[512];
    if (strncmp(line, "ref: ", 5) == 0) {
        snprintf(target_path, sizeof(target_path), "%s/%s", PES_DIR, line + 5);
    } else {
        snprintf(target_path, sizeof(target_path), "%s", HEAD_FILE);
    }

    char tmp_path[512];
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", target_path);

    f = fopen(tmp_path, "w");
    if (!f) return -1;

    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(new_commit, hex);
    fprintf(f, "%s\n", hex);
    fflush(f);
    fsync(fileno(f));
    fclose(f);

    return rename(tmp_path, target_path);
}

// ─── IMPLEMENTED ─────────────────────────────────────────────────────────────

// Create a new commit from whatever is currently staged in the index.
//
// Here's what happens step by step:
//   1. Build a tree object from the staged files (the snapshot)
//   2. Read the current HEAD commit (will be our parent, if one exists)
//   3. Fill in the Commit struct with tree, parent, author, timestamp, message
//   4. Serialize and write the commit object
//   5. Update HEAD to point to our new commit
//
// Returns 0 on success, -1 on error.
int commit_create(const char *message, ObjectID *commit_id_out) {
    // Step 1: Build the directory tree from the staged files
    // This recursively creates tree objects for every directory level
    // and returns the hash of the root tree.
    ObjectID tree_id;
    if (tree_from_index(&tree_id) != 0) {
        fprintf(stderr, "error: failed to build tree from index\n");
        return -1;
    }

    // Step 2: Fill in the commit struct
    Commit commit;
    memset(&commit, 0, sizeof(commit));

    commit.tree = tree_id;
    commit.timestamp = (uint64_t)time(NULL);  // current unix timestamp
    strncpy(commit.author, pes_author(), sizeof(commit.author) - 1);
    strncpy(commit.message, message, sizeof(commit.message) - 1);

    // Step 3: Try to read the current HEAD as the parent commit
    // For the very first commit there won't be a parent — that's fine.
    ObjectID parent_id;
    if (head_read(&parent_id) == 0) {
        // There's an existing commit — link to it as parent
        commit.has_parent = 1;
        commit.parent = parent_id;
    } else {
        // First commit ever — no parent
        commit.has_parent = 0;
    }

    // Step 4: Serialize the commit struct into a text buffer
    void *commit_data;
    size_t commit_len;
    if (commit_serialize(&commit, &commit_data, &commit_len) != 0) {
        fprintf(stderr, "error: failed to serialize commit\n");
        return -1;
    }

    // Step 5: Write the commit as an object to the store
    if (object_write(OBJ_COMMIT, commit_data, commit_len, commit_id_out) != 0) {
        free(commit_data);
        fprintf(stderr, "error: failed to write commit object\n");
        return -1;
    }
    free(commit_data);

    // Step 6: Move the branch pointer to our new commit (the "atomic publish")
    // HEAD → refs/heads/main → <new commit hash>
    if (head_update(commit_id_out) != 0) {
        fprintf(stderr, "error: failed to update HEAD\n");
        return -1;
    }

    return 0;
}
