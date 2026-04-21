// index.c — Staging area implementation
//
// Text format of .pes/index (one entry per line, sorted by path):
//
// <mode-octal> <64-char-hex-hash> <mtime-seconds> <size> <path>
//
// Example:
// 100644 a1b2c3d4e5f6... 1699900000 42 README.md
// 100644 f7e8d9c0b1a2... 1699900100 128 src/main.c
//
// PROVIDED functions: index_find, index_remove, index_status
// TODO functions: index_load, index_save, index_add

#include "index.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>

// Forward declaration (implemented in object.c)
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out);

// ─── PROVIDED ────────────────────────────────────────────────────────────────

// Find an index entry by path (linear scan).
IndexEntry* index_find(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0)
            return &index->entries[i];
    }
    return NULL;
}

// Remove a file from the index.
// Returns 0 on success, -1 if path not in index.
int index_remove(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0) {
            int remaining = index->count - i - 1;
            if (remaining > 0)
                memmove(&index->entries[i], &index->entries[i + 1],
                        remaining * sizeof(IndexEntry));
            index->count--;
            return index_save(index);
        }
    }
    fprintf(stderr, "error: '%s' is not in the index\n", path);
    return -1;
}

// Print the status of the working directory.
int index_status(const Index *index) {
    printf("Staged changes:\n");
    int staged_count = 0;
    for (int i = 0; i < index->count; i++) {
        printf("    staged: %s\n", index->entries[i].path);
        staged_count++;
    }
    if (staged_count == 0) printf("    (nothing to show)\n");
    printf("\n");

    printf("Unstaged changes:\n");
    int unstaged_count = 0;
    for (int i = 0; i < index->count; i++) {
        struct stat st;
        if (stat(index->entries[i].path, &st) != 0) {
            printf("    deleted: %s\n", index->entries[i].path);
            unstaged_count++;
        } else {
            if (st.st_mtime != (time_t)index->entries[i].mtime_sec ||
                st.st_size  != (off_t)index->entries[i].size) {
                printf("    modified: %s\n", index->entries[i].path);
                unstaged_count++;
            }
        }
    }
    if (unstaged_count == 0) printf("    (nothing to show)\n");
    printf("\n");

    printf("Untracked files:\n");
    int untracked_count = 0;
    DIR *dir = opendir(".");
    if (dir) {
        struct dirent *ent;
        while ((ent = readdir(dir)) != NULL) {
            if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0) continue;
            if (strcmp(ent->d_name, ".pes") == 0) continue;
            if (strcmp(ent->d_name, "pes") == 0) continue;
            if (strstr(ent->d_name, ".o") != NULL) continue;

            int is_tracked = 0;
            for (int i = 0; i < index->count; i++) {
                if (strcmp(index->entries[i].path, ent->d_name) == 0) {
                    is_tracked = 1;
                    break;
                }
            }
            if (!is_tracked) {
                struct stat st;
                stat(ent->d_name, &st);
                if (S_ISREG(st.st_mode)) {
                    printf("    untracked: %s\n", ent->d_name);
                    untracked_count++;
                }
            }
        }
        closedir(dir);
    }
    if (untracked_count == 0) printf("    (nothing to show)\n");
    printf("\n");

    return 0;
}

// ─── Helper for sorting entries by path ─────────────────────────────────────
static int compare_entries(const void *a, const void *b) {
    return strcmp(((const IndexEntry *)a)->path, ((const IndexEntry *)b)->path);
}

// ─── IMPLEMENTED ─────────────────────────────────────────────────────────────

// Load the index from .pes/index into memory.
//
// The index file looks like:
//   100644 <64-hex-hash> <mtime> <size> <path>
//
// If the file doesn't exist yet, that's fine — just start with an empty index.
//
// Returns 0 on success, -1 on error.
int index_load(Index *index) {
    // Start fresh
    index->count = 0;

    FILE *f = fopen(INDEX_FILE, "r");
    if (!f) {
        // No index file yet — that's normal on first run, not an error
        return 0;
    }

    char line[1024];
    while (fgets(line, sizeof(line), f)) {
        // Strip the trailing newline
        line[strcspn(line, "\r\n")] = '\0';
        if (line[0] == '\0') continue;  // skip blank lines

        if (index->count >= MAX_INDEX_ENTRIES) break;  // safety cap

        IndexEntry *e = &index->entries[index->count];

        // Each line: "<mode_octal> <hex_hash> <mtime> <size> <path>"
        char hex[HASH_HEX_SIZE + 1];
        uint32_t mode;
        uint64_t mtime;
        uint64_t size;
        char path[512];

        // sscanf parses all 5 fields at once
        if (sscanf(line, "%o %64s %llu %llu %511s",
                   &mode, hex,
                   (unsigned long long *)&mtime,
                   (unsigned long long *)&size,
                   path) != 5) {
            continue;  // skip malformed lines
        }

        e->mode = mode;
        if (hex_to_hash(hex, &e->hash) != 0) continue;
        e->mtime_sec = mtime;
        e->size = size;
        strncpy(e->path, path, sizeof(e->path) - 1);
        e->path[sizeof(e->path) - 1] = '\0';

        index->count++;
    }

    fclose(f);
    return 0;
}

// Save the index to .pes/index atomically.
//
// We write to a temp file first, then rename it over the real index.
// This way, if we crash mid-write, the old index is untouched.
//
// Returns 0 on success, -1 on error.
int index_save(const Index *index) {
    // Sort entries by path (keeps the file human-readable and consistent)
    Index sorted = *index;
    qsort(sorted.entries, sorted.count, sizeof(IndexEntry), compare_entries);

    // Write to a temporary file first
    char tmp_path[] = ".pes/index.tmp";
    FILE *f = fopen(tmp_path, "w");
    if (!f) return -1;

    for (int i = 0; i < sorted.count; i++) {
        const IndexEntry *e = &sorted.entries[i];

        char hex[HASH_HEX_SIZE + 1];
        hash_to_hex(&e->hash, hex);

        // Format: "<mode_octal> <hex_hash> <mtime> <size> <path>\n"
        fprintf(f, "%o %s %llu %llu %s\n",
                e->mode,
                hex,
                (unsigned long long)e->mtime_sec,
                (unsigned long long)e->size,
                e->path);
    }

    // Flush userspace buffer → kernel buffer
    fflush(f);
    // Flush kernel buffer → disk (so data survives a crash)
    fsync(fileno(f));
    fclose(f);

    // Atomically replace the old index with the new one
    return rename(tmp_path, INDEX_FILE);
}

// Stage a file for the next commit.
//
// This does three things:
//   1. Reads the file from disk
//   2. Stores it as a blob in the object store
//   3. Adds/updates the index entry for it
//
// Returns 0 on success, -1 on error.
int index_add(Index *index, const char *path) {
    // Step 1: Open and read the file
    FILE *f = fopen(path, "rb");
    if (!f) {
        fprintf(stderr, "error: cannot open '%s'\n", path);
        return -1;
    }

    fseek(f, 0, SEEK_END);
    long file_size = ftell(f);
    fseek(f, 0, SEEK_SET);

    if (file_size < 0) { fclose(f); return -1; }

    uint8_t *file_data = malloc((size_t)file_size + 1);
    if (!file_data) { fclose(f); return -1; }

    fread(file_data, 1, (size_t)file_size, f);
    fclose(f);

    // Step 2: Hash and store the file contents as a blob object
    ObjectID blob_id;
    if (object_write(OBJ_BLOB, file_data, (size_t)file_size, &blob_id) != 0) {
        free(file_data);
        fprintf(stderr, "error: failed to store '%s'\n", path);
        return -1;
    }
    free(file_data);

    // Step 3: Get file metadata (mtime, size, mode) for the index entry
    struct stat st;
    if (lstat(path, &st) != 0) return -1;

    // Determine file mode (executable or regular)
    uint32_t mode = (st.st_mode & S_IXUSR) ? 0100755 : 0100644;

    // Step 4: Update or create the index entry for this path
    IndexEntry *existing = index_find(index, path);
    if (existing) {
        // Already staged — just update it
        existing->hash = blob_id;
        existing->mtime_sec = (uint64_t)st.st_mtime;
        existing->size = (uint64_t)st.st_size;
        existing->mode = mode;
    } else {
        // New file — add it
        if (index->count >= MAX_INDEX_ENTRIES) {
            fprintf(stderr, "error: index is full\n");
            return -1;
        }
        IndexEntry *e = &index->entries[index->count];
        e->hash = blob_id;
        e->mtime_sec = (uint64_t)st.st_mtime;
        e->size = (uint64_t)st.st_size;
        e->mode = mode;
        strncpy(e->path, path, sizeof(e->path) - 1);
        e->path[sizeof(e->path) - 1] = '\0';
        index->count++;
    }

    // Step 5: Save the updated index to disk
    return index_save(index);
}
