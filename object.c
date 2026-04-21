// object.c — Content-addressable object store
//
// Every piece of data (file contents, directory listings, commits) is stored
// as an "object" named by its SHA-256 hash. Objects are stored under
// .pes/objects/XX/YYYYYY... where XX is the first two hex characters of the
// hash (directory sharding).
//
// PROVIDED functions: compute_hash, object_path, object_exists, hash_to_hex, hex_to_hash
// TODO functions: object_write, object_read

#include "pes.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <openssl/sha.h>

// ─── PROVIDED ────────────────────────────────────────────────────────────────

void hash_to_hex(const ObjectID *id, char *hex_out) {
    for (int i = 0; i < HASH_SIZE; i++) {
        sprintf(hex_out + i * 2, "%02x", id->hash[i]);
    }
    hex_out[HASH_HEX_SIZE] = '\0';
}

int hex_to_hash(const char *hex, ObjectID *id_out) {
    if (strlen(hex) < HASH_HEX_SIZE) return -1;
    for (int i = 0; i < HASH_SIZE; i++) {
        unsigned int byte;
        if (sscanf(hex + i * 2, "%2x", &byte) != 1) return -1;
        id_out->hash[i] = (uint8_t)byte;
    }
    return 0;
}

void compute_hash(const void *data, size_t len, ObjectID *id_out) {
    SHA256_CTX ctx;
    SHA256_Init(&ctx);
    SHA256_Update(&ctx, data, len);
    SHA256_Final(id_out->hash, &ctx);
}

// Get the filesystem path where an object should be stored.
// Format: .pes/objects/XX/YYYYYYYY...
// The first 2 hex chars form the shard directory; the rest is the filename.
void object_path(const ObjectID *id, char *path_out, size_t path_size) {
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id, hex);
    snprintf(path_out, path_size, "%s/%.2s/%s", OBJECTS_DIR, hex, hex + 2);
}

int object_exists(const ObjectID *id) {
    char path[512];
    object_path(id, path, sizeof(path));
    return access(path, F_OK) == 0;
}

// ─── IMPLEMENTED ─────────────────────────────────────────────────────────────

// Write an object to the store.
//
// Think of this like saving a file where the filename IS the content's
// fingerprint. Two identical files always get the same name → stored once.
//
// On-disk format: "blob 16\0Hello, World!\n"
//                  ^type ^size ^NUL ^actual data
//
// Returns 0 on success, -1 on error.
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out) {
    // Step 1: Pick the type string ("blob", "tree", or "commit")
    const char *type_str;
    if      (type == OBJ_BLOB)   type_str = "blob";
    else if (type == OBJ_TREE)   type_str = "tree";
    else if (type == OBJ_COMMIT) type_str = "commit";
    else return -1;

    // Step 2: Build the header: e.g. "blob 42\0"
    // The header is: "<type> <size>\0"
    char header[64];
    int header_len = snprintf(header, sizeof(header), "%s %zu", type_str, len) + 1;
    // +1 to include the null terminator as part of the header

    // Step 3: Build the full object = header + data in one buffer
    size_t full_len = (size_t)header_len + len;
    uint8_t *full = malloc(full_len);
    if (!full) return -1;
    memcpy(full, header, header_len);          // copy header (including \0)
    memcpy(full + header_len, data, len);      // copy actual data after it

    // Step 4: Hash the whole thing — this becomes the object's "name"
    compute_hash(full, full_len, id_out);

    // Step 5: If it already exists in the store, no need to write it again
    // (deduplication — same content = same hash = same file)
    if (object_exists(id_out)) {
        free(full);
        return 0;
    }

    // Step 6: Create the shard directory if needed
    // e.g. .pes/objects/2f/
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id_out, hex);

    char shard_dir[256];
    snprintf(shard_dir, sizeof(shard_dir), "%s/%.2s", OBJECTS_DIR, hex);
    mkdir(shard_dir, 0755);  // OK if it already exists

    // Step 7: Build the final file path
    // e.g. .pes/objects/2f/8a3b5c7d...
    char final_path[512];
    object_path(id_out, final_path, sizeof(final_path));

    // Step 8: Write to a TEMP file first (atomic write pattern)
    // We don't write directly to the final path because if we crash
    // mid-write, we'd have a corrupt half-written object.
    char tmp_path[512];
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", final_path);

    int fd = open(tmp_path, O_CREAT | O_WRONLY | O_TRUNC, 0444);
    if (fd < 0) { free(full); return -1; }

    // Write the full object (header + data) to temp file
    ssize_t written = write(fd, full, full_len);
    free(full);
    if (written < 0 || (size_t)written != full_len) {
        close(fd);
        unlink(tmp_path);
        return -1;
    }

    // Step 9: fsync the temp file — make sure data is on disk, not just in OS cache
    fsync(fd);
    close(fd);

    // Step 10: Rename temp → final path (atomic on POSIX/Linux)
    // This is the "commit" — either the whole object exists or it doesn't.
    if (rename(tmp_path, final_path) != 0) {
        unlink(tmp_path);
        return -1;
    }

    // Step 11: Also fsync the shard directory so the rename is persisted
    int dir_fd = open(shard_dir, O_RDONLY);
    if (dir_fd >= 0) {
        fsync(dir_fd);
        close(dir_fd);
    }

    return 0;
}

// Read an object from the store.
//
// Given a hash (the object's "name"), find the file, verify its integrity
// by re-hashing it, then return the data portion.
//
// The caller must free(*data_out) when done.
//
// Returns 0 on success, -1 on error (not found, corrupt, etc.).
int object_read(const ObjectID *id, ObjectType *type_out, void **data_out, size_t *len_out) {
    // Step 1: Get the file path from the hash
    char path[512];
    object_path(id, path, sizeof(path));

    // Step 2: Open and read the entire file into memory
    FILE *f = fopen(path, "rb");
    if (!f) return -1;

    // Find out how big the file is
    fseek(f, 0, SEEK_END);
    long file_size = ftell(f);
    fseek(f, 0, SEEK_SET);

    if (file_size <= 0) { fclose(f); return -1; }

    uint8_t *buf = malloc((size_t)file_size);
    if (!buf) { fclose(f); return -1; }

    if (fread(buf, 1, (size_t)file_size, f) != (size_t)file_size) {
        fclose(f); free(buf); return -1;
    }
    fclose(f);

    // Step 3: Verify integrity — re-hash the file and compare to the requested hash
    // If someone modified the file on disk, the hashes won't match → corruption!
    ObjectID actual_id;
    compute_hash(buf, (size_t)file_size, &actual_id);
    if (memcmp(actual_id.hash, id->hash, HASH_SIZE) != 0) {
        // Hashes don't match — the file is corrupt
        free(buf);
        return -1;
    }

    // Step 4: Parse the header
    // Format: "blob 16\0<data>" or "tree 42\0<data>" or "commit 128\0<data>"
    // Find the null byte that separates header from data
    uint8_t *null_pos = memchr(buf, '\0', (size_t)file_size);
    if (!null_pos) { free(buf); return -1; }

    // Figure out the type from the header string ("blob", "tree", or "commit")
    if      (strncmp((char*)buf, "blob",   4) == 0) *type_out = OBJ_BLOB;
    else if (strncmp((char*)buf, "tree",   4) == 0) *type_out = OBJ_TREE;
    else if (strncmp((char*)buf, "commit", 6) == 0) *type_out = OBJ_COMMIT;
    else { free(buf); return -1; }

    // Step 5: Extract just the data portion (everything after the \0)
    uint8_t *data_start = null_pos + 1;
    size_t data_len = (size_t)(buf + file_size - data_start);

    // Step 6: Return a copy of the data — caller is responsible for freeing it
    *data_out = malloc(data_len + 1);  // +1 for safety (string terminator)
    if (!*data_out) { free(buf); return -1; }
    memcpy(*data_out, data_start, data_len);
    ((uint8_t*)*data_out)[data_len] = '\0';  // null-terminate for convenience
    *len_out = data_len;

    free(buf);
    return 0;
}
